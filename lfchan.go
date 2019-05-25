package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var broken = (unsafe.Pointer) ((uintptr) (1))


// == Channel structure ==

type LFChan struct {
	_deqIdx int64
	_enqIdx int64

	_head    unsafe.Pointer
	_tail    unsafe.Pointer
}

func NewLFChan() *LFChan {
	emptyNode := (unsafe.Pointer) (newNode(0, nil))
	c := &LFChan{
		_deqIdx: 1,
		_enqIdx: 1,
		_head: emptyNode,
		_tail: emptyNode,
	}
	return c
}

// == Segment structure ==

const segmentSizeShift = 5
const segmentSize = 1 << segmentSizeShift
const segmentIndexMask = segmentSize - 1

type segment struct {
	id       int64
	_next    unsafe.Pointer
	_cleaned uint32
	_prev    unsafe.Pointer
	data     [segmentSize*2]unsafe.Pointer
}

func newNode(id int64, prev *segment) *segment {
	return &segment{
		id:       id,
		_next:    nil,
		_cleaned: 0,
		_prev:    unsafe.Pointer(prev),
	}
}

func indexInNode(index int64) int32 {
	return int32((index * 1) & segmentIndexMask)
}

func nodeId(index int64) int64 {
	return index >> segmentSizeShift
}

// == `send` and `receive` functions ==

func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceive(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceive(ReceiverElement)
}

func (c* LFChan) sendOrReceive(element unsafe.Pointer) unsafe.Pointer {
try_again: for { // CAS-loop
	enqIdx := c.enqIdx()
	deqIdx := c.deqIdx()
	if enqIdx < deqIdx {
		continue try_again
	}
	// Check if queue is empty
	if deqIdx == enqIdx {
		if c.addToWaitingQueue2(enqIdx, element, nil) {
			gp := runtime.GetGoroutine()
			runtime.ParkUnsafe(gp)
			return runtime.GetGParam(gp)
		} else {
			continue try_again
		}
	} else {
		// Queue is not empty
		head := c.getHead()
		headId := head.id
		// check consistency
		deqIdxNodeId := nodeId(deqIdx)
		if headId != deqIdxNodeId {
			if headId < deqIdxNodeId {
				headNext := head.next()
				headNext._prev = nil
				c.casHead(head, headNext)
			} else {
				c.casDeqIdx(deqIdx, headId<<segmentSizeShift)
			}
			continue try_again
		}
		// Read the first element
		deqIndexInHead := indexInNode(deqIdx)
		firstElement := head.readElement(deqIndexInHead)
		// Check that the element is not taken already.
		if firstElement == broken {
			c.casDeqIdx(deqIdx, deqIdx+1)
			continue try_again
		}
		// Decide should we make a rendezvous or not
		makeRendezvous := (element == ReceiverElement) != (firstElement == ReceiverElement)
		if makeRendezvous {
			if c.tryResumeContinuation(head, deqIndexInHead, deqIdx, element) {
				return firstElement
			} else {
				continue try_again
			}
		} else {
			if c.addToWaitingQueue2(enqIdx, element, nil) {
				gp := runtime.GetGoroutine()
				runtime.ParkUnsafe(gp)
				return runtime.GetGParam(gp)
			} else {
				continue try_again
			}
		}
	}
}
}

func (c *LFChan) addToWaitingQueue(enqIdx int64, element unsafe.Pointer, cont unsafe.Pointer) (bool, RegInfo) {
	// Read tail and its id
	tail := c.getTail()
	tailId := tail.id
	// check consistency
	if enqIdx <= tailId << segmentSizeShift {
		if enqIdx == tailId << segmentSizeShift {
			c.casEnqIdx(enqIdx, enqIdx + 1)
		}
		return false, RegInfo{}
	}
	// Get continuation if needed
	if cont == nil { cont = runtime.GetGoroutine() }
	// Check if a new node should be added
	if enqIdx == (tailId + 1) << segmentSizeShift  {
		success, node := c.addNewNode(tail, element, enqIdx, cont)
		if success {
			return true, RegInfo{segment: node, index: 0}
		}  else { return false, RegInfo{} }
	}
	// Just check that `enqIdx` is valid and try to store the current
	// goroutine into the `tail` by `enqIdxInNode`
	enqIdxInNode := indexInNode(enqIdx)
	if c.storeContinuation(tail, enqIdxInNode, enqIdx, element, cont) {
		return true, RegInfo{segment: tail, index: enqIdxInNode}
	} else { return false, RegInfo{} }
}

func (c *LFChan) addToWaitingQueue2(enqIdx int64, element unsafe.Pointer, cont unsafe.Pointer) bool {
	success, _ := c.addToWaitingQueue(enqIdx, element, cont)
	return success
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) addNewNode(tail *segment, element unsafe.Pointer, enqIdx int64, cont unsafe.Pointer) (bool, *segment) {
	for {
		// If next node is not null, help to move the tail pointer
		tailNext := (*segment) (tail.next())
		if tailNext != nil {
			// If this CAS fails, another thread moved the tail pointer
			c.casTail(tail, tailNext)
			c.casEnqIdx(enqIdx, enqIdx + 1) // help
			return false, nil
		}
		// Create a new node with this continuation and element and try to add it
		newTail := newNode(tail.id + 1, tail)
		newTail.data[0] = element
		newTail.data[1] = cont
		if tail.casNext(nil, unsafe.Pointer(newTail)) {
			// New node added, try to move tail,
			// if the CAS fails, another thread moved it.
			c.casTail(tail, newTail)
			c.casEnqIdx(enqIdx, enqIdx + 1) // help for others
			// Remove the previous tail from the waiting queue
			// if it was marked as logically removed.
			if tail.isRemoved() {
				tail.remove()
			}
			// Success, return true.
			return true, newTail
		} else { continue }
	}
}

// Tries to store the current continuation and element (in this order!)
// to the specified node at the specified index. Returns `true` on success,
// `false` otherwise`.
func (c *LFChan) storeContinuation(node *segment, indexInNode int32, enqIdx int64, element unsafe.Pointer, cont unsafe.Pointer) bool {
	// Try to move enqueue index forward, return `false` if fails
	if !c.casEnqIdx(enqIdx, enqIdx + 1) {
		return false
	}
	// Slot `index` is claimed, try to store the continuation and the element (in this order!) to it.
	// Can fail if another thread marked this slot as broken, return `false` in this case.
	node.data[indexInNode * 2 + 1] = cont

	if atomic.CompareAndSwapPointer(&node.data[indexInNode * 2], nil, element) {
		// Can be suspended, return true
		return true
	} else {
		// The slot is broken, clean it and return `false`
		node.data[indexInNode * 2 + 1] = broken
		return false
	}
}

// Try to remove a continuation from the specified node at the
// specified index and resume it. Returns `true` on success, `false` otherwise.
func (c *LFChan) tryResumeContinuation(head *segment, indexInNode int32, deqIdx int64, element unsafe.Pointer) bool {
	// Try to move 'deqIdx' forward, return `false` if fails
	if !c.casDeqIdx(deqIdx, deqIdx + 1) { return false }
	// Read continuation and CAS it to `takenContinuation`
	var cont unsafe.Pointer
	var isContSelectInstance bool
	for {
		cont, isContSelectInstance = head.readContinuation(indexInNode)
		if cont == broken { return false }
		if head.casContinuation(indexInNode, cont, broken) { break }
	}
	// Clear element's cell
	head.data[indexInNode * 2] = broken
	// Try to resume the continuation
	if isContSelectInstance {
		selectInstance := (*SelectInstance) (cont)
		if !selectInstance.trySetDescriptor(unsafe.Pointer(c)) { return false }
		runtime.SetGParam(selectInstance.gp, element)
		runtime.UnparkUnsafe(selectInstance.gp)
		return true
	} else { // *g
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return true
	}
}


func (c *LFChan) tryResumeContinuationForSelect(head *segment, indexInNode int32, deqIdx int64, element unsafe.Pointer, selectInstance *SelectInstance, firstElement unsafe.Pointer) bool {
	// Set descriptor at first
	var desc *SelectDesc
	var descCont unsafe.Pointer
	var isDescContSelectInstance bool
	for {
		descCont, isDescContSelectInstance = head.readContinuation(indexInNode)
		if descCont == broken {
			c.casDeqIdx(deqIdx, deqIdx + 1)
			return false
		}
		desc = &SelectDesc {
			__type: SelectDescType,
			channel: c,
			selectInstance: selectInstance,
			cont: descCont,
		}
		if head.casContinuation(indexInNode, descCont, unsafe.Pointer(desc)) { break }
	}
	// Invoke selectDesc and update the continuation's cell
	if desc.invoke() {
		head.setContinuation(indexInNode, broken)
	} else {
		if !selectInstance.isSelected() {
			head.setContinuation(indexInNode, broken)
			c.casDeqIdx(deqIdx, deqIdx + 1)
			return false
		}
		head.casContinuation(indexInNode, unsafe.Pointer(desc), desc.cont)
		return false
	}
	// Move deque index forward
	c.casDeqIdx(deqIdx, deqIdx + 1)
	// Resume all continuations
	var anotherG unsafe.Pointer
	if isDescContSelectInstance {
		anotherG = ((*SelectInstance) (descCont)).gp
	} else {
		anotherG = descCont
	}
	runtime.SetGParam(anotherG, element)
	runtime.UnparkUnsafe(anotherG)
	runtime.SetGParam(selectInstance.gp, firstElement)
	runtime.UnparkUnsafe(selectInstance.gp)
	return true
}

func (n *segment) readContinuation(index int32) (cont unsafe.Pointer, isSelectInstance bool) {
	contPointer := &n.data[index * 2 + 1]
	for {
		cont := atomic.LoadPointer(contPointer)
		if cont == broken {
			return broken, false
		}
		contType := IntType(cont)
		switch contType {
		case SelectDescType:
			desc := (*SelectDesc)(cont)
			if desc.invoke() {
				atomic.StorePointer(contPointer, broken)
				return broken, false
			} else {
				atomic.CompareAndSwapPointer(contPointer, cont, desc.cont)
				return desc.cont, IntType(desc.cont) == SelectInstanceType
			}
		case SelectInstanceType: // *SelectInstance
			return cont, true
		default: // *g
			return cont, false
		}
	}
}


func (s *segment) readElement(i int32) unsafe.Pointer {
	// Element index in `Node#_data` array
	// Spin wait on the slot
	elementAddr := &s.data[i * 2]
	element := atomic.LoadPointer(elementAddr) // volatile read
	if element != nil {
		return element
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(elementAddr, nil, broken) {
		return broken
	} else {
		// The element is set, read it and return
		return s.data[i * 2]
	}
}

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	try_again:
	for { // CAS-loop
		if selectInstance.isSelected() {
			return false, RegInfo{}
		}
		enqIdx := c.enqIdx()
		deqIdx := c.deqIdx()
		if enqIdx < deqIdx {
			continue try_again
		}
		// Check if queue is empty
		if deqIdx == enqIdx {
			addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
			if addSuccess {
				return true, regInfo
			} else {
				continue try_again
			}
		} else {
			// Queue is not empty
			head := c.getHead()
			headId := head.id
			// check consistency
			deqIdxNodeId := nodeId(deqIdx)
			if headId != deqIdxNodeId {
				if headId < deqIdxNodeId {
					headNext := head.next()
					headNext._prev = nil
					c.casHead(head, headNext)
				} else {
					c.casDeqIdx(deqIdx, headId<<segmentSizeShift)
				}
				continue try_again
			}
			// Read the first element
			deqIndexInHead := indexInNode(deqIdx)
			firstElement := head.readElement(deqIndexInHead)
			// Check that the element is not taken already.
			if firstElement == broken {
				c.casDeqIdx(deqIdx, deqIdx+1)
				continue try_again
			}
			// Decide should we make a rendezvous or not
			makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
			if makeRendezvous {
				if c.tryResumeContinuationForSelect(head, deqIndexInHead, deqIdx, element, selectInstance, firstElement) {
					return false, RegInfo{}
				} else {
					continue try_again
				}
			} else {
				addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
				if addSuccess {
					return true, regInfo
				} else {
					continue try_again
				}
			}
		}
	}
}


// == Cleaning ==

func (s *segment) clean(index int32) {
	cont, _ := s.readContinuation(index)
	if cont == broken { return }
	if !s.casContinuation(index, cont, broken) { return }
	atomic.StorePointer(&s.data[index * 2], broken)
	if atomic.AddUint32(&s._cleaned, 1) < segmentSize { return }
	// Remove the node
	s.remove()
}

func (n *segment) isRemoved() bool {
	return atomic.LoadUint32(&n._cleaned) == segmentSize
}

func (n *segment) remove() {
	next := n.next()
	if next == nil { return }
	for next.isRemoved() {
		newNext := next.next()
		if newNext == nil { break }
		next = newNext
	}
	prev := n.prev()
	for {
		if prev == nil {
			next.movePrevToTheLeft(nil)
			return
		}
		if prev.isRemoved() {
			prev = prev.prev()
			continue
		}
		next.movePrevToTheLeft(prev)
		prev.moveNextToTheRight(next)
		if next.isRemoved() || !prev.isRemoved() { return }
		prev = prev.prev()
	}
}

func (n *segment) moveNextToTheRight(newNext *segment) {
	for {
		curNext := n.next()
		if curNext.id >= newNext.id { return }
		if n.casNext(unsafe.Pointer(curNext), unsafe.Pointer(newNext)) { return }
	}
}

func (n *segment) movePrevToTheLeft(newPrev *segment) {
	for {
		curPrev := n.prev()
		if curPrev == nil { return }
		if newPrev != nil && curPrev.id <= newPrev.id { return }
		if n.casPrev(unsafe.Pointer(curPrev), unsafe.Pointer(newPrev)) { return }
	}
}














func (c *LFChan) casEnqIdx(old int64, new int64) bool {
	return atomic.CompareAndSwapInt64(&c._enqIdx, old, new)
}

func (c *LFChan) casDeqIdx(old int64, new int64) bool {
	return atomic.CompareAndSwapInt64(&c._deqIdx, old, new)
}

func (c *LFChan) enqIdx() int64 {
	return atomic.LoadInt64(&c._enqIdx)
}

func (c *LFChan) deqIdx() int64 {
	return atomic.LoadInt64(&c._deqIdx)
}

func (c *LFChan) getTail() *segment {
	return (*segment) (atomic.LoadPointer(&c._tail))
}

func (c *LFChan) getHead() *segment {
	return (*segment) (atomic.LoadPointer(&c._head))
}

func (c *LFChan) casHead(oldHead *segment, newHead *segment) bool {
	return atomic.CompareAndSwapPointer(&c._head, (unsafe.Pointer) (oldHead), (unsafe.Pointer) (newHead))
}

func (n *segment) casContinuation(index int32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n.data[index * 2 + 1], old, new)
}

func (n *segment) setContinuation(index int32, cont unsafe.Pointer) {
	atomic.StorePointer(&n.data[index * 2 + 1], cont)
}

func (c *LFChan) casTail(oldTail *segment, newTail *segment) bool {
	return atomic.CompareAndSwapPointer(&c._tail, (unsafe.Pointer) (oldTail), (unsafe.Pointer) (newTail))
}

func (n *segment) next() *segment {
	return (*segment)(atomic.LoadPointer(&n._next))
}

func (n *segment) casNext(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._next, old, new)
}

func (n *segment) prev() *segment {
	return (*segment)(atomic.LoadPointer(&n._prev))
}

func (n *segment) casPrev(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._prev, old, new)
}