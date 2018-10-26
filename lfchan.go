package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Determines whether the specified pointer references an element or an internal (including `G`) structure.
// TODO let's make the address of all internal data structures with a specified bit. This change, however, requires
// TODO a huge work, so we assume that small integers are passed to this channel only.
func isElement(p unsafe.Pointer) bool {
	return uintptr(p) < 0xc000000000
}

var broken = (unsafe.Pointer) ((uintptr) (1))
var fail = (unsafe.Pointer) ((uintptr) (2))

// == Channel structure ==

type LFChan struct {
	capacity uint64
	counters counters
	_head    unsafe.Pointer
	_tail    unsafe.Pointer
}

func NewLFChan(capacity uint64) *LFChan {
	node := unsafe.Pointer(newNode(0, nil))
	return &LFChan{
		capacity: capacity,
		counters: counters{},
		_head:    node,
		_tail:    node,
	}
}

// == Segment structure ==

const segmentSizeShift = uint32(6)
const segmentSize = uint32(1 << segmentSizeShift)
const segmentIndexMask = uint64(segmentSize - 1)

type segment struct {
	id       uint64
	_next    unsafe.Pointer
	_cleaned uint32
	_prev    unsafe.Pointer
	data     [segmentSize]unsafe.Pointer
}

func newNode(id uint64, prev *segment) *segment {
	return &segment{
		id:       id,
		_next:    nil,
		_cleaned: 0,
		_prev:    unsafe.Pointer(prev),
	}
}

func indexInNode(index uint64) uint32 {
	return uint32(index & segmentIndexMask)
}

func nodeId(index uint64) uint64 {
	return index >> segmentSizeShift
}

// == `send` and `receive` functions ==

func (c *LFChan) Send(element unsafe.Pointer) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.incSendersAndGetSnapshot()
		if senders <= receivers {
			if c.tryResume(head, senders, element) != fail { return }
		} else {
			if c.trySuspendAndReturn(tail, senders, element, senders - receivers > c.capacity) != fail { return }
		}
	}
}

func (c *LFChan) Receive() unsafe.Pointer {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResume(head, receivers, nil)
			if result != fail {
				return result
			}
		} else {
			result := c.trySuspendAndReturn(tail, receivers, nil, true)
			if result != fail {
				return result
			}
		}
	}
}

func (c *LFChan) tryResume(head *segment, deqIdx uint64, element unsafe.Pointer) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont, isSelectInstance := head.readContinuation(i)
	if cont == broken { return fail }
	if isSelectInstance {
		selectInstance := (*SelectInstance) (cont)
		if !selectInstance.trySetState(unsafe.Pointer(c)) {
			return fail
		}
		runtime.SetGParam(selectInstance.gp, element)
		runtime.UnparkUnsafe(selectInstance.gp)
		// todo find the result
		return nil
	} else {
		if isElement(cont) { return cont } // todo fix this
		result := runtime.GetGParam(cont)
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return result
	}
}

func (c *LFChan) trySuspendAndReturn(tail *segment, enqIdx uint64, element unsafe.Pointer, suspend bool) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	if suspend {
		curG := runtime.GetGoroutine()
		runtime.SetGParam(curG, element)
		if !tail.casContinuation(i, nil, curG) {
			runtime.SetGParam(curG, nil)
			return fail
		}
		runtime.ParkUnsafe()
		result := runtime.GetGParam(curG)
		runtime.SetGParam(curG, nil)
		return result
	} else { // buffering
		if tail.casContinuation(i, nil, element) { return nil } else { return fail }
	}
}

func (c *LFChan) getHead(id uint64, cur *segment) *segment {
	if cur.id == id { return cur }
	cur = c.findOrCreateNode(id, cur)
	cur._prev = nil
	c.moveHeadForward(cur)
	return cur
}

func (c *LFChan) getTail(id uint64, cur *segment) *segment {
	return c.findOrCreateNode(id, cur)
}

func (c *LFChan) findOrCreateNode(id uint64, cur *segment) *segment {
	for cur.id < id {
		curNext := cur.next()
		if curNext == nil {
			// add new segment
			newTail := newNode(cur.id + 1, cur)
			if cur.casNext(nil, unsafe.Pointer(newTail)) {
				if cur.isRemoved() { cur.remove() }
				c.moveTailForward(newTail)
				curNext = newTail
			} else {
				curNext = cur.next()
			}
		}
		cur = curNext
	}
	return cur
}

func (n *segment) readContinuation(i uint32) (cont unsafe.Pointer, isSelectInstance bool) {
	for {
		cont := atomic.LoadPointer(&n.data[i])
		if cont == nil {
			if atomic.CompareAndSwapPointer(&n.data[i], nil, broken) {
				return broken, false
			} else { continue }
		}
		if cont == broken {
			return broken, false
		}
		if isElement(cont) {
			return cont, false
		}
		contType := IntType(cont)
		switch contType {
		case SelectDescType:
			desc := (*SelectDesc)(cont)
			if desc.invoke() {
				atomic.StorePointer(&n.data[i], broken)
				return broken, false
			} else {
				atomic.CompareAndSwapPointer(&n.data[i], cont, desc.cont)
				return desc.cont, IntType(desc.cont) == SelectInstanceType
			}
		case SelectInstanceType: // *SelectInstance
			return cont, true
		default: // *g
			return cont, false
		}
	}
}


// === SELECT ===

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	if element == ReceiverElement {
		return c.regSelectForReceive(selectInstance)
	} else {
		return c.regSelectForSend(selectInstance, element)
	}
}

func (c *LFChan) regSelectForSend(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	try_again: for { // CAS-loop
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.getSnapshot()
		if (senders + 1) <= receivers {
			deqIdx := senders + 1
			head = c.getHead(nodeId(deqIdx), head)
			i := indexInNode(deqIdx)
			// Set descriptor at first
			var desc *SelectDesc
			var descCont unsafe.Pointer
			var isDescContSelectInstance bool
			for {
				descCont, isDescContSelectInstance = head.readContinuation(i)
				if descCont == broken {
					c.counters.incSendersFrom(senders)
					continue try_again
				}
				desc = &SelectDesc {
					__type: SelectDescType,
					channel: c,
					selectInstance: selectInstance,
					cont: descCont,
				}
				if head.casContinuation(i, descCont, unsafe.Pointer(desc)) { break }
			}
			// Invoke selectDesc and update the continuation's cell
			if desc.invoke() {
				head.setContinuation(i, broken)
			} else {
				if !selectInstance.isSelected() {
					head.setContinuation(i, broken)
					c.counters.incSendersFrom(senders)
					continue try_again
				}
				head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
				return false, RegInfo{}
			}
			// Move deque index forward
			c.counters.incSendersFrom(senders)
			// Resume all continuations
			var anotherG unsafe.Pointer
			if isDescContSelectInstance {
				anotherG = ((*SelectInstance) (descCont)).gp
			} else {
				anotherG = descCont
			}
			runtime.SetGParam(anotherG, element)
			runtime.UnparkUnsafe(anotherG)
			runtime.SetGParam(selectInstance.gp, nil)
			runtime.UnparkUnsafe(selectInstance.gp)
			return false, RegInfo{}
		} else {
			if !c.counters.tryIncSendersFrom(senders, receivers) { continue try_again }
			enqIdx := senders + 1
			tail := c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if atomic.CompareAndSwapPointer(&tail.data[i], nil, unsafe.Pointer(selectInstance)) {
				return true, RegInfo{ tail, i }
			} else {
				continue try_again
			}
		}
	}
}

func (c *LFChan) regSelectForReceive(selectInstance *SelectInstance) (added bool, regInfo RegInfo) {
	try_again: for { // CAS-loop
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.getSnapshot()
		if (receivers + 1) <= senders {
			deqIdx := receivers + 1
			head = c.getHead(nodeId(deqIdx), head)
			i := indexInNode(deqIdx)
			// Set descriptor at first
			var desc *SelectDesc
			var descCont unsafe.Pointer
			var isDescContSelectInstance bool
			for {
				descCont, isDescContSelectInstance = head.readContinuation(i)
				if descCont == broken {
					c.counters.incReceiversFrom(receivers)
					continue try_again
				}
				desc = &SelectDesc {
					__type: SelectDescType,
					channel: c,
					selectInstance: selectInstance,
					cont: descCont,
				}
				if head.casContinuation(i, descCont, unsafe.Pointer(desc)) { break }
			}
			// Invoke selectDesc and update the continuation's cell
			if desc.invoke() {
				head.setContinuation(i, broken)
			} else {
				if !selectInstance.isSelected() {
					head.setContinuation(i, broken)
					c.counters.incReceiversFrom(receivers)
					continue try_again
				}
				head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
				return false, RegInfo{}
			}
			// Move deque index forward
			c.counters.incReceiversFrom(receivers)
			// Resume all continuations

			if isElement(descCont) {
				runtime.SetGParam(selectInstance.gp, nil) // TODO element
				runtime.UnparkUnsafe(selectInstance.gp)
				return false, RegInfo{}
			}

			var anotherG unsafe.Pointer
			if isDescContSelectInstance {
				anotherG = ((*SelectInstance) (descCont)).gp
			} else {
				anotherG = descCont
			}
			runtime.SetGParam(anotherG, ReceiverElement)
			runtime.UnparkUnsafe(anotherG)
			runtime.SetGParam(selectInstance.gp, nil) // TODO element
			runtime.UnparkUnsafe(selectInstance.gp)
			return false, RegInfo{}
		} else {
			if !c.counters.tryIncReceiversFrom(senders, receivers) { continue try_again }
			enqIdx := receivers + 1
			tail := c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if atomic.CompareAndSwapPointer(&tail.data[i], nil, unsafe.Pointer(selectInstance)) {
				return true, RegInfo{ tail, i }
			} else {
				continue try_again
			}
		}
	}
}


// == Cleaning ==

func clean(node *segment, index uint32) {
	cont, _ := node.readContinuation(index)
	if cont == broken { return }
	if !node.casContinuation(index, cont, broken) { return }
	if atomic.AddUint32(&node._cleaned, 1) < segmentSize { return }
	// Remove the segment
	node.remove()
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
		if newPrev != nil && curPrev.id <= newPrev.id { return }
		if n.casPrev(unsafe.Pointer(curPrev), unsafe.Pointer(newPrev)) { return }
	}
}