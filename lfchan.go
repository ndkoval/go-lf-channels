package go_lf_channels

import (
	"sync/atomic"
	"runtime"
	"unsafe"
	"math/rand"
	"time"
)

type LFChan struct {
	spinThreshold int

	_head unsafe.Pointer
	_tail unsafe.Pointer
}

func NewLFChan(spinThreshold int) *LFChan {
	emptyNode := (unsafe.Pointer) (newNode(0, nil))
	return &LFChan{
		spinThreshold: spinThreshold,
		_head: emptyNode,
		_tail: emptyNode,
	}
}

type node struct {
	id          int64
	_enqIdx     int32
	_deqIdx     int32
	_next       unsafe.Pointer
	_data       [segmentSize * 2]unsafe.Pointer
	_cleaned int32
	_prev 		unsafe.Pointer
}

func newNode(id int64, prev *node) *node {
	return &node{
		id: id,
		_next: nil,
		_cleaned: 0,
		_prev: unsafe.Pointer(prev),
	}
}

type SelectAlternative struct {
	channel *LFChan
	element unsafe.Pointer
	action  func(result unsafe.Pointer)
}

const SelectInstanceType int32 = 1298498092
type SelectInstance struct {
	__type int32
	id int64
	alternatives *[]SelectAlternative
	regInfos *[]RegInfo
	state unsafe.Pointer
	gp	  unsafe.Pointer // goroutine
}

type RegInfo struct {
	node *node
	index int32
}

const SelectDescType int32 = 2019727883
type SelectDesc struct {
	__type int32
	channel *LFChan
	selectInstance *SelectInstance
	cont unsafe.Pointer // either *SelectInstance or *g

	status int32 // 0 -- UNDECIDED, 1 -- SUCCESS, 2 -- FAIL
}
const UNDECIDED = 0
const SUCCEEDED = 1
const FAILED = 2

var takenContinuation = (unsafe.Pointer) ((uintptr) (1))
var takenElement = (unsafe.Pointer) ((uintptr) (2))
var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
const segmentSize = 64

var selectIdGen int64 = 0


func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceiveSuspend(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceiveSuspend(ReceiverElement)
}

const maxBackoffMask = 0x11111111111
var consumedCPU = int32(time.Now().Unix())

func consumeCPU(tokens int) {
	t := int(atomic.LoadInt32(&consumedCPU)) // volatile read
	for i := tokens; i > 0; i-- {
		t += (t * 0x5DEECE66D + 0xB + i) & (0xFFFFFFFFFFFF)
	}
	if t == 42 { atomic.StoreInt32(&consumedCPU, consumedCPU + int32(t)) }
}

func (c* LFChan) sendOrReceiveSuspend(element unsafe.Pointer) unsafe.Pointer {
	backoff := 4
	try_again: for { // CAS-loop
		tail := c.getTail()
		tailEnqIdx := tail.enqIdx()
		head := c.getHead()
		headDeqIdx := head.deqIdx()
		headEnqIdx := head.enqIdx()
		if headDeqIdx == headEnqIdx {
			if headDeqIdx == segmentSize {
				if c.adjustHead(head) { continue try_again }
				if c.addNewNode(head, element, nil) {
					return parkAndThenReturn()
				} else {
					backoff *= 2
					backoff &= maxBackoffMask
					consumeCPU(backoff)
					continue try_again
				}
			} else {
				if storeContinuation(head, headEnqIdx, element, nil) {
					return parkAndThenReturn()
				} else {
					backoff *= 2
					backoff &= maxBackoffMask
					consumeCPU(backoff)
					continue try_again
				}
			}
		} else {
			firstElement := c.readElement(head, headDeqIdx)
			// Check that the element is not taken already.
			if firstElement == takenElement {
				head.casDeqIdx(headDeqIdx, headDeqIdx + 1)
				continue try_again
			}
			// Decide should we make a rendezvous or not
			makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
			if makeRendezvous {
				if c.tryResumeContinuation(head, headDeqIdx, element) {
					return firstElement
				} else {
					backoff *= 2
					backoff &= maxBackoffMask
					consumeCPU(backoff)
					continue try_again
				}
			} else {
				if tailEnqIdx == segmentSize {
					if c.addNewNode(tail, element, nil) { return parkAndThenReturn() }
				} else {
					if storeContinuation(tail, tailEnqIdx, element, nil) { return parkAndThenReturn() }
				}
				backoff *= 2
				backoff &= maxBackoffMask
				consumeCPU(backoff)
				continue try_again
			}
		}
	}
}

func (c *LFChan) adjustHead(head *node) bool {
	headNext := head.next()
	if headNext == nil { return false }
	c.casHead(head, headNext)
	return true
}

// Tries to read an element from the specified node
// at the specified index. Returns this element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) readElement(node *node, index int32) unsafe.Pointer {
	// Element index in `Node#_data` array
	// Spin wait on the slot
	elementAddr := &node._data[index * 2]
	var attempt = 0
	for {
		if attempt % 8 == 0 {
			element := atomic.LoadPointer(elementAddr) // volatile read
			if element != nil {
				return element
			}
			if attempt >= c.spinThreshold {
				break
			}
		}
		attempt++
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(elementAddr, nil, takenElement) {
		return takenElement
	} else {
		// The element is set, read it and return
		return node._data[index * 2]
	}
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) addNewNode(tail *node, element unsafe.Pointer, cont unsafe.Pointer) bool {
	for {
		// If next node is not null, help to move the tail pointer
		tailNext := (*node) (tail.next())
		if tailNext != nil {
			// If this CAS fails, another thread moved the tail pointer
			c.casTail(tail, tailNext)
			return false
		}
		// Create a new node with this continuation and element and try to add it
		if cont == nil { cont = runtime.GetGoroutine() }
		newTail := newNode(tail.id + 1, tail)
		newTail._data[0] = element
		newTail._data[1] = cont
		newTail._enqIdx = 1
		if tail.casNext(nil, unsafe.Pointer(newTail)) {
			// New node added, try to move tail,
			// if the CAS fails, another thread moved it.
			c.casTail(tail, newTail)
			// Remove the previous tail from the waiting queue
			// if it was marked as logically removed.
			if tail.isRemoved() {
				tail.remove()
			}
			// Success, return true.
			return true
		} else { continue }
	}
}

// Tries to store the current continuation and element (in this order!)
// to the specified node at the specified index. Returns `true` on success,
// `false` otherwise`.
func storeContinuation(node *node, enqIdx int32, element unsafe.Pointer, cont unsafe.Pointer) bool {
	// Try to move enqueue index forward, return `false` if fails
	if !node.casEnqIdx(enqIdx, enqIdx + 1) {
		return false
	}
	// Slot `index` is claimed, try to store the continuation and the element (in this order!) to it.
	// Can fail if another thread marked this slot as broken, return `false` in this case.
	if cont == nil { cont = runtime.GetGoroutine() }
	node._data[enqIdx * 2 + 1] = cont
	if atomic.CompareAndSwapPointer(&node._data[enqIdx * 2], nil, element) {
		// Can be suspended, return true
		return true
	} else {
		// The slot is broken, clean it and return `false`
		node._data[enqIdx * 2 + 1] = takenContinuation
		return false
	}
}

// Try to remove a continuation from the specified node at the
// specified index and resume it. Returns `true` on success, `false` otherwise.
func (c *LFChan) tryResumeContinuation(head *node, deqIdx int32, element unsafe.Pointer) bool {
	// Try to move 'deqIdx' forward, return `false` if fails
	if !head.casDeqIdx(deqIdx, deqIdx + 1) { return false }
	// Read continuation and CAS it to `takenContinuation`
	var cont unsafe.Pointer
	for {
		cont = head.readContinuation(deqIdx)
		if cont == takenContinuation { return false }
		if head.casContinuation(deqIdx, cont, takenContinuation) { break }
	}
	// Clear element's cell
	head._data[deqIdx * 2] = takenElement
	// Try to resume the continuation
	contType := IntType(cont)
	if contType == SelectInstanceType {
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


func (c *LFChan) tryResumeContinuationForSelect(head *node, deqIdx int32, element unsafe.Pointer, selectInstance *SelectInstance, firstElement unsafe.Pointer) bool {
	//// Set descriptor at first
	//var desc *SelectDesc
	//for {
	//	cont := head.readContinuation(deqIdx)
	//	if cont == takenContinuation {
	//		head.casDeqIdx(deqIdx, deqIdx + 1)
	//		return false
	//	}
	//	desc = &SelectDesc {
	//		__type: SelectDescType,
	//		channel: c,
	//		selectInstance: selectInstance,
	//		cont: cont,
	//	}
	//	if head.casContinuation(indexInNode, cont, unsafe.Pointer(desc)) { break }
	//}
	//// Invoke selectDesc and update the continuation's cell
	//if desc.invoke() {
	//	head.setContinuation(indexInNode, takenContinuation)
	//} else {
	//	if !selectInstance.isSelected() {
	//		head.setContinuation(indexInNode, takenContinuation)
	//		c.casDeqIdx(deqIdx, deqIdx + 1)
	//		return false
	//	}
	//	head.casContinuation(indexInNode, unsafe.Pointer(desc), desc.cont)
	//	return false
	//}
	//// Move deque index forward
	//c.casDeqIdx(deqIdx, deqIdx + 1)
	//// Resume all continuations
	//anotherCont := desc.cont
	//var anotherG unsafe.Pointer
	//anotherContType := IntType(anotherCont)
	//if anotherContType == SelectInstanceType {
	//	anotherG = ((*SelectInstance) (anotherCont)).gp
	//} else {
	//	anotherG = anotherCont
	//}
	//runtime.SetGParam(anotherG, element)
	//runtime.UnparkUnsafe(anotherG)
	//runtime.SetGParam(selectInstance.gp, firstElement)
	//runtime.UnparkUnsafe(selectInstance.gp)
	return true
}

func (n *node) readContinuation(index int32) unsafe.Pointer {
	contPointer := &n._data[index * 2 + 1]
	for {
		cont := atomic.LoadPointer(contPointer)
		if cont == takenContinuation {
			return cont
		}
		contType := IntType(cont)
		switch contType {
		case SelectDescType:
			desc := (*SelectDesc)(cont)
			if desc.invoke() {
				atomic.StorePointer(contPointer, takenContinuation)
				return takenContinuation
			} else {
				atomic.CompareAndSwapPointer(contPointer, cont, desc.cont)
				return desc.cont
			}
		default: // *g or *SelectInstance
			return cont
		}
	}
}


// === SELECT ===

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	return false, RegInfo{}
	//try_again: for { // CAS-loop
	//	//println(selectInstance)
	//	if selectInstance.isSelected() { return false, RegInfo{} }
	//	enqIdx := c.enqIdx()
	//	deqIdx := c.deqIdx()
	//	if enqIdx < deqIdx { continue try_again }
	//	// Check if queue is empty
	//	if deqIdx == enqIdx {
	//		addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
	//		if addSuccess {
	//			return true, regInfo
	//		} else { continue try_again }
	//	} else {
	//		// Queue is not empty
	//		head := c.getHead()
	//		headId := head.id
	//		//println(selectInstance, "  ", deqIdx, "   ", enqIdx, "   H:", head, "  HN:", head.next())
	//		deqIdxNodeId := nodeId(deqIdx)
	//		// Check that deqIdx is not outdated
	//		if headId > deqIdxNodeId {
	//			continue try_again
	//		}
	//		// Check that head pointer should be moved forward
	//		if headId < deqIdxNodeId {
	//			headNext := head.next()
	//			headNextNode := (*node) (headNext)
	//			headNextNode._prev = nil
	//			c.casHead(head, headNext)
	//			continue try_again
	//		}
	//		// Read the first element
	//		deqIdxInNode := indexInNode(deqIdx)
	//		firstElement := c.readElement(head, deqIdxInNode)
	//		// Check that the element is not taken already.
	//		if firstElement == takenElement {
	//			c.casDeqIdx(deqIdx, deqIdx + 1)
	//			continue try_again
	//		}
	//		// Decide should we make a rendezvous or not
	//		makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
	//		deqIdxLimit := enqIdx
	//		if makeRendezvous {
	//			if c.tryResumeContinuationForSelect(head, deqIdxInNode, deqIdx, element, selectInstance, firstElement) {
	//				return false, RegInfo{}
	//			} else { continue try_again }
	//		} else {
	//			for {
	//				addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
	//				if addSuccess {
	//					return true, regInfo
	//				} else { continue try_again }
	//				enqIdx = c.enqIdx()
	//				deqIdx = c.deqIdx()
	//				if deqIdx >= deqIdxLimit { continue try_again }
	//			}
	//		}
	//	}
	//}
}

func (sd *SelectDesc) invoke() bool {
	curStatus := sd.getStatus()
	if curStatus != UNDECIDED { return curStatus == SUCCEEDED }
	// Phase 1 -- set descriptor to the select's state,
	// help for others if needed.
	selectInstance := sd.selectInstance
	anotherCont := sd.cont
	anotherContType := IntType(anotherCont)
	sdp := unsafe.Pointer(sd)
	failed := false
	if anotherContType == SelectInstanceType {
		anotherSelectInstance := (*SelectInstance) (anotherCont)
		if selectInstance.id < anotherSelectInstance.id {
			if selectInstance.trySetDescriptor(sdp) {
				if !anotherSelectInstance.trySetDescriptor(sdp) {
					failed = true
					selectInstance.resetState(sdp)
				}
			} else { failed = true }
		} else {
			if anotherSelectInstance.trySetDescriptor(sdp) {
				if !selectInstance.trySetDescriptor(sdp) {
					failed = true
					anotherSelectInstance.resetState(sdp)
				}
			} else { failed = true }
		}
	} else {
		if !selectInstance.trySetDescriptor(sdp) {
			sd.setStatus(FAILED)
			return false
		}
	}
	// Phase 3 -- update descriptor's and selectInstance statuses
	if (failed) {
		sd.setStatus(FAILED)
		return false
	} else {
		sd.setStatus(SUCCEEDED)
		return true
	}
}

func (s *SelectInstance) resetState(desc unsafe.Pointer) {
	s.casState(desc, nil)
}


func (s *SelectInstance) readState(allowedUnprocessedDesc unsafe.Pointer) unsafe.Pointer {
	for {
		// Read state
		state := s.getState()
		// Check if state is `nil` or `*LFChan` and return it in this case
		if state == nil || state == allowedUnprocessedDesc {
			return state
		}
		stateType := IntType(state)
		if stateType != SelectDescType {
			return state
		}
		// If this SelectDesc is allowed return it
		// State is SelectDesc, help it
		desc := (*SelectDesc) (state)
		// Try to help with the found descriptor processing
		// and update state
		if desc.invoke() {
			return state
		} else {
			if s.casState(state, nil) { return nil }
		}
	}
}

func (s *SelectInstance) trySetDescriptor(desc unsafe.Pointer) bool {
	for {
		state := s.readState(desc)
		if state == desc { return true }
		if state != nil { return false }
		if s.casState(nil, desc) { return true }
	}
}

func Select(alternatives ...SelectAlternative)  {
	selectImpl(&alternatives)
}

func SelectUnbiased(alternatives ...SelectAlternative) {
	shuffleAlternatives(&alternatives)
	selectImpl(&alternatives)
}

func selectImpl(alternatives *[]SelectAlternative) {
	selectInstance := &SelectInstance {
		__type: SelectInstanceType,
		id: atomic.AddInt64(&selectIdGen, 1),
		alternatives: alternatives,
		regInfos: &([]RegInfo{}),
		state: nil,
		gp: runtime.GetGoroutine(),
	}
	selectInstance.doSelect()
}

// Shuffles alternatives randomly for `SelectUnbiased`.
func shuffleAlternatives(alternatives *[]SelectAlternative) {
	alts := *alternatives
	rand.Shuffle(len(alts), func (i, j int) {
		alts[i], alts[j] = alts[j], alts[i]
	})
}

func (s *SelectInstance) isSelected() bool {
	return s.readState(nil) != nil
}

// Does select in 3-phase way. At first it selects
// an alternative atomically (suspending if needed),
// then it unregisters from unselected channels,
// and invokes the specified for the selected
// alternative action at last.
func (s *SelectInstance) doSelect() {
	result, alternative := s.selectAlternative()
	s.cancelNonSelectedAlternatives()
	alternative.action(result)
}

func (s *SelectInstance) selectAlternative() (unsafe.Pointer, SelectAlternative) {
	for _, alt := range *(s.alternatives) {
		added, regInfo := alt.channel.regSelect(s, alt.element)
		if added {
			c := make([]RegInfo, len(*s.regInfos))
			copy(c, *s.regInfos)
			c = append(c, regInfo)
			s.regInfos = &c
		}
		if s.isSelected() { break }
	}
	result := parkAndThenReturn()
	selectState := s.state
	var channel *LFChan
	if IntType(selectState) == SelectDescType {
		channel = (*LFChan) ((*SelectDesc) (selectState).channel)
	} else {
		channel = (*LFChan) (selectState)
	}
	alternative := s.findAlternative(channel)
	return result, alternative
}

/**
 * Finds the selected alternative and returns it. This method relies on the fact
 * that `state` field stores the selected channel and looks for an alternative
 * with this channel.
 */
func (s *SelectInstance) findAlternative(channel *LFChan) SelectAlternative {
	for _, alt := range *(s.alternatives) {
		if alt.channel == channel {
			return alt
		}
	}
	stateType := IntType(s.state)
	switch stateType {
	case SelectInstanceType: panic("Impossible: SelectInstance")
	case SelectDescType: panic("Impossible: SelectDesc")
	}
	panic("Impossible")
}

func (s *SelectInstance) cancelNonSelectedAlternatives() {
	for _, ri := range *s.regInfos {
		clean(ri.node, ri.index)
	}
}

// ==== CLEANING ====

func clean(node *node, index int32) {
	cont := node.readContinuation(index)
	if cont == takenContinuation { return }
	if !node.casContinuation(index, cont, takenContinuation) { return }
	atomic.StorePointer(&node._data[index * 2], takenElement)
	if atomic.AddInt32(&node._cleaned, 1) < segmentSize { return }
	// Remove the node
	node.remove()
}

func (n *node) isRemoved() bool {
	return atomic.LoadInt32(&n._cleaned) == segmentSize
}

func (n *node) remove() {
	if true { return }
	next := (*node) (n.next())
	if next == nil { return }
	for next.isRemoved() {
		newNext := (*node) (next.next())
		if newNext == nil { break }
		next = newNext
	}
	prev := (*node) (n.prev())
	for {
		if prev == nil {
			next.movePrevLefter(nil)
			return
		}
		if prev.isRemoved() {
			prev = (*node) (prev.prev())
			continue
		}
		prev.moveNextRighter(next)
		next.movePrevLefter(prev)
		if next.isRemoved() || !prev.isRemoved() { return }
		prev = (*node) (prev.prev())
	}
}

func (n *node) moveNextRighter(newNext *node) {
	for {
		curNext := n.next()
		curNextNode := (*node) (curNext)
		if curNextNode.id >= newNext.id { return }
		if n.casNext(curNext, unsafe.Pointer(newNext)) { return }
	}
}

func (n *node) movePrevLefter(newPrev *node) {
	for {
		curPrev := n.prev()
		if curPrev == nil { return }
		curPrevNode := (*node) (curPrev)
		if newPrev != nil && curPrevNode.id <= newPrev.id { return }
		if n.casPrev(curPrev, unsafe.Pointer(newPrev)) { return }
	}
}

// === FUCKING GOLANG ===

func (n *node) enqIdx() int32 {
	return atomic.LoadInt32(&n._enqIdx)
}

func (n *node) deqIdx() int32 {
	return atomic.LoadInt32(&n._deqIdx)
}

func parkAndThenReturn() unsafe.Pointer {
	runtime.ParkUnsafe()
	return runtime.GetGParam(runtime.GetGoroutine())
}

func (c *LFChan) casTail(oldTail *node, newTail *node) bool {
	return atomic.CompareAndSwapPointer(&c._tail, (unsafe.Pointer) (oldTail), (unsafe.Pointer) (newTail))
}

func (c *LFChan) getTail() *node {
	return (*node) (atomic.LoadPointer(&c._tail))
}

func (c *LFChan) getHead() *node {
	return (*node) (atomic.LoadPointer(&c._head))
}

func (c *LFChan) casHead(oldHead *node, newHead unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&c._head, (unsafe.Pointer) (oldHead), newHead)
}

func (n *node) casEnqIdx(old int32, new int32) bool {
	return atomic.CompareAndSwapInt32(&n._enqIdx, old, new)
}

func (n *node) casDeqIdx(old int32, new int32) bool {
	return atomic.CompareAndSwapInt32(&n._deqIdx, old, new)
}

func (sd *SelectDesc) getStatus() int32 {
	return atomic.LoadInt32(&sd.status)
}

func (sd *SelectDesc) setStatus(status int32) {
	atomic.StoreInt32(&sd.status, status)
}

func (n *node) next() unsafe.Pointer {
	return atomic.LoadPointer(&n._next)
}

func (n *node) casNext(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._next, old, new)
}

func (n *node) prev() unsafe.Pointer {
	return atomic.LoadPointer(&n._prev)
}

func (n *node) casPrev(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._prev, old, new)
}

func (s *SelectInstance) getState() unsafe.Pointer {
	return atomic.LoadPointer(&s.state)
}

func (s *SelectInstance) setState(state unsafe.Pointer) {
	atomic.StorePointer(&s.state, state)
}

func (s *SelectInstance) casState(old, new unsafe.Pointer) bool{
	return atomic.CompareAndSwapPointer(&s.state, old, new)
}

func IntType(p unsafe.Pointer) int32 {
	return *(*int32)(p)
}

func (n *node) casContinuation(index int32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._data[index * 2 + 1], old, new)
}

func (n *node) setContinuation(index int32, cont unsafe.Pointer) {
	atomic.StorePointer(&n._data[index * 2 + 1], cont)
}