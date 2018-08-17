package main

import (
	"sync/atomic"
	"runtime"
	"unsafe"
	"math/rand"
	"time"
)

const sendersOffset = 32
type LFChan struct {
	sendersAndReceivers uint64

	_data [1 << 25]unsafe.Pointer
}

func NewLFChan( ) *LFChan {
	return &LFChan{}
}

type node struct {
	id          uint64
	_next       unsafe.Pointer
	_cleaned 	uint32
	_prev 		unsafe.Pointer
}

func newNode(id uint64, prev *node) *node {
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
var ParkResult = (unsafe.Pointer) ((uintptr) (4097))
const segmentSizeShift = uint32(25)
const segmentSize = uint32(1 << segmentSizeShift)
const segmentIndexMask = uint64(segmentSize - 1)

var selectIdGen int64 = 0


func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceive(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceive(ReceiverElement)
}

const maxBackoffMaskShift = 10
var consumedCPU = int32(time.Now().Unix())

func ConsumeCPU(tokens int) {
	t := int(atomic.LoadInt32(&consumedCPU)) // volatile read
	for i := tokens; i > 0; i-- {
		t += (t * 0x5DEECE66D + 0xB + i) & (0xFFFFFFFFFFFF)
	}
	if t == 42 { atomic.StoreInt32(&consumedCPU, consumedCPU + int32(t)) }
}

func (c* LFChan) sendOrReceive(element unsafe.Pointer) unsafe.Pointer {
	try_again: for { // CAS-loop
		var sendersAndReceivers uint64
		if element == ReceiverElement {
			sendersAndReceivers = atomic.AddUint64(&c.sendersAndReceivers, 1)
		} else {
			sendersAndReceivers = atomic.AddUint64(&c.sendersAndReceivers, 1 << sendersOffset)
		}
		senders := sendersAndReceivers >> sendersOffset
		receivers := sendersAndReceivers & ((1 << sendersOffset) - 1)
		//println(element == ReceiverElement, senders, receivers)
		var deqIdx uint64
		var enqIdx uint64
		var makeRendezvous bool
		if senders < receivers {
			deqIdx = senders
			enqIdx = receivers
			makeRendezvous = element != ReceiverElement
		} else if senders > receivers {
			deqIdx = receivers
			enqIdx = senders
			makeRendezvous = element == ReceiverElement
		} else {
			makeRendezvous = true
			deqIdx = senders
		}
		if !makeRendezvous {
			c._data[enqIdx * 2 + 1] = runtime.GetGoroutine()
			if atomic.CompareAndSwapPointer(&c._data[enqIdx * 2], nil, element) {
				return parkAndThenReturn()
			}
		} else {
			el := c.readElement(deqIdx)
			if el == takenElement { continue try_again }
			cont := c._data[deqIdx * 2 + 1]
			c._data[deqIdx * 2 + 1] = takenContinuation
			c._data[deqIdx * 2] = takenElement
			runtime.SetGParam(cont, element)
			runtime.UnparkUnsafe(cont)
			return el
		}
	}
}

// Tries to read an element from the specified node
// at the specified index. Returns this element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) readElement(index uint64) unsafe.Pointer {
	// Element index in `Node#_data` array
	// Spin wait on the slot
	elementAddr := &c._data[index * 2]
	element := atomic.LoadPointer(elementAddr) // volatile read
	if element != nil {
		return element
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(elementAddr, nil, takenElement) {
		return takenElement
	} else {
		// The element is set, read it and return
		return c._data[index * 2]
	}
}

func (c *LFChan) tryResumeContinuationForSelect(head *node, indexInNode int32, deqIdx int64, element unsafe.Pointer, selectInstance *SelectInstance, firstElement unsafe.Pointer) bool {
	// Set descriptor at first
	//var desc *SelectDesc
	//var descCont unsafe.Pointer
	//var isDescContSelectInstance bool
	//for {
	//	descCont, isDescContSelectInstance = head.readContinuation(indexInNode)
	//	if descCont == takenContinuation {
	//		c.casDeqIdx(deqIdx, deqIdx + 1)
	//		return false
	//	}
	//	desc = &SelectDesc {
	//		__type: SelectDescType,
	//		channel: c,
	//		selectInstance: selectInstance,
	//		cont: descCont,
	//	}
	//	if head.casContinuation(indexInNode, descCont, unsafe.Pointer(desc)) { break }
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
	//var anotherG unsafe.Pointer
	//if isDescContSelectInstance {
	//	anotherG = ((*SelectInstance) (descCont)).gp
	//} else {
	//	anotherG = descCont
	//}
	//runtime.SetGParam(anotherG, element)
	//runtime.UnparkUnsafe(anotherG)
	//runtime.SetGParam(selectInstance.gp, firstElement)
	//runtime.UnparkUnsafe(selectInstance.gp)
	//return true
	return false
}

func (n *node) readContinuation(index uint32) (cont unsafe.Pointer, isSelectInstance bool) {
	return takenContinuation, false
	//contPointer := &n._data[index * 2 + 1]
	//for {
	//	cont := atomic.LoadPointer(contPointer)
	//	if cont == takenContinuation {
	//		return takenContinuation, false
	//	}
	//	contType := IntType(cont)
	//	switch contType {
	//	case SelectDescType:
	//		desc := (*SelectDesc)(cont)
	//		if desc.invoke() {
	//			atomic.StorePointer(contPointer, takenContinuation)
	//			return takenContinuation, false
	//		} else {
	//			atomic.CompareAndSwapPointer(contPointer, cont, desc.cont)
	//			return desc.cont, IntType(desc.cont) == SelectInstanceType
	//		}
	//	case SelectInstanceType: // *SelectInstance
	//		return cont, true
	//	default: // *g
	//		return cont, false
	//	}
	//}
}


// === SELECT ===

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	//r := int32(uintptr(element))
	//backoffMaskShift := -5
	//try_again: for { // CAS-loop
	//	if selectInstance.isSelected() { return false, RegInfo{} }
	//	enqIdx := c.enqIdx()
	//	deqIdx := c.deqIdx()
	//	if enqIdx < deqIdx {
	//		backoffMaskShift++
	//		if backoffMaskShift > 0 {
	//			if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//			r ^= r << 13
	//			r ^= r >> 17
	//			r ^= r << 5
	//			backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//			ConsumeCPU(int(backoff))
	//		}
	//		continue try_again
	//	}
	//	// Check if queue is empty
	//	if deqIdx == enqIdx {
	//		addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
	//		if addSuccess {
	//			return true, regInfo
	//		} else {
	//			backoffMaskShift++
	//			if backoffMaskShift > 0 {
	//				if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//				r ^= r << 13
	//				r ^= r >> 17
	//				r ^= r << 5
	//				backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//				ConsumeCPU(int(backoff))
	//			}
	//			continue try_again
	//		}
	//	} else {
	//		// Queue is not empty
	//		head := c.getHead()
	//		headId := head.id
	//		// check consistency
	//		deqIdxNodeId := nodeId(deqIdx)
	//		if headId != deqIdxNodeId {
	//			if headId < deqIdxNodeId {
	//				headNext := head.next()
	//				headNextNode := (*node) (headNext)
	//				headNextNode._prev = nil
	//				c.casHead(head, headNext)
	//			} else {
	//				c.casDeqIdx(deqIdx, headId << segmentSizeShift)
	//				backoffMaskShift++
	//				if backoffMaskShift > 0 {
	//					if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//					r ^= r << 13
	//					r ^= r >> 17
	//					r ^= r << 5
	//					backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//					ConsumeCPU(int(backoff))
	//				}
	//			}
	//			continue try_again
	//		}
	//		// Read the first element
	//		deqIndexInHead := indexInNode(deqIdx)
	//		firstElement := head.readElement(deqIndexInHead)
	//		// Check that the element is not taken already.
	//		if firstElement == takenElement {
	//			c.casDeqIdx(deqIdx, deqIdx + 1)
	//			backoffMaskShift++
	//			if backoffMaskShift > 0 {
	//				if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//				r ^= r << 13
	//				r ^= r >> 17
	//				r ^= r << 5
	//				backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//				ConsumeCPU(int(backoff))
	//			}
	//			continue try_again
	//		}
	//		// Decide should we make a rendezvous or not
	//		makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
	//		if makeRendezvous {
	//			if c.tryResumeContinuationForSelect(head, deqIndexInHead, deqIdx, element, selectInstance, firstElement) {
	//				return false, RegInfo{}
	//			} else {
	//				backoffMaskShift++
	//				if backoffMaskShift > 0 {
	//					if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//					r ^= r << 13
	//					r ^= r >> 17
	//					r ^= r << 5
	//					backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//					ConsumeCPU(int(backoff))
	//				}
	//				continue try_again
	//			}
	//		} else {
	//			addSuccess, regInfo := c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance))
	//			if addSuccess {
	//				return true, regInfo
	//			} else {
	//				backoffMaskShift++
	//				if backoffMaskShift > 0 {
	//					if  backoffMaskShift > maxBackoffMaskShift { backoffMaskShift = maxBackoffMaskShift }
	//					r ^= r << 13
	//					r ^= r >> 17
	//					r ^= r << 5
	//					backoff := int(r) & ((1 << uint(backoffMaskShift)) - 1)
	//					ConsumeCPU(int(backoff))
	//				}
	//				continue try_again
	//			}
	//		}
	//	}
	//}
	return false, RegInfo{}
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
	if failed {
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
	//cont, _ := node.readContinuation(index)
	//if cont == takenContinuation { return }
	//if !node.casContinuation(index, cont, takenContinuation) { return }
	//atomic.StorePointer(&node._data[index * 2], takenElement)
	//if atomic.AddInt32(&node._cleaned, 1) < segmentSize { return }
	//// Remove the node
	//node.remove()
}

func (n *node) isRemoved() bool {
	return false
	//return atomic.LoadInt32(&n._cleaned) == segmentSize
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

//func (c *LFChan) enqIdx() int64 {
//	return atomic.LoadInt64(&c._enqIdx)
//}

//func (c *LFChan) deqIdx() int64 {
//	return atomic.LoadInt64(&c._deqIdx)
//}

func indexInNode(index uint64) uint32 {
	return uint32(index & segmentIndexMask)
}

func nodeId(index uint64) uint64 {
	return index >> segmentSizeShift
}

func parkAndThenReturn() unsafe.Pointer {
	runtime.ParkUnsafe()
	return runtime.GetGParam(runtime.GetGoroutine())
}
//
//func (c *LFChan) casTail(oldTail *node, newTail *node) bool {
//	return atomic.CompareAndSwapPointer(&c._tail, (unsafe.Pointer) (oldTail), (unsafe.Pointer) (newTail))
//}
//
//func (c *LFChan) getTail() *node {
//	return (*node) (atomic.LoadPointer(&c._tail))
//}
//
//func (c *LFChan) getHead() *node {
//	return (*node) (atomic.LoadPointer(&c._head))
//}
//
//func (c *LFChan) casHead(oldHead *node, newHead unsafe.Pointer) bool {
//	return atomic.CompareAndSwapPointer(&c._head, (unsafe.Pointer) (oldHead), newHead)
//}

//func (c *LFChan) casEnqIdx(old int64, new int64) bool {
//	return atomic.CompareAndSwapInt64(&c._enqIdx, old, new)
//}

//func (c *LFChan) casDeqIdx(old int64, new int64) bool {
//	return atomic.CompareAndSwapInt64(&c._deqIdx, old, new)
//}

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
//
//func (n *node) casContinuation(index uint32, old, new unsafe.Pointer) bool {
//	return atomic.CompareAndSwapPointer(&n._data[index * 2 + 1], old, new)
//}
//
//func (n *node) setContinuation(index int32, cont unsafe.Pointer) {
//	atomic.StorePointer(&n._data[index * 2 + 1], cont)
//}