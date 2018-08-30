package main

import (
	"sync/atomic"
	"runtime"
	"unsafe"
	"math/rand"
	"time"
)


type LFChan struct {
	balance int64 // senders - receivers

	senders queue
	receivers queue
}

func NewLFChan( ) *LFChan {
	return &LFChan{
		balance:0,
		senders:newQueue(),
		receivers:newQueue(),
	}
}

func newQueue() queue {
	node := unsafe.Pointer(newNode(0, nil))
	return queue{
		deqIdx: 0,
		enqIdx: 0,
		_head: node,
		_tail: node,
	}
}

type queue struct {
	enqIdx uint64
	deqIdx uint64

	_head unsafe.Pointer
	_tail unsafe.Pointer
}

type node struct {
	id       uint64
	_next    unsafe.Pointer
	_cleaned uint32
	_prev    unsafe.Pointer
	data     [segmentSize]unsafe.Pointer
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
	index uint32
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
const segmentSizeShift = uint32(6)
const segmentSize = uint32(1 << segmentSizeShift)
const segmentIndexMask = uint64(segmentSize - 1)

var selectIdGen int64 = 0

func (q* queue) enq(cont unsafe.Pointer) bool {
	tail := q.tail()
	i := atomic.AddUint64(&q.enqIdx, 1)
	tail = q.getTail(nodeId(i), tail)
	indexInTail := indexInNode(i)
	return atomic.CompareAndSwapPointer(&tail.data[indexInTail], nil, cont)
}

func (q* queue) deq() unsafe.Pointer {
	head := q.head()
	i := atomic.AddUint64(&q.deqIdx, 1)
	head = q.getHead(nodeId(i), head)
	indexInHead := indexInNode(i)
	cont, _ := head.readContinuation(indexInHead, takenContinuation)
	return cont
}


func (c *LFChan) Send(element unsafe.Pointer) {
	try_again: for { // CAS-loop
		balance := atomic.AddInt64(&c.balance, 1)
		if balance <= 0 { // rendezvous
			cont := c.receivers.deq()
			if cont == takenContinuation {
				continue try_again
			}
			runtime.SetGParam(cont, element)
			runtime.UnparkUnsafe(cont)
			return
		} else {
			curG := runtime.GetGoroutine()
			runtime.SetGParam(curG, element)
			if c.senders.enq(curG) {
				runtime.ParkUnsafe()
				return
			} else {
				runtime.SetGParam(curG, nil)
				continue try_again
			}
		}
	}
	//
	//	head := c.head()
	//	tail := c.tail()
	//	state := atomic.AddUint64(&c.state, sendersInc)
	//	senders, receivers := getSendersAndReceivers(state)
	//	c.head()
	//	c.tail()
	//	if senders <= receivers {
	//		deqIdx := senders
	//		head = c.getHead(nodeId(deqIdx), head)
	//		i := indexInNode(deqIdx)
	//		var cont unsafe.Pointer
	//		var isSelectInstance bool
	//		for {
	//			cont, isSelectInstance = head.readContinuation(i, element)
	//			if cont == element {
	//				return // done
	//			}
	//			if cont == takenContinuation {
	//				continue try_again
	//			}
	//			if head.casContinuation(i, cont, takenContinuation) {
	//				break
	//			}
	//		}
	//
	//	} else {
	//		enqIdx := senders
	//		tail := c.getTail(nodeId(enqIdx), tail)
	//		i := indexInNode(enqIdx)
	//
	//		if atomic.CompareAndSwapPointer(&tail.data[i], nil, curG) {
	//			runtime.ParkUnsafe()
	//			return
	//		}
	//	}
	//}
}

func (c *LFChan) Receive() unsafe.Pointer {
	try_again: for { // CAS-loop
		balance := atomic.AddInt64(&c.balance, -1)
		if balance >= 0 { // rendezvous
			cont := c.senders.deq()
			if cont == takenContinuation {
				continue try_again
			}
			res := runtime.GetGParam(cont)
			runtime.UnparkUnsafe(cont)
			return res
		} else {
			curG := runtime.GetGoroutine()
			if c.receivers.enq(curG) {
				return parkAndThenReturn()
			} else {
				runtime.SetGParam(curG, nil)
				continue try_again
			}
		}
	}
	//try_again: for { // CAS-loop
	//	head := c.head()
	//	tail := c.tail()
	//	state := atomic.AddUint64(&c.state, receiversInc)
	//	senders, receivers := getSendersAndReceivers(state)
	//	c.head()
	//	c.tail()
	//	if receivers <= senders {
	//		deqIdx := receivers
	//		head = c.getHead(nodeId(deqIdx), head)
	//		i := indexInNode(deqIdx)
	//		var cont unsafe.Pointer
	//		var isSelectInstance bool
	//		for {
	//			cont, isSelectInstance = head.readContinuation(i, takenContinuation)
	//			if cont == takenContinuation {
	//				continue try_again
	//			}
	//			if head.casContinuation(i, cont, takenContinuation) {
	//				break
	//			}
	//		}
	//		if isSelectInstance {
	//			selectInstance := (*SelectInstance) (cont)
	//			if !selectInstance.trySetDescriptor(unsafe.Pointer(c)) {
	//				continue try_again
	//			}
	//			runtime.UnparkUnsafe(selectInstance.gp)
	//			return nil // todo find it
	//		} else {
	//			res := runtime.GetGParam(cont)
	//			runtime.SetGParam(cont, nil)
	//			runtime.UnparkUnsafe(cont)
	//			return res
	//		}
	//	} else {
	//		enqIdx := receivers
	//		tail := c.getTail(nodeId(enqIdx), tail)
	//		i := indexInNode(enqIdx)
	//		curG := runtime.GetGoroutine()
	//		if atomic.CompareAndSwapPointer(&tail.data[i], nil, curG) {
	//			return parkAndThenReturn()
	//		} else {
	//			res := atomic.LoadPointer(&tail.data[i])
	//			if res == takenElement { continue try_again }
	//			tail.data[i] = takenContinuation
	//			return res
	//		}
	//	}
	//}
}

func (c *queue) findOrCreateNode(id uint64, cur *node) *node {
	for cur.id < id {
		curNext := cur.next()
		if curNext == nil {
			// add new node
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

func (c *queue) getHead(id uint64, cur *node) *node {
	if cur.id == id { return cur }
	cur = c.findOrCreateNode(id, cur)
	cur._prev = nil
	c.moveHeadForward(cur)
	return cur
}

func (c *queue) getTail(id uint64, cur *node) *node {
	return c.findOrCreateNode(id, cur)
}

var consumedCPU = int32(time.Now().Unix())

func ConsumeCPU(tokens int) {
	t := int(atomic.LoadInt32(&consumedCPU)) // volatile read
	for i := tokens; i > 0; i-- {
		t += (t * 0x5DEECE66D + 0xB + i) & (0xFFFFFFFFFFFF)
	}
	if t == 42 { atomic.StoreInt32(&consumedCPU, consumedCPU + int32(t)) }
}

// Tries to read an element from the specified node
// at the specified index. Returns this element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (n *node) readElement(index uint32) unsafe.Pointer {
	// Element index in `Node#data` array
	// Spin wait on the slot
	elementAddr := &n.data[index * 2]
	element := atomic.LoadPointer(elementAddr) // volatile read
	if element != nil {
		return element
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(elementAddr, nil, takenElement) {
		return takenElement
	} else {
		// The element is set, read it and return
		return n.data[index * 2]
	}
}

func (n *node) readContinuation(i uint32, element unsafe.Pointer) (cont unsafe.Pointer, isSelectInstance bool) {
	for {
		cont := atomic.LoadPointer(&n.data[i])
		if cont == nil {
			if atomic.CompareAndSwapPointer(&n.data[i], nil, element) {
				return element, false
			} else { continue }
		}
		if cont == takenContinuation {
			return takenContinuation, false
		}
		contType := IntType(cont)
		switch contType {
		case SelectDescType:
			desc := (*SelectDesc)(cont)
			if desc.invoke() {
				atomic.StorePointer(&n.data[i], takenContinuation)
				return takenContinuation, false
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
	return false, RegInfo{}
	//try_again: for { // CAS-loop
	//	head := c.head()
	//	tail := c.tail()
	//	sendersAndReceivers := atomic.LoadUint64(&c.state)
	//	senders := sendersAndReceivers >> sendersOffset
	//	receivers := sendersAndReceivers & ((1 << sendersOffset) - 1)
	//	if (senders + 1) <= receivers {
	//		deqIdx := senders + 1
	//		head = c.getHead(nodeId(deqIdx), head)
	//		i := indexInNode(deqIdx)
	//		el := head.readElement(i)
	//		if el == takenElement {
	//			c.moveSendersForward(senders + 1)
	//			continue try_again
	//		}
	//		// Set descriptor at first
	//		var desc *SelectDesc
	//		var descCont unsafe.Pointer
	//		var isDescContSelectInstance bool
	//		for {
	//			descCont, isDescContSelectInstance = head.readContinuation(i, takenContinuation)
	//			if descCont == takenContinuation {
	//				c.moveSendersForward(senders + 1)
	//				continue try_again
	//			}
	//			desc = &SelectDesc {
	//				__type: SelectDescType,
	//				channel: c,
	//				selectInstance: selectInstance,
	//				cont: descCont,
	//			}
	//			if head.casContinuation(i, descCont, unsafe.Pointer(desc)) { break }
	//		}
	//		// Invoke selectDesc and update the continuation's cell
	//		if desc.invoke() {
	//			head.setContinuation(i, takenContinuation)
	//		} else {
	//			if !selectInstance.isSelected() {
	//				head.setContinuation(i, takenContinuation)
	//				c.moveSendersForward(senders + 1)
	//				continue try_again
	//			}
	//			head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
	//			return false, RegInfo{}
	//		}
	//		// Move deque index forward
	//		c.moveSendersForward(senders + 1)
	//		// Resume all continuations
	//		var anotherG unsafe.Pointer
	//		if isDescContSelectInstance {
	//			anotherG = ((*SelectInstance) (descCont)).gp
	//		} else {
	//			anotherG = descCont
	//		}
	//		runtime.SetGParam(anotherG, element)
	//		runtime.UnparkUnsafe(anotherG)
	//		runtime.SetGParam(selectInstance.gp, el)
	//		runtime.UnparkUnsafe(selectInstance.gp)
	//		return false, RegInfo{}
	//	} else {
	//		newSenderAndReceivers := (senders + 1) << sendersOffset + receivers
	//		if !atomic.CompareAndSwapUint64(&c.state, sendersAndReceivers, newSenderAndReceivers) {
	//			continue try_again
	//		}
	//		enqIdx := senders + 1
	//		tail := c.getTail(nodeId(enqIdx), tail)
	//		i := indexInNode(enqIdx)
	//		tail.data[i * 2 + 1] = unsafe.Pointer(selectInstance)
	//		if atomic.CompareAndSwapPointer(&tail.data[i * 2], nil, element) {
	//			return true, RegInfo{ tail, i }
	//		} else {
	//			continue try_again
	//		}
	//	}
	//}
}

func (c *LFChan) regSelectForReceive(selectInstance *SelectInstance) (added bool, regInfo RegInfo) {
	return false, RegInfo{}
	//try_again: for { // CAS-loop
	//	head := c.head()
	//	tail := c.tail()
	//	sendersAndReceivers := atomic.LoadUint64(&c.state)
	//	senders := sendersAndReceivers >> sendersOffset
	//	receivers := sendersAndReceivers & ((1 << sendersOffset) - 1)
	//	if (receivers + 1) <= senders {
	//		deqIdx := receivers + 1
	//		head = c.getHead(nodeId(deqIdx), head)
	//		i := indexInNode(deqIdx)
	//		el := head.readElement(i)
	//		if el == takenElement {
	//			c.moveReceiversForward(receivers + 1)
	//			continue try_again
	//		}
	//		// Set descriptor at first
	//		var desc *SelectDesc
	//		var descCont unsafe.Pointer
	//		var isDescContSelectInstance bool
	//		for {
	//			descCont, isDescContSelectInstance = head.readContinuation(i, takenContinuation)
	//			if descCont == takenContinuation {
	//				c.moveReceiversForward(receivers + 1)
	//				continue try_again
	//			}
	//			desc = &SelectDesc {
	//				__type: SelectDescType,
	//				channel: c,
	//				selectInstance: selectInstance,
	//				cont: descCont,
	//			}
	//			if head.casContinuation(i, descCont, unsafe.Pointer(desc)) { break }
	//		}
	//		// Invoke selectDesc and update the continuation's cell
	//		if desc.invoke() {
	//			head.setContinuation(i, takenContinuation)
	//		} else {
	//			if !selectInstance.isSelected() {
	//				head.setContinuation(i, takenContinuation)
	//				c.moveReceiversForward(receivers + 1)
	//				continue try_again
	//			}
	//			head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
	//			return false, RegInfo{}
	//		}
	//		// Move deque index forward
	//		c.moveReceiversForward(receivers + 1)
	//		// Resume all continuations
	//		var anotherG unsafe.Pointer
	//		if isDescContSelectInstance {
	//			anotherG = ((*SelectInstance) (descCont)).gp
	//		} else {
	//			anotherG = descCont
	//		}
	//		runtime.SetGParam(anotherG, ReceiverElement)
	//		runtime.UnparkUnsafe(anotherG)
	//		runtime.SetGParam(selectInstance.gp, el)
	//		runtime.UnparkUnsafe(selectInstance.gp)
	//		return false, RegInfo{}
	//	} else {
	//		newSenderAndReceivers := senders << sendersOffset + (receivers + 1)
	//		if !atomic.CompareAndSwapUint64(&c.state, sendersAndReceivers, newSenderAndReceivers) {
	//			continue try_again
	//		}
	//		enqIdx := receivers + 1
	//		tail := c.getTail(nodeId(enqIdx), tail)
	//		i := indexInNode(enqIdx)
	//		tail.data[i * 2 + 1] = unsafe.Pointer(selectInstance)
	//		if atomic.CompareAndSwapPointer(&tail.data[i * 2], nil, ReceiverElement) {
	//			return true, RegInfo{ tail, i }
	//		} else {
	//			continue try_again
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

func clean(node *node, index uint32) {
	cont, _ := node.readContinuation(index, takenContinuation)
	if cont == takenContinuation { return }
	if !node.casContinuation(index, cont, takenContinuation) { return }
	atomic.StorePointer(&node.data[index * 2], takenElement)
	if atomic.AddUint32(&node._cleaned, 1) < segmentSize { return }
	// Remove the node
	node.remove()
}

func (n *node) isRemoved() bool {
	return atomic.LoadUint32(&n._cleaned) == segmentSize
}

func (n *node) remove() {
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

func (n *node) moveNextToTheRight(newNext *node) {
	for {
		curNext := n.next()
		if curNext.id >= newNext.id { return }
		if n.casNext(unsafe.Pointer(curNext), unsafe.Pointer(newNext)) { return }
	}
}

func (n *node) movePrevToTheLeft(newPrev *node) {
	for {
		curPrev := n.prev()
		if newPrev != nil && curPrev.id <= newPrev.id { return }
		if n.casPrev(unsafe.Pointer(curPrev), unsafe.Pointer(newPrev)) { return }
	}
}

// === FUCKING GOLANG ===

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

func (q *queue) head() *node {
	return (*node) (atomic.LoadPointer(&q._head))
}

func (q *queue) tail() *node {
	return (*node) (atomic.LoadPointer(&q._tail))
}

func (q *queue) moveHeadForward(new *node) {
	for {
		cur := q.head()
		if cur.id > new.id { return }
		if atomic.CompareAndSwapPointer(&q._head, unsafe.Pointer(cur), unsafe.Pointer(new)) { return }
	}
}

func (q *queue) moveTailForward(new *node) {
	for {
		cur := q.tail()
		if cur.id > new.id { return }
		if atomic.CompareAndSwapPointer(&q._tail, unsafe.Pointer(cur), unsafe.Pointer(new)) { return }
	}
}

func (sd *SelectDesc) getStatus() int32 {
	return atomic.LoadInt32(&sd.status)
}

func (sd *SelectDesc) setStatus(status int32) {
	atomic.StoreInt32(&sd.status, status)
}

func (n *node) next() *node {
	return (*node) (atomic.LoadPointer(&n._next))
}

func (n *node) casNext(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._next, old, new)
}

func (n *node) prev() *node {
	return (*node) (atomic.LoadPointer(&n._prev))
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

func (n *node) casContinuation(index uint32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n.data[index], old, new)
}

func (n *node) setContinuation(index uint32, cont unsafe.Pointer) {
	atomic.StorePointer(&n.data[index], cont)
}