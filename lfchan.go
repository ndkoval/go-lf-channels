package main

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const sendersOffset = 32
const receiversMask = (1 << sendersOffset) - 1

const sendersInc = 1 << sendersOffset
const receiversInc = 1

const overflowed = 1 << 31

type SRCounter struct {
	lowest  uint64
	highest uint64
	lock    uint32
}
const W_LOCKED = 1 << 30

func (c *SRCounter) h() uint64 { return atomic.LoadUint64(&c.highest) }
func (c *SRCounter) l() uint64 { return atomic.LoadUint64(&c.lowest) }

func (c *SRCounter) acquireReadLock() {
	lock := atomic.AddUint32(&c.lock, 1)
	for lock > W_LOCKED {
		lock = atomic.LoadUint32(&c.lock)
	}
}

func (c *SRCounter) releaseReadLock() {
	atomic.AddUint32(&c.lock, ^uint32(0))
}

func (c *SRCounter) tryAcquireWriteLock() bool {
	lock := c.lock
	if lock != 0 { return false }
	return atomic.CompareAndSwapUint32(&c.lock, 0, W_LOCKED)
}

func (c *SRCounter) releaseWriteLock() {
	atomic.AddUint32(&c.lock, ^uint32(W_LOCKED - 1))
}


func counters(lowest uint64, highest uint64) (senders uint64, receivers uint64) {
	ls := lowest >> sendersOffset
	lr := lowest & receiversMask
	hs := highest >> sendersOffset
	hr := highest & receiversMask
	senders = ls + (hs << 31)
	receivers = lr + (hr << 31)
	return
}

func (c *SRCounter) getSnapshot() (senders uint64, receivers uint64) {
	for {
		h := c.h()
		l := c.l()
		if c.h() != h { continue }
		return counters(l, h)
	}
}

func (c *SRCounter) tryIncSendersFrom(sendersFrom uint64, receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.h()
		l := c.l()
		s, r := counters(l, h)
		if s == sendersFrom && r == receiversFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + sendersInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}


func (c *SRCounter) incSendersFrom(sendersFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.h()
		l := c.l()
		s, _ := counters(l, h)
		if s == sendersFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + sendersInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *SRCounter) tryIncReceiversFrom(sendersFrom uint64, receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.h()
		l := c.l()
		s, r := counters(l, h)
		if r == receiversFrom && s == sendersFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + receiversInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *SRCounter) incReceiversFrom(receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.h()
		l := c.l()
		_, r := counters(l, h)
		if r == receiversFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + receiversInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *SRCounter) incSendersAndGetSnapshot() (senders uint64, receivers uint64) {
	c.acquireReadLock()
	l := atomic.AddUint64(&c.lowest, sendersInc)
	h := c.h()
	c.releaseReadLock()
	if (l >> sendersOffset) >= overflowed {
		for {
			if c.tryAcquireWriteLock() {
				if c.h() == h {
					c.lowest -= overflowed << sendersOffset
					c.highest = h + 1 // todo hl, hr
				}
				c.releaseWriteLock()
			} else {
				if c.h() != h { break }
			}
		}
	}
	return counters(l, h)
}

func (c *SRCounter) incReceiversAndGetSnapshot() (senders uint64, receivers uint64) {
	c.acquireReadLock()
	l := atomic.AddUint64(&c.lowest, receiversInc)
	h := c.h()
	c.releaseReadLock()
	if (l & receiversMask) >= overflowed {
		for {
			if c.tryAcquireWriteLock() {
				if c.h() == h {
					c.lowest -= overflowed
					c.highest = h + 1
				}
				c.releaseWriteLock()
			} else {
				if c.h() != h { break }
			}
		}
	}
	return counters(l, h)
}


type LFChan struct {
	capacity uint64
	counter SRCounter

	_head unsafe.Pointer
	_tail unsafe.Pointer
}

func NewLFChan(capacity uint64) *LFChan {
	node := unsafe.Pointer(newNode(0, nil))
	return &LFChan{
		capacity: capacity,
		_head: node,
		_tail: node,
	}
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
var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
const segmentSizeShift = uint32(6)
const segmentSize = uint32(1 << segmentSizeShift)
const segmentIndexMask = uint64(segmentSize - 1)

var selectIdGen int64 = 0

func (c *LFChan) Send(element unsafe.Pointer) {
	try_again: for { // CAS-loop
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counter.incSendersAndGetSnapshot()
		if senders <= receivers {
			deqIdx := senders
			head = c.getHead(nodeId(deqIdx), head)
			i := indexInNode(deqIdx)
			cont, isSelectInstance := head.readContinuation(i)
			if cont == takenContinuation { continue try_again }
			if isSelectInstance {
				selectInstance := (*SelectInstance) (cont)
				if !selectInstance.trySetDescriptor(unsafe.Pointer(c)) {
					continue try_again
				}
				runtime.SetGParam(selectInstance.gp, element)
				runtime.UnparkUnsafe(selectInstance.gp)
			} else {
				runtime.SetGParam(cont, element)
				runtime.UnparkUnsafe(cont)
			}
			return
		} else {
			enqIdx := senders
			tail := c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if senders - receivers <= c.capacity {
				// buffering
				if atomic.CompareAndSwapPointer(&tail.data[i], nil, element) {
					return
				} else { continue try_again }
			} else {
				curG := runtime.GetGoroutine()
				runtime.SetGParam(curG, element)
				if atomic.CompareAndSwapPointer(&tail.data[i], nil, curG) {
					runtime.ParkUnsafe()
					return
				} else { continue try_again }
			}
		}
	}
}

func (c *LFChan) Receive() unsafe.Pointer {
	try_again: for { // CAS-loop
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counter.incReceiversAndGetSnapshot()
		if receivers <= senders {
			deqIdx := receivers
			head = c.getHead(nodeId(deqIdx), head)
			i := indexInNode(deqIdx)
			var cont unsafe.Pointer
			var isSelectInstance bool
			for {
				cont, isSelectInstance = head.readContinuation(i)
				if cont == takenContinuation {
					continue try_again
				}
				if head.casContinuation(i, cont, takenContinuation) {
					break
				}
			}
			if isSelectInstance {
				selectInstance := (*SelectInstance) (cont)
				if !selectInstance.trySetDescriptor(unsafe.Pointer(c)) {
					continue try_again
				}
				runtime.UnparkUnsafe(selectInstance.gp)
				return nil // todo find it
			} else {
				if isElement(cont) { return cont }

				res := runtime.GetGParam(cont)
				runtime.SetGParam(cont, nil)
				runtime.UnparkUnsafe(cont)
				return res
			}
		} else {
			enqIdx := receivers
			tail := c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			curG := runtime.GetGoroutine()
			if atomic.CompareAndSwapPointer(&tail.data[i], nil, curG) {
				return parkAndThenReturn()
			} else {
				continue try_again
				res := atomic.LoadPointer(&tail.data[i])
				if res == takenContinuation { continue try_again }
				tail.data[i] = takenContinuation
				return res
			}
		}
	}
}

func (c *LFChan) findOrCreateNode(id uint64, cur *node) *node {
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

func (c *LFChan) getHead(id uint64, cur *node) *node {
	if cur.id == id { return cur }
	cur = c.findOrCreateNode(id, cur)
	cur._prev = nil
	c.moveHeadForward(cur)
	return cur
}

func (c *LFChan) getTail(id uint64, cur *node) *node {
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

func (n *node) readContinuation(i uint32) (cont unsafe.Pointer, isSelectInstance bool) {
	for {
		cont := atomic.LoadPointer(&n.data[i])
		if cont == nil {
			if atomic.CompareAndSwapPointer(&n.data[i], nil, takenContinuation) {
				return takenContinuation, false
			} else { continue }
		}
		if cont == takenContinuation {
			return takenContinuation, false
		}
		if isElement(cont) {
			return cont, false
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
	try_again: for { // CAS-loop
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counter.getSnapshot()
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
				if descCont == takenContinuation {
					c.counter.incSendersFrom(senders)
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
				head.setContinuation(i, takenContinuation)
			} else {
				if !selectInstance.isSelected() {
					head.setContinuation(i, takenContinuation)
					c.counter.incSendersFrom(senders)
					continue try_again
				}
				head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
				return false, RegInfo{}
			}
			// Move deque index forward
			c.counter.incSendersFrom(senders)
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
			if !c.counter.tryIncSendersFrom(senders, receivers) { continue try_again }
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
		senders, receivers := c.counter.getSnapshot()
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
				if descCont == takenContinuation {
					c.counter.incReceiversFrom(receivers)
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
				head.setContinuation(i, takenContinuation)
			} else {
				if !selectInstance.isSelected() {
					head.setContinuation(i, takenContinuation)
					c.counter.incReceiversFrom(receivers)
					continue try_again
				}
				head.casContinuation(i, unsafe.Pointer(desc), desc.cont)
				return false, RegInfo{}
			}
			// Move deque index forward
			c.counter.incReceiversFrom(receivers)
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
			if !c.counter.tryIncReceiversFrom(senders, receivers) { continue try_again }
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

func (sd *SelectDesc) invoke() bool {
	curStatus := sd.getStatus()
	if curStatus != UNDECIDED { return curStatus == SUCCEEDED }
	// Phase 1 -- set descriptor to the select's state,
	// help for others if needed.
	selectInstance := sd.selectInstance
	anotherCont := sd.cont
	anotherContType := int32(-1)
	if !isElement(anotherCont) { anotherContType = IntType(anotherCont) }
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
	cont, _ := node.readContinuation(index)
	if cont == takenContinuation { return }
	if !node.casContinuation(index, cont, takenContinuation) { return }
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

func (c *LFChan) head() *node {
	return (*node) (atomic.LoadPointer(&c._head))
}

func (c *LFChan) tail() *node {
	return (*node) (atomic.LoadPointer(&c._tail))
}

func (c *LFChan) moveHeadForward(new *node) {
	for {
		cur := c.head()
		if cur.id > new.id { return }
		if atomic.CompareAndSwapPointer(&c._head, unsafe.Pointer(cur), unsafe.Pointer(new)) { return }
	}
}

func (c *LFChan) moveTailForward(new *node) {
	for {
		cur := c.tail()
		if cur.id > new.id { return }
		if atomic.CompareAndSwapPointer(&c._tail, unsafe.Pointer(cur), unsafe.Pointer(new)) { return }
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

func isElement(cont unsafe.Pointer) bool {
	return uintptr(cont) < 0xc000000000
}