package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var broken = (unsafe.Pointer) ((uintptr) (1))
var fail = (unsafe.Pointer) ((uintptr) (2))
var confirmed = unsafe.Pointer(uintptr(3))


// == Channel structure ==

const LFChanPPoPPType int32 = 1094645093
type LFChanPPoPP struct {
	__type 	     int32
	lock    uint32
	highest uint64
	lowest  uint64
	_head    unsafe.Pointer
	_tail    unsafe.Pointer
}

func NewLFChanPPoPP(capacity uint64) *LFChanPPoPP {
	node := unsafe.Pointer(newNodePPoPP(0, nil))
	return &LFChanPPoPP{
		__type: LFChanPPoPPType,
		_head:    node,
		_tail:    node,
	}
}

// == segmentPPoPP structure ==

const segmentPPoPPSizeShift = uint32(10)
const segmentPPoPPSize = uint32(1 << segmentPPoPPSizeShift)
const segmentPPoPPIndexMask = uint64(segmentPPoPPSize - 1)

type segmentPPoPP struct {
	id       uint64
	_next    unsafe.Pointer
	_cleaned uint32
	_prev    unsafe.Pointer
	data     [segmentPPoPPSize*2]unsafe.Pointer
}

func newNodePPoPP(id uint64, prev *segmentPPoPP) *segmentPPoPP {
	return &segmentPPoPP{
		id:       id,
		_next:    nil,
		_cleaned: 0,
		_prev:    unsafe.Pointer(prev),
	}
}

func indexInNodePPoPP(index uint64) uint32 {
	return uint32((index * 1) & segmentPPoPPIndexMask)
}

func nodeIdPPoPP(index uint64) uint64 {
	return index >> segmentPPoPPSizeShift
}

// == `send` and `receive` functions ==

func (c *LFChanPPoPP) Send(element unsafe.Pointer) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incSendersAndGetSnapshot()
		if senders <= receivers {
			if c.tryResumeSimpleSend(head, senders, element) { return }
		} else {
			if c.trySuspendAndReturnSend(tail, senders, element) { return }
		}
	}
}

func (c *LFChanPPoPP) Receive() unsafe.Pointer {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResumeSimpleReceive(head, receivers)
			if result != fail {
				return result
			}
		} else {
			result := c.trySuspendAndReturnReceive(tail, receivers)
			if result != fail {
				return result
			}
		}
	}
}

func (c *LFChanPPoPP) tryResumeSimpleSend(head *segmentPPoPP, deqIdx uint64, element unsafe.Pointer) bool {
	head = c.getHead(nodeIdPPoPP(deqIdx), head)
	i := indexInNodePPoPP(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return false }

	if IntType(cont) != SelectPPoPPInstanceType {
		head.data[i * 2] = broken
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return true
	} else {
		SelectPPoPPInstance := (*SelectPPoPPInstance)(cont)

		sid := atomic.LoadUint64(&SelectPPoPPInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return false }
		head.data[i * 2] = broken

		if SelectPPoPPInstance.trySelectPPoPPSimple(sid, c, element) {
			return true
		} else {
			return false
		}
	}
}

func (c *LFChanPPoPP) tryResumeSimpleReceive(head *segmentPPoPP, deqIdx uint64) unsafe.Pointer {
	head = c.getHead(nodeIdPPoPP(deqIdx), head)
	i := indexInNodePPoPP(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return fail }

	if IntType(cont) != SelectPPoPPInstanceType {
		head.data[i * 2] = broken
		elementToReturn := runtime.GetGParam(cont)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	} else {
		elementToReturn := head.data[i * 2 + 1]
		head.data[i * 2 + 1] = nil
		SelectPPoPPInstance := (*SelectPPoPPInstance)(cont)

		sid := atomic.LoadUint64(&SelectPPoPPInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return fail }
		head.data[i * 2] = broken

		if SelectPPoPPInstance.trySelectPPoPPSimple(sid, c, ReceiverElement) {
			return elementToReturn
		} else {
			return fail
		}
	}
}

func (c *LFChanPPoPP) trySuspendAndReturnReceive(tail *segmentPPoPP, enqIdx uint64) unsafe.Pointer {
	tail = c.getTail(nodeIdPPoPP(enqIdx), tail)
	i := indexInNodePPoPP(enqIdx)
	curG := runtime.GetGoroutine()
	if atomic.LoadPointer(&tail.data[i * 2]) != nil { return fail }
	if !tail.casContinuation(i, nil, curG) {
		// the cell is broken
		return fail
	}
	runtime.ParkUnsafe(curG)
	result := runtime.GetGParam(curG)
	runtime.SetGParam(curG, nil)
	return result
}

func (c *LFChanPPoPP) trySuspendAndReturnSend(tail *segmentPPoPP, enqIdx uint64, element unsafe.Pointer) bool {
	tail = c.getTail(nodeIdPPoPP(enqIdx), tail)
	i := indexInNodePPoPP(enqIdx)
	curG := runtime.GetGoroutine()
	if atomic.LoadPointer(&tail.data[i * 2]) != nil { return false }
	runtime.SetGParam(curG, element)
	if !tail.casContinuation(i, nil, curG) {
		// the cell is broken
		runtime.SetGParam(curG, nil)
		return false
	}
	runtime.ParkUnsafe(curG)
	return true
}

func (c *LFChanPPoPP) trySuspendAndReturn(tail *segmentPPoPP, enqIdx uint64, element unsafe.Pointer) unsafe.Pointer {
	tail = c.getTail(nodeIdPPoPP(enqIdx), tail)
	i := indexInNodePPoPP(enqIdx)
	tail.data[i * 2 + 1] = element
	curG := runtime.GetGoroutine()
	if !tail.casContinuation(i, nil, curG) {
		// the cell is broken
		tail.data[i * 2 + 1] = nil
		return fail
	}
	runtime.ParkUnsafe(curG)
	result := runtime.GetGParam(curG)
	runtime.SetGParam(curG, nil)
	return result
}

func (c *LFChanPPoPP) getHead(id uint64, cur *segmentPPoPP) *segmentPPoPP {
	if cur.id == id { return cur }
	cur = c.findOrCreateNode(id, cur)
	cur._prev = nil
	c.moveHeadForward(cur)
	return cur
}

func (c *LFChanPPoPP) getTail(id uint64, cur *segmentPPoPP) *segmentPPoPP {
	return c.findOrCreateNode(id, cur)
}

func (c *LFChanPPoPP) findOrCreateNode(id uint64, cur *segmentPPoPP) *segmentPPoPP {
	for cur.id < id {
		curNext := cur.next()
		if curNext == nil {
			// add new segmentPPoPP
			newTail := newNodePPoPP(cur.id + 1, cur)
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

func (s *segmentPPoPP) readContinuation(i uint32) unsafe.Pointer {
	cont := atomic.LoadPointer(&s.data[i * 2])
	if cont == nil {
		if atomic.CompareAndSwapPointer(&s.data[i * 2], nil, broken) {
			return broken
		} else {
			return atomic.LoadPointer(&s.data[i * 2])
		}
	} else {
		return cont
	}
}


// === SelectPPoPP ===
const reg_added = uint8(0)
const reg_rendezvous = uint8(1)
const reg_confirmed = uint8(2)


func (c *LFChanPPoPP) regSelectPPoPP(SelectPPoPPInstance *SelectPPoPPInstance, element unsafe.Pointer) (uint8, RegInfoPPoPP) {
	if element == ReceiverElement {
		return c.regSelectPPoPPForReceive(SelectPPoPPInstance)
	} else {
		return c.regSelectPPoPPForSend(SelectPPoPPInstance, element)
	}
}

func (c *LFChanPPoPP) regSelectPPoPPForSend(SelectPPoPPInstance *SelectPPoPPInstance, element unsafe.Pointer) (uint8, RegInfoPPoPP) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incSendersAndGetSnapshot()
		if senders <= receivers {
			result := c.tryResumeSelectPPoPP(head, senders, element, SelectPPoPPInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfoPPoPP{}}
			return reg_rendezvous, RegInfoPPoPP{}
		} else {
			enqIdx := senders
			tail = c.getTail(nodeIdPPoPP(enqIdx), tail)
			i := indexInNodePPoPP(enqIdx)
			tail.data[i*2+1] = element
			if !tail.casContinuation(i, nil, unsafe.Pointer(SelectPPoPPInstance)) {
				// the cell is broken
				tail.data[i*2+1] = nil
				continue
			}
			return reg_added, RegInfoPPoPP{tail, i}
		}
	}
}

func (c *LFChanPPoPP) regSelectPPoPPForReceive(SelectPPoPPInstance *SelectPPoPPInstance) (uint8, RegInfoPPoPP) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResumeSelectPPoPP(head, receivers, ReceiverElement, SelectPPoPPInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfoPPoPP{}}
			runtime.SetGParam(SelectPPoPPInstance.gp, result)
			return reg_rendezvous, RegInfoPPoPP{}
		} else {
			enqIdx := receivers
			tail = c.getTail(nodeIdPPoPP(enqIdx), tail)
			i := indexInNodePPoPP(enqIdx)
			if !tail.casContinuation(i, nil, unsafe.Pointer(SelectPPoPPInstance)) {
				// the cell is broken
				continue
			}
			return reg_added, RegInfoPPoPP{tail, i}
		}
	}
}

func (c *LFChanPPoPP) tryResumeSelectPPoPP(head *segmentPPoPP, deqIdx uint64, element unsafe.Pointer, curSelectPPoPPInstance *SelectPPoPPInstance) unsafe.Pointer {
	head = c.getHead(nodeIdPPoPP(deqIdx), head)
	i := indexInNodePPoPP(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return fail }

	if IntType(cont) == SelectPPoPPInstanceType {
		elementToReturn := head.data[i * 2 + 1]
		head.data[i * 2 + 1] = nil

		SelectPPoPPInstance := (*SelectPPoPPInstance) (cont)

		sid := atomic.LoadUint64(&SelectPPoPPInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return fail }
		head.data[i * 2] = broken

		switch SelectPPoPPInstance.trySelectPPoPPFromSelectPPoPP(sid, curSelectPPoPPInstance, unsafe.Pointer(c), element) {
		case try_SelectPPoPP_sucess: return elementToReturn
		case try_SelectPPoPP_fail: return fail
		case try_SelectPPoPP_confirmed: return confirmed
		default: panic("Impossible")
		}
	} else {
		elementToReturn := runtime.GetGParam(cont)
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	}
}


// == Cleaning ==

func (s *segmentPPoPP) clean(index uint32) {
	//cont := s.readContinuation(index)
	//if cont == broken { return }
	//if !s.casContinuation(index, cont, broken) { return }
	atomic.StorePointer(&s.data[index * 2], broken)
	s.data[index * 2 + 1] = nil
	if atomic.AddUint32(&s._cleaned, 1) < segmentPPoPPSize { return }
	// Remove the segmentPPoPP
	s.remove()
}

func (n *segmentPPoPP) isRemoved() bool {
	return atomic.LoadUint32(&n._cleaned) == segmentPPoPPSize
}

func (n *segmentPPoPP) remove() {
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

func (n *segmentPPoPP) moveNextToTheRight(newNext *segmentPPoPP) {
	for {
		curNext := n.next()
		if curNext.id >= newNext.id { return }
		if n.casNext(unsafe.Pointer(curNext), unsafe.Pointer(newNext)) { return }
	}
}

func (n *segmentPPoPP) movePrevToTheLeft(newPrev *segmentPPoPP) {
	for {
		curPrev := n.prev()
		if newPrev != nil && curPrev.id <= newPrev.id { return }
		if n.casPrev(unsafe.Pointer(curPrev), unsafe.Pointer(newPrev)) { return }
	}
}