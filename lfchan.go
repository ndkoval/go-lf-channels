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

const LFChanType int32 = 1094645093
type LFChan struct {
	__type 	     int32
	lock    uint32
	highest uint64
	lowest  uint64
	_head    unsafe.Pointer
	_tail    unsafe.Pointer
}

func NewLFChan(capacity uint64) *LFChan {
	node := unsafe.Pointer(newNode(0, nil))
	return &LFChan{
		__type: LFChanType,
		_head:    node,
		_tail:    node,
	}
}

// == Segment structure ==

const segmentSizeShift = uint32(10)
const segmentSize = uint32(1 << segmentSizeShift)
const segmentIndexMask = uint64(segmentSize - 1)

type segment struct {
	id       uint64
	_next    unsafe.Pointer
	_cleaned uint32
	_prev    unsafe.Pointer
	data     [segmentSize*2]unsafe.Pointer
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
	return uint32((index * 1) & segmentIndexMask)
}

func nodeId(index uint64) uint64 {
	return index >> segmentSizeShift
}

// == `send` and `receive` functions ==

func (c *LFChan) Send(element unsafe.Pointer) {
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

func (c *LFChan) Receive() unsafe.Pointer {
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

func (c *LFChan) tryResumeSimpleSend(head *segment, deqIdx uint64, element unsafe.Pointer) bool {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return false }

	if IntType(cont) != SelectInstanceType {
		head.data[i * 2] = broken
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return true
	} else {
		selectInstance := (*SelectInstance)(cont)

		sid := atomic.LoadUint64(&selectInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return false }
		head.data[i * 2] = broken

		if selectInstance.trySelectSimple(sid, c, element) {
			return true
		} else {
			return false
		}
	}
}

func (c *LFChan) tryResumeSimpleReceive(head *segment, deqIdx uint64) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return fail }

	if IntType(cont) != SelectInstanceType {
		head.data[i * 2] = broken
		elementToReturn := runtime.GetGParam(cont)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	} else {
		elementToReturn := head.data[i * 2 + 1]
		head.data[i * 2 + 1] = nil
		selectInstance := (*SelectInstance)(cont)

		sid := atomic.LoadUint64(&selectInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return fail }
		head.data[i * 2] = broken

		if selectInstance.trySelectSimple(sid, c, ReceiverElement) {
			return elementToReturn
		} else {
			return fail
		}
	}
}

func (c *LFChan) trySuspendAndReturnReceive(tail *segment, enqIdx uint64) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
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

func (c *LFChan) trySuspendAndReturnSend(tail *segment, enqIdx uint64, element unsafe.Pointer) bool {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
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

func (c *LFChan) trySuspendAndReturn(tail *segment, enqIdx uint64, element unsafe.Pointer) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
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

func (s *segment) readContinuation(i uint32) unsafe.Pointer {
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


// === SELECT ===
const reg_added = uint8(0)
const reg_rendezvous = uint8(1)
const reg_confirmed = uint8(2)


func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (uint8, RegInfo) {
	if element == ReceiverElement {
		return c.regSelectForReceive(selectInstance)
	} else {
		return c.regSelectForSend(selectInstance, element)
	}
}

func (c *LFChan) regSelectForSend(selectInstance *SelectInstance, element unsafe.Pointer) (uint8, RegInfo) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incSendersAndGetSnapshot()
		if senders <= receivers {
			result := c.tryResumeSelect(head, senders, element, selectInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfo{}}
			return reg_rendezvous, RegInfo{}
		} else {
			enqIdx := senders
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			tail.data[i*2+1] = element
			if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
				// the cell is broken
				tail.data[i*2+1] = nil
				continue
			}
			return reg_added, RegInfo{tail, i}
		}
	}
}

func (c *LFChan) regSelectForReceive(selectInstance *SelectInstance) (uint8, RegInfo) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResumeSelect(head, receivers, ReceiverElement, selectInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfo{}}
			runtime.SetGParam(selectInstance.gp, result)
			return reg_rendezvous, RegInfo{}
		} else {
			enqIdx := receivers
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
				// the cell is broken
				continue
			}
			return reg_added, RegInfo{tail, i}
		}
	}
}

func (c *LFChan) tryResumeSelect(head *segment, deqIdx uint64, element unsafe.Pointer, curSelectInstance *SelectInstance) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i)

	if cont == broken { return fail }

	if IntType(cont) == SelectInstanceType {
		elementToReturn := head.data[i * 2 + 1]
		head.data[i * 2 + 1] = nil

		selectInstance := (*SelectInstance) (cont)

		sid := atomic.LoadUint64(&selectInstance.id)
		if atomic.LoadPointer(&head.data[i * 2]) == broken { return fail }
		head.data[i * 2] = broken

		switch selectInstance.trySelectFromSelect(sid, curSelectInstance, unsafe.Pointer(c), element) {
		case try_select_sucess: return elementToReturn
		case try_select_fail: return fail
		case try_select_confirmed: return confirmed
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

func (s *segment) clean(index uint32) {
	//cont := s.readContinuation(index)
	//if cont == broken { return }
	//if !s.casContinuation(index, cont, broken) { return }
	atomic.StorePointer(&s.data[index * 2], broken)
	s.data[index * 2 + 1] = nil
	if atomic.AddUint32(&s._cleaned, 1) < segmentSize { return }
	// Remove the segment
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
		if newPrev != nil && curPrev.id <= newPrev.id { return }
		if n.casPrev(unsafe.Pointer(curPrev), unsafe.Pointer(newPrev)) { return }
	}
}