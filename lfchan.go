package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var done = (unsafe.Pointer) ((uintptr) (1))
var fail = (unsafe.Pointer) ((uintptr) (2))
var confirmed = unsafe.Pointer(uintptr(3))
var buffered = unsafe.Pointer(uintptr(4))
var resuming = unsafe.Pointer(uintptr(5))


// == Channel structure ==

const LFChanType int32 = 1094645093
type LFChan struct {
	__type 	     int32
	capacity uint64

	lock    uint32
	lowest  uint64
	hSenders uint64
	hReceivers uint64
	hBufferEnd uint64

	_head    unsafe.Pointer
	_tail    unsafe.Pointer
}

func NewLFChan(capacity uint64) *LFChan {
	node := unsafe.Pointer(newNode(0, nil))
	return &LFChan{
		__type: LFChanType,
		capacity: capacity,
		lowest: capacity << (2 * _counterOffset), // TODO fix overflowing here
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
		senders, receivers, bufferEnd := c.incSendersAndGetSnapshot()
		if senders <= receivers {
			if c.tryResumeSimple(head, senders, element) != fail { return }
		} else if senders <= bufferEnd {
			c.storeWaiter(tail, senders, element, buffered)
			return
		} else {
			curG := runtime.GetGoroutine()
			c.storeWaiter(tail, senders, element, curG)
			runtime.ParkUnsafe(curG)
			return
		}
	}
}

func (c *LFChan) Receive() unsafe.Pointer {
	startHead := c.head()
	startTail := c.tail()

	senders, receivers, startBufferEnd := c.incReceiversAndBufferEndAndGetSnapshot()
	if receivers <= senders {
		result := c.tryResumeSimple(startHead, receivers, ReceiverElement)
		if result != fail {
			if senders >= startBufferEnd { c.resumeNextWaitingSend0(startHead, startBufferEnd) }
			return result
		}
	} else {
		curG := runtime.GetGoroutine()
		c.storeWaiter(startTail, receivers, ReceiverElement, curG)
		runtime.ParkUnsafe(curG)
		res := runtime.GetGParam(curG)
		runtime.SetGParam(curG, nil)
		return res
	}

	for {
		head := c.head()
		tail := c.tail()
		senders, receivers, _ := c.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResumeSimple(head, receivers, ReceiverElement)
			if result != fail {
				if senders >= startBufferEnd { c.resumeNextWaitingSend0(startHead, startBufferEnd) }
				return result
			}
		} else {
			curG := runtime.GetGoroutine()
			c.storeWaiter(tail, receivers, ReceiverElement, curG)
			runtime.ParkUnsafe(curG)
			res := runtime.GetGParam(curG)
			runtime.SetGParam(curG, nil)
			return res
		}
	}
}

func (c *LFChan) resumeNextWaitingSend0(startHead *segment, startBufferEnd uint64) {
	segment := c.findOrCreateNode(nodeId(startBufferEnd), startHead)
	i := indexInNode(startBufferEnd)
	if c.makeBuffered(i, segment) { return }
	c.resumeNextWaitingSend()
}

func (c *LFChan) resumeNextWaitingSend() {
	segment := c.head()
	for {
		senders, _, bufferEnd := c.incBufferEndAndGetSnapshot()
		if senders < bufferEnd { return }
		segment = c.findOrCreateNode(nodeId(bufferEnd), segment)
		i := indexInNode(bufferEnd)
		if c.makeBuffered(i, segment) { return }
	}
}

// return true if we should finish
func (c *LFChan) makeBuffered(i uint32, s *segment) bool {
	w := s.readContinuation(i, resuming)
	el := s.data[i * 2]
	if el == ReceiverElement { return true }
	if w == done || w == buffered { return true }

	if IntType(w) != SelectInstanceType {
		runtime.UnparkUnsafe(w)
		atomic.StorePointer(&s.data[i * 2 + 1], buffered)
		return true
	} else {
		selectInstance := (*SelectInstance)(w)
		sid := selectInstance.id
		if selectInstance.trySelectSimple(sid, c, ReceiverElement) { return true }
	}
	return false
}

func (c *LFChan) tryResumeSimple(head *segment, deqIdx uint64, element unsafe.Pointer) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i, done)

	if cont == done { return fail }
	res := head.data[i * 2]
	head.data[i * 2] = nil

	if cont == buffered { return res }

	if IntType(cont) != SelectInstanceType {
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return res
	} else {
		selectInstance := (*SelectInstance)(cont)
		sid := selectInstance.id
		if selectInstance.trySelectSimple(sid, c, element) { return res } else { return fail }
	}
}

func (c *LFChan) storeWaiter(tail *segment, enqIdx uint64, element unsafe.Pointer, waiter unsafe.Pointer) {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	tail.data[i * 2] = element
	atomic.StorePointer(&tail.data[i * 2 + 1], waiter)
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

func (s *segment) readContinuation(i uint32, replacement unsafe.Pointer) unsafe.Pointer {
	x := 0
	for {
		cont := atomic.LoadPointer(&s.data[i * 2 + 1])
		if cont == nil || cont == resuming {
			x++;
			if x % 10000 == 0 { runtime.Gosched() }
			continue
		}
		if cont == done { return done }
		if atomic.CompareAndSwapPointer(&s.data[i * 2 + 1], cont, replacement) { return cont }
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
		senders, receivers, bufferEnd := c.incSendersAndGetSnapshot()
		if senders <= receivers {
			result := c.tryResumeSelect(head, senders, element, selectInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfo{}}
			return reg_rendezvous, RegInfo{}
		} else if senders <= bufferEnd {
			c.storeWaiter(tail, senders, element, buffered)
			return reg_rendezvous, RegInfo{}
		} else {
			tail = c.getTail(nodeId(senders), tail)
			c.storeWaiter(tail, senders, element, unsafe.Pointer(selectInstance))
			return reg_added, RegInfo{tail, indexInNode(senders)}
		}
	}
}

func (c *LFChan) regSelectForReceive(selectInstance *SelectInstance) (uint8, RegInfo) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers, _ := c.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResumeSelect(head, receivers, ReceiverElement, selectInstance)
			if result == fail { continue }
			if result == confirmed { return reg_confirmed, RegInfo{}}
			runtime.SetGParam(selectInstance.gp, result)
			return reg_rendezvous, RegInfo{}
		} else {
			tail = c.getTail(nodeId(receivers), tail)
			c.storeWaiter(tail, receivers, ReceiverElement, unsafe.Pointer(selectInstance))
			return reg_added, RegInfo{tail, indexInNode(receivers)}
		}
	}
}

func (c *LFChan) tryResumeSelect(head *segment, deqIdx uint64, element unsafe.Pointer, curSelectInstance *SelectInstance) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i, done)

	if cont == done { return fail }
	res := head.data[i * 2]
	head.data[i * 2] = nil

	if cont == buffered { return res }

	if IntType(cont) == SelectInstanceType {
		selectInstance := (*SelectInstance) (cont)

		sid := atomic.LoadUint64(&selectInstance.id)
		switch selectInstance.trySelectFromSelect(sid, curSelectInstance, unsafe.Pointer(c), element) {
		case try_select_sucess: return res
		case try_select_fail: return fail
		case try_select_confirmed: return confirmed
		default: panic("Impossible")
		}
	} else {
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return res
	}
}


// == Cleaning ==

func (s *segment) clean(index uint32) {
	//cont := s.readContinuation(index)
	//if cont == broken { return }
	//if !s.casContinuation(index, cont, broken) { return }
	atomic.StorePointer(&s.data[index * 2 + 1], done)
	s.data[index * 2] = nil
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