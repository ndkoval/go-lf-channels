package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var broken = (unsafe.Pointer) ((uintptr) (1))
var fail = (unsafe.Pointer) ((uintptr) (2))
var _element = (unsafe.Pointer) ((uintptr) (3))

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
	cont := head.readContinuation(i)

	elementToReturn := head.data[i * 2 + 1]
	head.data[i * 2 + 1] = nil

	if cont == broken { return fail }
	if cont == _element { return elementToReturn }

	if IntType(cont) == SelectInstanceType {
		selectInstance := (*SelectInstance) (cont)
		selected, non_suspended := selectInstance.trySetState(unsafe.Pointer(c), false)
		if !selected { return fail }
		runtime.SetGParam(selectInstance.gp, element)
		if non_suspended {
			selectInstance.trySetState(state_finished2, true)
		} else {
			runtime.UnparkUnsafe(selectInstance.gp)
		}
		return elementToReturn
	} else {
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	}
}

func (c *LFChan) trySuspendAndReturn(tail *segment, enqIdx uint64, element unsafe.Pointer, suspend bool) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	tail.data[i * 2 + 1] = element
	if suspend {
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
	} else { // buffering
		if tail.casContinuation(i, nil, _element) {
			return nil
		} else {
			tail.data[i * 2 + 1] = nil
			return fail
		}
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

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	if element == ReceiverElement {
		return c.regSelectForReceive(selectInstance)
	} else {
		return c.regSelectForSend(selectInstance, element)
	}
}

func (c *LFChan) regSelectForSend(selectInstance *SelectInstance, element unsafe.Pointer) (bool, RegInfo) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.incSendersAndGetSnapshot()
		if senders <= receivers {
			if c.tryResume(head, senders, element) != fail {
				selectInstance.trySetState(unsafe.Pointer(c), true)
				runtime.UnparkUnsafe(selectInstance.gp)
				return false, RegInfo{}
			}
		} else {
			enqIdx := senders
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			tail.data[i*2+1] = element
			if senders-receivers <= c.capacity { // do not suspend
				if tail.casContinuation(i, nil, _element) {
					selectInstance.trySetState(unsafe.Pointer(c), true)
					runtime.UnparkUnsafe(selectInstance.gp)
					return false, RegInfo{}
				} else {
					tail.data[i*2+1] = nil
					continue
				}
			} else { // suspend
				if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
					// the cell is broken
					tail.data[i*2+1] = nil
					continue
				}
				return true, RegInfo{tail, i}
			}
		}
	}
}

func (c *LFChan) regSelectForReceive(selectInstance *SelectInstance) (added bool, regInfo RegInfo) {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.incReceiversAndGetSnapshot()
		if receivers <= senders {
			result := c.tryResume(head, receivers, nil)
			if result != fail {
				selectInstance.trySetState(unsafe.Pointer(c), true)
				runtime.SetGParam(selectInstance.gp, result)
				runtime.UnparkUnsafe(selectInstance.gp)
				return false, RegInfo{}
			}
		} else {
			enqIdx := receivers
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
				// the cell is broken
				tail.data[i*2+1] = nil
				continue
			}
			return true, RegInfo{tail, i}
		}
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