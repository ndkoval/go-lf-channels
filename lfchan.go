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

const LFChanType int32 = 1094645093
type LFChan struct {
	__type 	     int32
	capacity uint64
	_head    unsafe.Pointer
	_tail    unsafe.Pointer
	counters counters
}

func NewLFChan(capacity uint64) *LFChan {
	node := unsafe.Pointer(newNode(0, nil))
	return &LFChan{
		__type: LFChanType,
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
	conts    [segmentSize]unsafe.Pointer
	elements [segmentSize]unsafe.Pointer
}

func newNode(id uint64, prev *segment) *segment {
	return &segment{
		id:       id,
		_next:    nil,
		_cleaned: 0,
		_prev:    unsafe.Pointer(prev),
	}
}

//go:nosplit
func indexInNode(index uint64) uint32 {
	return uint32(index & segmentIndexMask)
}

//go:nosplit
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
			if c.tryResumeSimpleSend(head, senders, element) { return }
		} else {
			if c.trySuspendAndReturnSend(tail, senders, element, senders - receivers > c.capacity) { return }
		}
	}
}

func (c *LFChan) Receive() unsafe.Pointer {
	for {
		head := c.head()
		tail := c.tail()
		senders, receivers := c.counters.incReceiversAndGetSnapshot()
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
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return true
	} else {
		selectInstance := (*SelectInstance)(cont)
		if selectInstance.trySelectSimple(c, element) {
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
	if cont == _element {
		elementToReturn := head.elements[i]
		head.elements[i] = nil
		return elementToReturn
	}

	if IntType(cont) != SelectInstanceType {
		elementToReturn := runtime.GetGParam(cont)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	} else {
		elementToReturn := head.elements[i]
		head.elements[i] = nil
		selectInstance := (*SelectInstance)(cont)
		if selectInstance.trySelectSimple(c, ReceiverElement) {
			return elementToReturn
		} else {
			return fail
		}
	}
}

func (c *LFChan) tryResume(head *segment, deqIdx uint64, element unsafe.Pointer, thisSelectInstance *SelectInstance) unsafe.Pointer {
	head = c.getHead(nodeId(deqIdx), head)
	i := indexInNode(deqIdx)
	cont := head.readContinuation(i)

	elementToReturn := head.elements[i]
	head.elements[i] = nil

	if cont == broken { return fail }
	if cont == _element { return elementToReturn }

	if IntType(cont) == SelectInstanceType {
		selectInstance := (*SelectInstance) (cont)
		if thisSelectInstance == nil {
			if selectInstance.trySelectSimple(c, element) {
				return elementToReturn
			} else {
				return fail
			}
		} else {
			if selectInstance.trySelectFromSelect(thisSelectInstance, unsafe.Pointer(c), element) {
				return elementToReturn
			} else {
				return fail
			}
		}
	} else {
		elementToReturn = runtime.GetGParam(cont)
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return elementToReturn
	}
}

func (c *LFChan) trySuspendAndReturnReceive(tail *segment, enqIdx uint64) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	curG := runtime.GetGoroutine()
	if !tail.casContinuation(i, nil, curG) {
		// the cell is broken
		return fail
	}
	runtime.ParkUnsafe(curG)
	result := runtime.GetGParam(curG)
	runtime.SetGParam(curG, nil)
	return result
}

func (c *LFChan) trySuspendAndReturnSend(tail *segment, enqIdx uint64, element unsafe.Pointer, suspend bool) bool {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	if suspend {
		curG := runtime.GetGoroutine()
		runtime.SetGParam(curG, element)
		if !tail.casContinuation(i, nil, curG) {
			// the cell is broken
			runtime.SetGParam(curG, nil)
			return false
		}
		runtime.ParkUnsafe(curG)
		return true
	} else { // buffering
		tail.elements[i] = element
		if tail.casContinuation(i, nil, _element) {
			return true
		} else {
			tail.elements[i] = nil
			return false
		}
	}
}

func (c *LFChan) trySuspendAndReturn(tail *segment, enqIdx uint64, element unsafe.Pointer, suspend bool) unsafe.Pointer {
	tail = c.getTail(nodeId(enqIdx), tail)
	i := indexInNode(enqIdx)
	tail.elements[i] = element
	if suspend {
		curG := runtime.GetGoroutine()
		if !tail.casContinuation(i, nil, curG) {
			// the cell is broken
			tail.elements[i] = nil
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
			tail.elements[i] = nil
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
	cont := atomic.LoadPointer(&s.conts[i])
	if cont == nil {
		if atomic.CompareAndSwapPointer(&s.conts[i], nil, broken) {
			return broken
		} else {
			return atomic.LoadPointer(&s.conts[i])
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
			if c.tryResume(head, senders, element, selectInstance) != fail {
				runtime.SetGParam(selectInstance.gp, ReceiverElement)
				return false, RegInfo{}
			}
		} else {
			enqIdx := senders
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			tail.elements[i] = element
			if senders-receivers <= c.capacity { // do not suspend
				if tail.casContinuation(i, nil, _element) {
					return false, RegInfo{}
				} else {
					tail.elements[i] = nil
					continue
				}
			} else { // suspend
				if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
					// the cell is broken
					tail.elements[i] = nil
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
			result := c.tryResume(head, receivers, ReceiverElement, selectInstance)
			if result != fail {
				runtime.SetGParam(selectInstance.gp, result)
				return false, RegInfo{}
			}
		} else {
			enqIdx := receivers
			tail = c.getTail(nodeId(enqIdx), tail)
			i := indexInNode(enqIdx)
			if !tail.casContinuation(i, nil, unsafe.Pointer(selectInstance)) {
				// the cell is broken
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
	s.conts[index] = broken
	s.elements[index] = nil
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