package main

import (
	"sync/atomic"
	"unsafe"
)

// == LFChanPPoPP ==

func (c *LFChanPPoPP) head() *segmentPPoPP {
	return (*segmentPPoPP)(atomic.LoadPointer(&c._head))
}

func (c *LFChanPPoPP) tail() *segmentPPoPP {
	return (*segmentPPoPP)(atomic.LoadPointer(&c._tail))
}

func (c *LFChanPPoPP) moveHeadForward(new *segmentPPoPP) {
	for {
		cur := c.head()
		if cur.id > new.id {
			return
		}
		if atomic.CompareAndSwapPointer(&c._head, unsafe.Pointer(cur), unsafe.Pointer(new)) {
			return
		}
	}
}

func (c *LFChanPPoPP) moveTailForward(new *segmentPPoPP) {
	for {
		cur := c.tail()
		if cur.id > new.id {
			return
		}
		if atomic.CompareAndSwapPointer(&c._tail, unsafe.Pointer(cur), unsafe.Pointer(new)) {
			return
		}
	}
}

// == SelectPPoPPInstance ==

func (s *SelectPPoPPInstance) getState() unsafe.Pointer {
	return atomic.LoadPointer(&s.state)
}

func (s *SelectPPoPPInstance) setState(newState unsafe.Pointer) {
	atomic.StorePointer(&s.state, newState)
}

func (s *SelectPPoPPInstance) casState(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&s.state, old, new)
}

func IntType(p unsafe.Pointer) int32 {
	return *(*int32)(p)
}

// == segmentPPoPP ==

func (n *segmentPPoPP) casContinuation(index uint32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n.data[index * 2], old, new)
}

func (n *segmentPPoPP) next() *segmentPPoPP {
	return (*segmentPPoPP)(atomic.LoadPointer(&n._next))
}

func (n *segmentPPoPP) casNext(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._next, old, new)
}

func (n *segmentPPoPP) prev() *segmentPPoPP {
	return (*segmentPPoPP)(atomic.LoadPointer(&n._prev))
}

func (n *segmentPPoPP) casPrev(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._prev, old, new)
}

