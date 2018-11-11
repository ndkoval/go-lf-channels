package main

import (
	"sync/atomic"
	"unsafe"
)

// == LFChan ==

func (c *LFChan) head() *segment {
	return (*segment)(atomic.LoadPointer(&c._head))
}

func (c *LFChan) tail() *segment {
	return (*segment)(atomic.LoadPointer(&c._tail))
}

func (c *LFChan) moveHeadForward(new *segment) {
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

func (c *LFChan) moveTailForward(new *segment) {
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

// == SelectInstance ==

func (s *SelectInstance) getState() unsafe.Pointer {
	return atomic.LoadPointer(&s.state)
}

func (s *SelectInstance) setState(newState unsafe.Pointer) {
	atomic.StorePointer(&s.state, newState)
}

func (s *SelectInstance) casState(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&s.state, old, new)
}

func IntType(p unsafe.Pointer) int32 {
	return *(*int32)(p)
}

// == segment ==

func (n *segment) casContinuation(index uint32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n.data[index * 2], old, new)
}

func (n *segment) next() *segment {
	return (*segment)(atomic.LoadPointer(&n._next))
}

func (n *segment) casNext(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._next, old, new)
}

func (n *segment) prev() *segment {
	return (*segment)(atomic.LoadPointer(&n._prev))
}

func (n *segment) casPrev(old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._prev, old, new)
}

