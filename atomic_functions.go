package main

import (
	"sync/atomic"
	"unsafe"
)

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
	if p == nil { panic( "XXX") }
	return *(*int32)(p)
}

// == segment ==

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

