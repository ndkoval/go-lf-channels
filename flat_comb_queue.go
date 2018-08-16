package main

import (
	"unsafe"
	"sync/atomic"
	"time"
)

type FCQueue struct {
	c *LFChan

	head unsafe.Pointer
	_tail unsafe.Pointer

	_fclock int32
}

type fcnode struct {
	deqIdx int32
	_enqIdx int32
	_next   unsafe.Pointer
	_data   [BUFFER_SIZE * 2]unsafe.Pointer
}

type fcnode_initial struct {
	deqIdx int32
	_enqIdx int32
	_next   unsafe.Pointer
}

const BUFFER_SIZE = 128
const UNLOCKED = 0
const LOCKED = 1

func NewFCQueue(c *LFChan) *FCQueue {
	initNode := &fcnode_initial{}
	initNode._enqIdx = BUFFER_SIZE
	initNode.deqIdx = BUFFER_SIZE
	return &FCQueue{
		c:       c,
		head:    unsafe.Pointer(initNode),
		_tail:   unsafe.Pointer(initNode),
		_fclock: UNLOCKED,
	}
}

func (q *FCQueue) addTask(element unsafe.Pointer, cont unsafe.Pointer) (node *fcnode, idx int32) {
	for {
		tail := q.getTail()
		idx := atomic.AddInt32(&tail._enqIdx, 1) - 1
		if idx > BUFFER_SIZE - 1 {
			if tail != q.getTail() { continue }
			tailNext := tail.getNext()
			if tailNext == nil {
				newTail := &fcnode{}
				newTail._enqIdx = 1
				newTail._data[0] = element
				newTail._data[1] = cont
				if atomic.CompareAndSwapPointer(&tail._next, nil, unsafe.Pointer(newTail)) {
					atomic.CompareAndSwapPointer(&q._tail, unsafe.Pointer(tail), unsafe.Pointer(newTail))
					return newTail, 0
				}
			} else {
				atomic.CompareAndSwapPointer(&q._tail, unsafe.Pointer(tail), unsafe.Pointer(tailNext))
			}
			continue
		}
		tail._data[idx * 2 + 1] = cont
		if atomic.CompareAndSwapPointer(&tail._data[idx * 2], nil, element) {
			return tail, idx
		} else {
			tail._data[idx * 2 + 1] = takenContinuation
		}
	}
}

func (q *FCQueue) addTaskAndCombine(element unsafe.Pointer, cont unsafe.Pointer) unsafe.Pointer {
	node, idx := q.addTask(element, cont)
	spin := 0
	for atomic.LoadPointer(&node._data[idx * 2 + 1]) != nil {
		if q.tryAcquireLock() {
			q.combine(node, idx)
			q._fclock = UNLOCKED
			break
		} else {
			spin++
			if spin > 30 {
				time.Sleep(700)
			}
		}
	}
	res := node._data[idx * 2]
	node._data[idx * 2] = nil
	if res == ParkResult {
		return parkAndThenReturn()
	} else {
		return res
	}
}

func (q *FCQueue) tryAcquireLock() bool {
	if atomic.LoadInt32(&q._fclock) == LOCKED { return false }
	return atomic.CompareAndSwapInt32(&q._fclock, UNLOCKED, LOCKED)
}

func (q *FCQueue) combine(limitNode *fcnode, limitIdx int32) {
	head := (*fcnode) (q.head)
	for {
		if head.deqIdx >= atomic.LoadInt32(&head._enqIdx) && head.getNext() == nil { return }
		idx := head.deqIdx
		head.deqIdx++
		if idx > BUFFER_SIZE - 1 {
			headNext := head.getNext()
			if headNext == nil { return }
			q.head = unsafe.Pointer(headNext)
			head = headNext
			continue
		}
		element := atomic.SwapPointer(&head._data[idx * 2], takenElement)
		if element == nil { continue }
		cont := head._data[idx * 2 + 1]
		var res unsafe.Pointer
		if IntType(cont) == SelectInstanceType {
			q.c.regSelectFC((*SelectInstance) (cont), element)
			res = nil
		} else { // *g
			res = q.c.sendOrReceiveFC(element, cont)
		}
		head._data[idx * 2] = res
		atomic.StorePointer(&head._data[idx * 2 + 1], nil)
		if limitNode == head && limitIdx == idx { return }
	}
}

func (n *fcnode) getNext() *fcnode {
	return (*fcnode) (atomic.LoadPointer(&n._next))
}

func (q *FCQueue) getTail() *fcnode {
	return (*fcnode) (atomic.LoadPointer(&q._tail))
}
