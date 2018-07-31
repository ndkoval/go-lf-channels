package go_lf_channels

import (
	"sync/atomic"
	"runtime"
	"unsafe"
	"sync"
)

func NewLFChan(spinThreshold int) *LFChan {
	emptyNode := (unsafe.Pointer) (newNode(0))
	return &LFChan{
		spinThreshold: spinThreshold,
		_deqIdx: 1,
		_enqIdx: 1,
		_head: emptyNode,
		_tail: emptyNode,
		mutex: sync.Mutex{},
	}
}

type LFChan struct {
	spinThreshold int

	_deqIdx int64
	_enqIdx int64

	_head unsafe.Pointer
	_tail unsafe.Pointer

	mutex sync.Mutex
}

func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceiveSuspend(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceiveSuspend(receiverElement)
}

func (c* LFChan) sendOrReceiveSuspend(element unsafe.Pointer) unsafe.Pointer {
	try_again: for { // CAS-loop
		enqIdx := c.enqIdx()
		deqIdx := c.deqIdx()
		if enqIdx < deqIdx { continue try_again }
		// Check if queue is empty
		if deqIdx == enqIdx {
			if c.addToWaitingQueue(enqIdx, element) {
				return parkAndThenReturn(element)
			} else { continue try_again }
		} else {
			// Queue is not empty
			head := c.getHead()
			headId := head.id
			deqIdxNodeId := nodeId(deqIdx)
			// Check that deqIdx is not outdated
			if headId > deqIdxNodeId {
				continue try_again
			}
			// Check that head pointer should be moved forward
			if headId < deqIdxNodeId {
				c.casHead(head, head.next())
				continue try_again
			}
			// Read the first element
			deqIdxInNode := indexInNode(deqIdx)
			firstElement := c.readElement(head, deqIdxInNode)
			// Check that the element is not taken already.
			if firstElement == takenElement {
				c.casDeqIdx(deqIdx, deqIdx + 1)
				continue try_again
			}
			// Decide should we make a rendezvous or not
			makeRendezvous := (element == receiverElement && firstElement != receiverElement) || (element != receiverElement && firstElement == receiverElement)
			//deqIdxLimit := enqIdx
			if makeRendezvous {
				if c.tryResumeContinuation(head, deqIdxInNode, deqIdx, element) {
					return firstElement
				} else { continue try_again }
			} else {
				//for {
					if c.addToWaitingQueue(enqIdx, element) {
						return parkAndThenReturn(element)
					} else { continue try_again }
					//enqIdx = c.enqIdx()
					//deqIdx = c.deqIdx()
					//if deqIdx >= deqIdxLimit { continue try_again }
				//}
			}
		}
	}
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) readElement(node *node, index int32) unsafe.Pointer {
	// Element index in `Node#_data` array
	// Spin wait on the slot
	element := atomic.LoadPointer(&node._data[index * 2]) // volatile read
	var attempt = 0
	for {
		if element != nil { return element }
		attempt++
		if attempt >= c.spinThreshold {
			break
		}
		element = atomic.LoadPointer(&node._data[index * 2]) // volatile read
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(&node._data[index * 2], nil, takenElement) {
		return takenElement
	} else {
		// The element is set, read it and return
		return node._data[index * 2]
	}
}


func (c *LFChan) addToWaitingQueue(enqIdx int64, element unsafe.Pointer) bool {
	// Count enqIdx parts
	enqIdxNodeId := nodeId(enqIdx)
	enqIdxInNode := indexInNode(enqIdx)
	// Read tail and its id
	tail := c.getTail()
	tailId := tail.id
	// Check if enqIdx is not outdated
	if tailId > enqIdxNodeId { return false }
	// Check if we should help with a new node adding
	if tailId == enqIdxNodeId && enqIdxInNode == 0 {
		c.casEnqIdx(enqIdx, enqIdx + 1)
		return false
	}
	// Check if a new node should be added
	if tailId == enqIdxNodeId - 1 && enqIdxInNode == 0 {
		return c.addNewNode(tail, element, enqIdx)
	}
	// Just check that `enqIdx` is valid and try to store the current
	// goroutine into the `tail` by `enqIdxInNode`
	if tailId != enqIdxNodeId { panic("Impossible!") }
	if enqIdxInNode == 0 { panic("Impossible 2!") }
	return c.storeContinuation(tail, enqIdxInNode, enqIdx, element)
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) addNewNode(tail *node, element unsafe.Pointer, enqIdx int64) bool {
	// If next node is not null, help to move the tail pointer
	tailNext := tail.getNext()
	if tailNext != nil {
		// If this CAS fails, another thread moved the tail pointer
		c.casTail(tail, tailNext)
		c.casEnqIdx(enqIdx, enqIdx + 1) // help
		return false
	}
	// Create a new node with this continuation and element and try to add it
	gp := runtime.GetGoroutine()
	node := newNode(tail.id + 1)
	node._data[0] = element
	node._data[1] = gp
	if tail.casNext(node) {
		// New node added, try to move tail,
		// if the CAS fails, another thread moved it.
		c.casTail(tail, node)
		c.casEnqIdx(enqIdx, enqIdx + 1) // help for others
		return true
	} else {
		// Next node is not null, help to move the tail pointer
		c.casTail2(tail, tail._next)
		c.casEnqIdx(enqIdx, enqIdx + 1) // help for others
		return false
	}
}

// Tries to store the current continuation and element (in this order!)
// to the specified node at the specified index. Returns `true` on success,
// `false` otherwise`.
func (c *LFChan) storeContinuation(node *node, indexInNode int32, enqIdx int64, element unsafe.Pointer) bool {
	// Try to move enqueue index forward, return `false` if fails
	if !c.casEnqIdx(enqIdx, enqIdx + 1) {
		return false
	}
	// Slot `index` is claimed, try to store the continuation and the element (in this order!) to it.
	// Can fail if another thread marked this slot as broken, return `false` in this case.
	gp := runtime.GetGoroutine()
	node._data[indexInNode * 2 + 1] = gp

	if atomic.CompareAndSwapPointer(&node._data[indexInNode * 2], nil, element) {
		// Can be suspended, return true
		return true
	} else {
		// The slot is broken, clean it and return `false`
		node._data[indexInNode * 2 + 1] = takenGoroutine
		return false
	}
}

// Try to remove a continuation from the specified node at the
// specified index and resume it. Returns `true` on success, `false` otherwise.
func (c *LFChan) tryResumeContinuation(head *node, indexInNode int32, deqIdx int64, element unsafe.Pointer) bool {
	// Try to move 'deqIdx' forward, return `false` if fails
	if c.casDeqIdx(deqIdx, deqIdx + 1) {
		// Get a continuation at the specified index and resume it
		gp := head._data[indexInNode * 2 + 1]

		head._data[indexInNode * 2] = takenElement
		head._data[indexInNode * 2 + 1] = takenGoroutine

		runtime.SetGParam(gp, element)
		runtime.UnparkUnsafe(gp)
		return true
	} else {
		return false
	}
}

func (c *LFChan) enqIdx() int64 {
	return atomic.LoadInt64(&c._enqIdx)
}

func (c *LFChan) deqIdx() int64 {
	return atomic.LoadInt64(&c._deqIdx)
}

func indexInNode(index int64) int32 {
	return int32(index % int64(segmentSize))
}

func nodeId(index int64) int64 {
	return index / int64(segmentSize)
}

func parkAndThenReturn(sendElement unsafe.Pointer) unsafe.Pointer {
	if sendElement == receiverElement {
		runtime.ParkReceiveAndReleaseUnsafe()
	} else {
		runtime.ParkSendAndReleaseUnsafe()
	}
	return runtime.GetGParam(runtime.GetGoroutine())
}

func (c *LFChan) casTail(oldTail *node, newTail *node) bool {
	return atomic.CompareAndSwapPointer(&c._tail, (unsafe.Pointer) (oldTail), (unsafe.Pointer) (newTail))
}

// Fuck this language
func (c *LFChan) casTail2(oldTail *node, newTail unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&c._tail, (unsafe.Pointer) (oldTail), newTail)
}

func (c *LFChan) getTail() *node {
	return (*node) (atomic.LoadPointer(&c._tail))
}

func (c *LFChan) getHead() *node {
	return (*node) (atomic.LoadPointer(&c._head))
}

func (c *LFChan) casHead(oldHead *node, newHead unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&c._head, (unsafe.Pointer) (oldHead), newHead)
}

func (c *LFChan) casEnqIdx(old int64, new int64) bool {
	return atomic.CompareAndSwapInt64(&c._enqIdx, old, new)
}

func (c *LFChan) casDeqIdx(old int64, new int64) bool {
	return atomic.CompareAndSwapInt64(&c._deqIdx, old, new)
}

var takenGoroutine = (unsafe.Pointer) ((uintptr) (1))
var takenElement = (unsafe.Pointer) ((uintptr) (2))
var receiverElement = (unsafe.Pointer) ((uintptr) (3))
const segmentSize = 32

func newNode(id int64) *node {
	return &node{
		id: id,
		_next: nil,
	}
}

type node struct {
	id          int64
	_next       unsafe.Pointer
	_data       [segmentSize * 2]unsafe.Pointer
}

func (n *node) next() unsafe.Pointer {
	return atomic.LoadPointer(&n._next)
}

func (n *node) getNext() *node {
	return (*node) (atomic.LoadPointer(&n._next))
}

func (n *node) casNext(newNext *node) bool {
	return atomic.CompareAndSwapPointer(&n._next, nil, (unsafe.Pointer) (newNext))
}
