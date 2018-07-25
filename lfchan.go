package go_lf_channels

import (
	"sync/atomic"
	"runtime"
	"unsafe"
)

func NewLFChan(segmentSize int32, spinThreshold int) *LFChan {
	emptyNode := (unsafe.Pointer) (newNode(segmentSize, 0))
	return &LFChan{
		segmentSize: segmentSize,
		spinThreshold: spinThreshold,
		_head: emptyNode,
		_tail: emptyNode,
	}
}

type LFChan struct {
	segmentSize   int32
	spinThreshold int

	_head unsafe.Pointer
	_tail unsafe.Pointer
}

func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceiveSuspend(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceiveSuspend(receiverElement)
}

// Main function in this chanel, which implements both `#send` and `#receive` operations.
// Note that `#sendOrReceiveNonSuspend` function is just a simplified versions of this.
func (c* LFChan) sendOrReceiveSuspend(element unsafe.Pointer) unsafe.Pointer {
	try_again: for {
		// Read the tail and its enqueue index at first, then the head and its indexes.
		// It is important to read tail and its index at first. If algorithm
		// realizes that the same continuations (senders or receivers) are stored
		// in the waiting queue, it can add the current continuation to the already
		// read tail index if it is not changed. In this case, it is guaranteed that
		// the waiting queue still has the same continuations or is empty.
		tail := c.getTail()
		tailEnqIdx := tail._enqIdx
		head := c.getHead()
		headDeqIdx := head._deqIdx
		headEnqIdx := head._enqIdx
		// If the waiting queue is empty, `headDeqIdx == headEnqIdx`.
		// This can also happen if the `head` node is full (`headDeqIdx == segmentSize`).
		if (headDeqIdx == headEnqIdx) {
			gp := runtime.GetGoroutine()
			if (headDeqIdx == c.segmentSize) {
				// The `head` node is full. Try to move `_head`
				// pointer forward and start the operation again.
				if (c.adjustHead(head)) { continue try_again }
				// Queue is empty, try to add a new node with the current continuation.
				runtime.AcqureMutex(gp)
				if (c.addNewNode(head, element)) {
					return parkAndThenReturn(element)
				} else {
					runtime.ReleaseMutex(gp)
				}
			} else {
				// The `head` node is not full, therefore the waiting queue
				// is empty. Try to add the current continuation to the queue.
				runtime.AcqureMutex(gp)
				if (storeContinuation(head, headEnqIdx, element)) {
					return parkAndThenReturn(element)
				} else {
					runtime.ReleaseMutex(gp)
				}
			}
		} else {
			// The waiting queue is not empty and it is guaranteed that `headDeqIdx < headEnqIdx`.
			// Try to remove the opposite continuation (a sender for a receiver or a receiver for a sender)
			// if waiting queue stores such in the `head` node at `headDeqIdx` index. In case the waiting
			// queue stores the same continuations, try to add the current continuation to it.
			//
			// In order to determine which continuations are stored, read the element from `head` node
			// at index `headDeqIdx`. When the algorithm add the continuation, it claims a slot at first,
			// stores the continuation and the element after that. This way, it is not guaranteed that
			// the first element is stored. The main idea is to spin on the value a bit and then change
			// element value from `null` to `TAKEN_ELEMENT` and increment the deque index if it is not appeared.
			// In this case the operation should start again. This simple  approach guarantees lock-freedom.
			firstElement := atomic.LoadPointer(&head._data[headDeqIdx * 2])
			if (firstElement == takenElement) {
				// Try to move the deque index in the `head` node
				atomic.CompareAndSwapInt32(&head._deqIdx, headDeqIdx, headDeqIdx + 1)
				continue try_again
			}
			// The `firstElement` is either sender or receiver. Check if a rendezvous is possible
			// and try to remove the first element in this case, try to add the current
			// continuation to the waiting queue otherwise.
			makeRendezvous := (element == receiverElement && firstElement != receiverElement) || (element != receiverElement && firstElement == receiverElement)
			// If removing the already read continuation fails (due to a failed CAS on moving `_deqIdx` forward)
			// it is possible not to try do the whole operation again, but to re-read new `_head` and its `_deqIdx`
			// values and try to remove this continuation if it is located between the already read deque
			// and enqueue positions. In this case it is guaranteed that the queue contains the same
			// continuation types as on making the rendezvous decision. The same optimization is possible
			// for adding the current continuation to the waiting queue if it fails.
			//headIdLimit := tail.id
			//headDeqIdxLimit := tailEnqIdx
			if (makeRendezvous) {
				for {
					if (tryResumeContinuation(head, headDeqIdx, element)) {
						// The rendezvous is happened, congratulations!
						// Resume the current continuation
						return firstElement
					} else { continue try_again }
					// Re-read the required pointers
					//read_state: for {
					//	// Re-read head pointer and its deque index
					//	head = c.getHead()
					//	headDeqIdx = head._deqIdx
					//	if (headDeqIdx == c.segmentSize) {
					//		if (!c.adjustHead(head)) { continue try_again }
					//		continue read_state
					//	}
					//	// Check that `(head.id, headDeqIdx) < (headIdLimit, headDeqIdxLimit)`
					//	// and re-start the whole operation if needed
					//	if (head.id > headIdLimit || (head.id == headIdLimit && headDeqIdx >= headDeqIdxLimit)) {
					//		continue try_again
					//	}
					//	// Re-read the first element
					//	firstElement = c.readElement(head, headDeqIdx)
					//	if (firstElement == takenElement) {
					//		atomic.CompareAndSwapInt32(&head._deqIdx, headDeqIdx, headDeqIdx + 1)
					//		continue read_state
					//	}
					//	break read_state
					//}
				}
			} else {
				for {
					// Try to add a new node with the current continuation and element
					// if the tail is full, otherwise try to store it at the `tailEnqIdx` index.
					gp := runtime.GetGoroutine()
					if (tailEnqIdx == c.segmentSize) {
						runtime.AcqureMutex(gp)
						if (c.addNewNode(tail, element)) {
							return parkAndThenReturn(element)
						} else {
							runtime.ReleaseMutex(gp)
							continue try_again
						}
					} else {
						runtime.AcqureMutex(gp)
						if (storeContinuation(tail, tailEnqIdx, element)) {
							return parkAndThenReturn(element)
						} else {
							runtime.ReleaseMutex(gp)
							continue try_again
						}
					}
					// Re-read the required pointers. Read tail and its indexes at first
					// and only then head with its indexes.
					//tail = c.getTail()
					//tailEnqIdx = tail._enqIdx
					//head = c.getHead()
					//headDeqIdx = head._deqIdx
					//if (head.id > headIdLimit || (head.id == headIdLimit && headDeqIdx >= headDeqIdxLimit)) {
					//	continue try_again
					//}
				}
			}
		}
	}
}

func parkAndThenReturn(sendElement unsafe.Pointer) unsafe.Pointer {
	if (sendElement == receiverElement) {
		runtime.ParkReceiveAndReleaseUnsafe()
	} else {
		runtime.ParkSendAndReleaseUnsafe()
	}
	return runtime.GetGParam(runtime.GetGoroutine())
}

// Tries to move `_head` pointer forward if the current node is full.
// Returns `false` if the waiting queue is empty, `true` on success.
func (c *LFChan) adjustHead(head *node) bool {
	// Read `_next` pointer and return `false` if the waiting queue is empty.
	headNext := head._next
	if (headNext == nil) { return false }
	// Move `_head` forward. If the CAS fails, another thread moved it.
	c.casHead(head, headNext)
	return true
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) addNewNode(tail *node, element unsafe.Pointer) bool {
	// If next node is not null, help to move the tail pointer
	tailNext := tail.getNext()
	if (tailNext != nil) {
		// If this CAS fails, another thread moved the tail pointer
		c.casTail(tail, tailNext)
		return false
	}
	// Create a new node with this continuation and element and try to add it
	node := newNode(c.segmentSize, tail.id + 1)
	node._data[0] = element
	node._data[1] = runtime.GetGoroutine()
	node._enqIdx = 1
	if (tail.casNext(node)) {
		// New node added, try to move tail,
		// if the CAS fails, another thread moved it.
		c.casTail(tail, node)
		return true
	} else {
		// Next node is not null, help to move the tail pointer
		c.casTail2(tail, tail._next)
		return false
	}
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
		if (element != nil) { return element }
		element = atomic.LoadPointer(&node._data[index * 2]) // volatile read
		attempt++
		if (attempt >= c.spinThreshold) {
			break
		}
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if (atomic.CompareAndSwapPointer(&node._data[index * 2], nil, takenElement)) {
		return takenElement
	} else {
		// The element is set, read it and return
		return node._data[index * 2]
	}
}

// Try to remove a continuation from the specified node at the
// specified index and resume it. Returns `true` on success, `false` otherwise.
func tryResumeContinuation(head *node, dequeIndex int32, element unsafe.Pointer) bool {
	// Try to move 'dequeIndex' forward, return `false` if fails
	if (atomic.CompareAndSwapInt32(&head._deqIdx, dequeIndex, dequeIndex + 1)) {
		// Get a continuation at the specified index and resume it
		gp := head._data[dequeIndex * 2 + 1]

		head._data[dequeIndex * 2] = takenElement
		head._data[dequeIndex * 2 + 1] = takenGoroutine

		runtime.SetGParam(gp, element)
		runtime.AcqureMutex(gp)
		runtime.UnparkUnsafe(gp)
		runtime.ReleaseMutex(gp)
		return true
	} else {
		return false
	}
}

// Tries to store the current continuation and element (in this order!)
// to the specified node at the specified index. Returns `true` on success,
// `false` otherwise`.
func storeContinuation(node *node, index int32, element unsafe.Pointer) bool {
	// Try to move enqueue index forward, return `false` if fails
	if !atomic.CompareAndSwapInt32(&node._enqIdx, index, index + 1) {
		return false
	}
	// Slot `index` is claimed, try to store the continuation and the element (in this order!) to it.
	// Can fail if another thread marked this slot as broken, return `false` in this case.
	node._data[index * 2 + 1] = runtime.GetGoroutine()
	if (atomic.CompareAndSwapPointer(&node._data[index * 2], nil, element)) {
		// Can be suspended, return true
		return true
	} else {
		// The slot is broken, clean it and return `false`
		node._data[index * 2 + 1] = takenGoroutine
		return false
	}
}

var takenGoroutine = (unsafe.Pointer) ((uintptr) (1))
var takenElement = (unsafe.Pointer) ((uintptr) (2))
var receiverElement = (unsafe.Pointer) ((uintptr) (3))

func newNode(segmentSize int32, id int64) *node {
	return &node{
		segmentSize: segmentSize,
		id: id,
		_deqIdx: 0,
		_enqIdx: 0, _next: nil,
		_data: make([]unsafe.Pointer, segmentSize * 2),
	}
}

type node struct {
	segmentSize int32
	id          int64

	_deqIdx     int32
	_enqIdx     int32
	_next       unsafe.Pointer
	_data       []unsafe.Pointer
}

func (n *node) getNext() *node {
	return (*node) (atomic.LoadPointer(&n._next))
}

func (n *node) casNext(newNext *node) bool {
	return atomic.CompareAndSwapPointer(&n._next, nil, (unsafe.Pointer) (newNext))
}