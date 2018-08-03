package go_lf_channels

import (
	"sync/atomic"
	"runtime"
	"unsafe"
	"math/rand"
)

func NewLFChan(spinThreshold int) *LFChan {
	emptyNode := (unsafe.Pointer) (newNode(0))
	return &LFChan{
		spinThreshold: spinThreshold,
		_deqIdx: 1,
		_enqIdx: 1,
		_head: emptyNode,
		_tail: emptyNode,
	}
}

type LFChan struct {
	spinThreshold int

	_deqIdx int64
	_enqIdx int64

	_head unsafe.Pointer
	_tail unsafe.Pointer
}

type node struct {
	id          int64
	_next       unsafe.Pointer
	_data       [segmentSize * 2]unsafe.Pointer
}

func newNode(id int64) *node {
	return &node{
		id: id,
		_next: nil,
	}
}

var takenContinuation = (unsafe.Pointer) ((uintptr) (1))
var takenElement = (unsafe.Pointer) ((uintptr) (2))
var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
const segmentSize = 32


func (c *LFChan) Send(element unsafe.Pointer) {
	c.sendOrReceiveSuspend(element)
}

func (c *LFChan) Receive() unsafe.Pointer {
	return c.sendOrReceiveSuspend(ReceiverElement)
}

func (c* LFChan) sendOrReceiveSuspend(element unsafe.Pointer) unsafe.Pointer {
	gp := runtime.GetGoroutine()
	try_again: for { // CAS-loop
		enqIdx := c.enqIdx()
		deqIdx := c.deqIdx()
		if enqIdx < deqIdx { continue try_again }
		// Check if queue is empty
		if deqIdx == enqIdx {
			if c.addToWaitingQueue(enqIdx, element, gp) {
				return parkAndThenReturn()
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
			makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
			deqIdxLimit := enqIdx
			if makeRendezvous {
				if c.tryResumeContinuation(head, deqIdxInNode, deqIdx, element) {
					return firstElement
				} else { continue try_again }
			} else {
				for {
					if c.addToWaitingQueue(enqIdx, element, gp) {
						return parkAndThenReturn()
					} else { continue try_again }
					enqIdx = c.enqIdx()
					deqIdx = c.deqIdx()
					if deqIdx >= deqIdxLimit { continue try_again }
				}
			}
		}
	}
}

// Tries to read an element from the specified node
// at the specified index. Returns this element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) readElement(node *node, index int32) unsafe.Pointer {
	// Element index in `Node#_data` array
	// Spin wait on the slot
	elementAddr := &node._data[index * 2]
	element := atomic.LoadPointer(elementAddr) // volatile read
	var attempt = 0
	for {
		if element != nil { return element }
		attempt++
		if attempt >= c.spinThreshold {
			break
		}
		element = atomic.LoadPointer(elementAddr) // volatile read
	}
	// Cannot spin forever, mark the slot as broken if it is still unavailable
	if atomic.CompareAndSwapPointer(elementAddr, nil, takenElement) {
		return takenElement
	} else {
		// The element is set, read it and return
		return node._data[index * 2]
	}
}


func (c *LFChan) addToWaitingQueue(enqIdx int64, element unsafe.Pointer, cont unsafe.Pointer) bool {
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
		return c.addNewNode(tail, element, enqIdx, cont)
	}
	// Just check that `enqIdx` is valid and try to store the current
	// goroutine into the `tail` by `enqIdxInNode`
	if tailId != enqIdxNodeId { panic("Impossible!") }
	if enqIdxInNode == 0 { panic("Impossible 2!") }
	return c.storeContinuation(tail, enqIdxInNode, enqIdx, element, cont)
}

// Tries to read an element from the specified node
// at the specified index. Returns the read element or
// marks the slot as broken (sets `TAKEN_ELEMENT` to the slot)
// and returns `TAKEN_ELEMENT` if the element is unavailable.
func (c *LFChan) addNewNode(tail *node, element unsafe.Pointer, enqIdx int64, cont unsafe.Pointer) bool {
	// If next node is not null, help to move the tail pointer
	tailNext := tail.getNext()
	if tailNext != nil {
		// If this CAS fails, another thread moved the tail pointer
		c.casTail(tail, tailNext)
		c.casEnqIdx(enqIdx, enqIdx + 1) // help
		return false
	}
	// Create a new node with this continuation and element and try to add it
	node := newNode(tail.id + 1)
	node._data[0] = element
	node._data[1] = cont
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
func (c *LFChan) storeContinuation(node *node, indexInNode int32, enqIdx int64, element unsafe.Pointer, cont unsafe.Pointer) bool {
	// Try to move enqueue index forward, return `false` if fails
	if !c.casEnqIdx(enqIdx, enqIdx + 1) {
		return false
	}
	// Slot `index` is claimed, try to store the continuation and the element (in this order!) to it.
	// Can fail if another thread marked this slot as broken, return `false` in this case.
	node._data[indexInNode * 2 + 1] = cont

	if atomic.CompareAndSwapPointer(&node._data[indexInNode * 2], nil, element) {
		// Can be suspended, return true
		return true
	} else {
		// The slot is broken, clean it and return `false`
		node._data[indexInNode * 2 + 1] = takenContinuation
		return false
	}
}

// Try to remove a continuation from the specified node at the
// specified index and resume it. Returns `true` on success, `false` otherwise.
func (c *LFChan) tryResumeContinuation(head *node, indexInNode int32, deqIdx int64, element unsafe.Pointer) bool {
	// Try to move 'deqIdx' forward, return `false` if fails
	if !c.casDeqIdx(deqIdx, deqIdx + 1) { return false }
	// Read continuation and CAS it to `takenContinuation`
	var cont unsafe.Pointer
	for {
		cont = head.readContinuation(indexInNode)
		if cont == takenContinuation { return false }
		if head.casContinuation(indexInNode, cont, takenContinuation) { break }
	}
	// Clear element's cell
	head._data[indexInNode * 2] = takenElement
	// Try to resume the continuation
	contType := IntType(cont)
	if contType == SelectInstanceType {
		selectInstance := (*SelectInstance) (cont)
		if !selectInstance.trySelect2(unsafe.Pointer(c)) { return false }
		runtime.SetGParam(selectInstance.gp, element)
		runtime.UnparkUnsafe(selectInstance.gp)
		return true
	} else { // *g
		runtime.SetGParam(cont, element)
		runtime.UnparkUnsafe(cont)
		return true
	}
}

func (c *LFChan) tryResumeContinuationForSelect(head *node, indexInNode int32, deqIdx int64, element unsafe.Pointer, selectInstance *SelectInstance, firstElement unsafe.Pointer) bool {
	// Set descriptor at first
	var desc *SelectDesc
	for {
		cont := head.readContinuation(indexInNode)
		if cont == takenContinuation {
			c.casDeqIdx(deqIdx, deqIdx + 1)
			return false
		}
		desc = &SelectDesc {
			__type: SelectDescType,
			channel: c,
			selectInstance: selectInstance,
			cont: cont,
		}
		if head.casContinuation(indexInNode, cont, unsafe.Pointer(desc)) { break }
	}
	// Invoke selectDesc and update the continuation's cell
	if desc.invoke() {
		head.casContinuation(indexInNode, unsafe.Pointer(desc), takenContinuation)
	} else {
		head.casContinuation(indexInNode, unsafe.Pointer(desc), desc.cont)
		return false
	}
	// Move deque index forward
	c.casDeqIdx(deqIdx, deqIdx + 1)
	// Change selectInstance' state
	selectInstance.setState(unsafe.Pointer(c))
	// Resume all continuations
	anotherCont := desc.cont
	var anotherG unsafe.Pointer
	anotherContType := IntType(anotherCont)
	if anotherContType == SelectInstanceType {
		anotherG = ((*SelectInstance) (anotherCont)).gp
	} else {
		anotherG = anotherCont
	}
	runtime.SetGParam(anotherG, element)
	runtime.UnparkUnsafe(anotherG)
	runtime.SetGParam(selectInstance.gp, firstElement)
	runtime.UnparkUnsafe(selectInstance.gp)
	return true
}

func (n *node) casContinuation(index int32, old, new unsafe.Pointer) bool {
	return atomic.CompareAndSwapPointer(&n._data[index * 2 + 1], old, new)
}

func (n *node) readContinuation(index int32) unsafe.Pointer {
	contPointer := &n._data[index * 2 + 1]
	for {
		cont := atomic.LoadPointer(contPointer)
		if cont == takenContinuation {
			return cont
		}
		contType := IntType(cont)
		switch contType {
		case SelectInstanceType:
			return cont
		case SelectDescType:
			desc := (*SelectDesc)(cont)
			if desc.invoke() {
				atomic.StorePointer(contPointer, takenContinuation)
				return takenContinuation
			} else {
				atomic.CompareAndSwapPointer(contPointer, cont, desc.cont)
				return desc.cont
			}
		default: // *g
			return cont
		}
	}
}


// === SELECT ===

func (c *LFChan) regSelect(selectInstance *SelectInstance, element unsafe.Pointer) {
	try_again: for { // CAS-loop
		enqIdx := c.enqIdx()
		deqIdx := c.deqIdx()
		if enqIdx < deqIdx { continue try_again }
		// Check if queue is empty
		if deqIdx == enqIdx {
			if c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance)) {
				return
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
			makeRendezvous := (element == ReceiverElement && firstElement != ReceiverElement) || (element != ReceiverElement && firstElement == ReceiverElement)
			deqIdxLimit := enqIdx
			if makeRendezvous {
				if c.tryResumeContinuationForSelect(head, deqIdxInNode, deqIdx, element, selectInstance, firstElement) {
					return
				} else { continue try_again }
			} else {
				for {
					if c.addToWaitingQueue(enqIdx, element, unsafe.Pointer(selectInstance)) {
						return
					} else { continue try_again }
					enqIdx = c.enqIdx()
					deqIdx = c.deqIdx()
					if deqIdx >= deqIdxLimit { continue try_again }
				}
			}
		}
	}
}

type SelectAlternative struct {
	channel *LFChan
	element unsafe.Pointer
	action  func(result unsafe.Pointer)
}

const SelectInstanceType int32 = 1298498092
type SelectInstance struct {
	__type int32
	alternatives *[]SelectAlternative
	state unsafe.Pointer
	gp	  unsafe.Pointer // goroutine
}

const SelectDescType int32 = 2019727883
type SelectDesc struct {
	__type int32
	channel *LFChan
	selectInstance *SelectInstance
	cont unsafe.Pointer // either *SelectInstance or *g

	status int32 // 0 -- UNDECIDED, 1 -- SUCCESS, 2 -- FAIL
}
const UNDECIDED = 0
const SUCCEEDED = 1
const FAILED = 2

func (sd *SelectDesc) invoke() bool {
	curStatus := sd.getStatus()
	if curStatus != UNDECIDED { return curStatus == SUCCEEDED }
	// Phase 1 -- set descriptor to the select's state,
	// help for others if needed.
	selectInstance := sd.selectInstance
	for {
		selectState := selectInstance.readState(sd)
		if selectState == unsafe.Pointer(sd) { break } // set already
		if selectState == nil {
			if selectInstance.casState(nil, unsafe.Pointer(sd)) { break }
		} else { // *LFChan
			if sd.getStatus() == SUCCEEDED {
				return true
			} else {
				sd.setStatus(FAILED)
				return false
			}
		}
	}
	// Phase 2 -- try to make a rendezvous with another continuation
	anotherCont := sd.cont
	anotherContType := IntType(anotherCont)
	if anotherContType == SelectInstanceType {
		anotherSelectInstance := (*SelectInstance) (anotherCont)
		if !anotherSelectInstance.trySelect(unsafe.Pointer(sd.channel)) {
			state := anotherSelectInstance.getState()
			println("XXXX ", state, "  ", sd.selectInstance, "   ", IntType(state) == SelectInstanceType)

			sd.setStatus(FAILED)
			return false
		}
	}
	// Phase 3 -- update descriptor's and selectInstance statuses
	if sd.getStatus() == FAILED { return false }
	sd.setStatus(SUCCEEDED)
	selectInstance.setState(unsafe.Pointer(sd.channel))
	return true
}


func (s *SelectInstance) readState(allowedDesc *SelectDesc) unsafe.Pointer {
	for {
		// Read state
		state := s.getState()
		// Check if state is `nil` or `*LFChan` and return it in this case
		if state == nil { return nil }
		stateType := IntType(state)
		if stateType != SelectDescType {
			return state
		}
		// State is SelectDesc, help it
		desc := (*SelectDesc) (state)
		// If this SelectDesc is allowed return it
		if desc == allowedDesc { return state }
		// Try to help with the found descriptor processing
		// and update state
		if desc.invoke() {
			s.setState(unsafe.Pointer(desc.channel))
			return unsafe.Pointer(desc.channel)
		} else {
			if s.casState(state, nil) { return nil }
		}
	}
}

func (s *SelectInstance) trySelect(channel unsafe.Pointer) bool {
	for {
		state := s.readState(nil)
		if state == channel { return true }
		if state != nil { return false }
		if s.casState(nil, unsafe.Pointer(channel)) { return true }
	}
}

func (s *SelectInstance) trySelect2(channel unsafe.Pointer) bool {
	for {
		state := s.readState(nil)
		if state != nil { return false }
		if s.casState(nil, unsafe.Pointer(channel)) { return true }
	}
}

func (s *SelectInstance) getState() unsafe.Pointer {
	return atomic.LoadPointer(&s.state)
}

func (s *SelectInstance) setState(state unsafe.Pointer) {
	stateType := IntType(state)
	if stateType == SelectInstanceType { panic("WTF?!!") }
	atomic.StorePointer(&s.state, state)
}

func (s *SelectInstance) casState(old, new unsafe.Pointer) bool{
	stateType := IntType(new)
	if stateType == SelectInstanceType { panic("WTF?!!") }
	return atomic.CompareAndSwapPointer(&s.state, old, new)
}

func IntType(p unsafe.Pointer) int32 {
	return *(*int32)(p)
}

func Select(alternatives ...SelectAlternative)  {
	selectImpl(&alternatives)
}

func SelectUnbiased(alternatives ...SelectAlternative) {
	shuffleAlternatives(&alternatives)
	selectImpl(&alternatives)
}

func selectImpl(alternatives *[]SelectAlternative) {
	selectInstance := &SelectInstance {
		__type: SelectInstanceType,
		alternatives: alternatives,
		state: nil,
		gp: runtime.GetGoroutine(),
	}
	selectInstance.doSelect()
}

// Shuffles alternatives randomly for `SelectUnbiased`.
func shuffleAlternatives(alternatives *[]SelectAlternative) {
	alts := *alternatives
	rand.Shuffle(len(alts), func (i, j int) {
		alts[i], alts[j] = alts[j], alts[i]
	})
}

func (s *SelectInstance) isSelected() bool {
	state := s.state
	if state == nil { return false }
	if IntType(state) == SelectDescType { return false }
	return true
}

// Does select in 3-phase way. At first it selects
// an alternative atomically (suspending if needed),
// then it unregisters from unselected channels,
// and invokes the specified for the selected
// alternative action at last.
func (s *SelectInstance) doSelect() {
	result, alternative := s.selectAlternative()
	s.cancelNonSelectedAlternatives()
	alternative.action(result)
}

func (s *SelectInstance) selectAlternative() (unsafe.Pointer, SelectAlternative) {
	for _, alt := range *(s.alternatives) {
		alt.channel.regSelect(s, alt.element)
		if s.isSelected() { break }
	}
	result := parkAndThenReturn()
	alternative := s.findAlternative((*LFChan)(s.state))
	return result, alternative
}

/**
 * Finds the selected alternative and returns it. This method relies on the fact
 * that `state` field stores the selected channel and looks for an alternative
 * with this channel.
 */
func (s *SelectInstance) findAlternative(channel *LFChan) SelectAlternative {
	for _, alt := range *(s.alternatives) {
		if alt.channel == channel {
			return alt
		}
	}
	stateType := IntType(s.state)
	switch stateType {
	case SelectInstanceType: panic("Impossible: SelectInstance")
	case SelectDescType: panic("Impossible: SelectDesc")
	}
	panic("Impossible")
}

func (s *SelectInstance) cancelNonSelectedAlternatives() {
	// TODO implement me, please, please
}


// === FUCKING GOLANG ===

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

func parkAndThenReturn() unsafe.Pointer {
	runtime.ParkUnsafe()
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

func (sd *SelectDesc) getStatus() int32 {
	return atomic.LoadInt32(&sd.status)
}

func (sd *SelectDesc) setStatus(status int32) {
	atomic.StoreInt32(&sd.status, status)
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