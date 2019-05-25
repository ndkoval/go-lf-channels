package main

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"unsafe"
)

var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
type SelectAlternative struct {
	channel *LFChan
	element unsafe.Pointer
	action  func(result unsafe.Pointer)
}

func Select(alternatives ...SelectAlternative)  {
	SelectImpl(alternatives)
}

func SelectUnbiased(alternatives ...SelectAlternative) {
	alternatives = shuffleAlternatives(alternatives)
	SelectImpl(alternatives)
}

// Shuffles alternatives randomly for `SelectUnbiased`.
func shuffleAlternatives(alts []SelectAlternative) []SelectAlternative {
	rand.Shuffle(len(alts), func (i, j int) {
		alts[i], alts[j] = alts[j], alts[i]
	})
	return alts
}

func SelectImpl(alternatives []SelectAlternative) {
	selectInstance := &SelectInstance{
		__type: SelectInstanceType,
		id:		nextSelectInstanceId(),
		gp:     runtime.GetGoroutine(),
	}
	selectInstance.doSelect(alternatives)
}

// Performs select in 3-phase way. At first it selects
// an alternative atomically (suspending if needed),
// then it unregisters from unselected channels,
// and invokes the specified for the selected
// alternative action at last.
func (s *SelectInstance) doSelect(alternatives []SelectAlternative) {
	result, alternative, reginfos := s.selectAlternative(alternatives)
	s.cancelNonSelectedAlternatives(reginfos)
	alternative.action(result)
}

func (s *SelectInstance) selectAlternative(alternatives []SelectAlternative) (result unsafe.Pointer, alternative SelectAlternative, reginfos [2]RegInfo) {
	reginfos = [2]RegInfo{}
	for i, alt := range alternatives {
		added, regInfo := alt.channel.regSelect(s, alt.element)
		if (added) {
			reginfos[i] = regInfo
		} else {
			break
		}
	}
	runtime.ParkUnsafe(s.gp)
	result = runtime.GetGParam(s.gp)
	runtime.SetGParam(s.gp, nil)
	selectState := s.state
	var channel *LFChan
	if IntType(selectState) == SelectDescType {
		channel = (*LFChan) ((*SelectDesc) (selectState).channel)
	} else {
		channel = (*LFChan) (selectState)
	}
	alternative = s.findAlternative(channel, alternatives)
	return
}

/**
 * Finds the selected alternative and returns it. This method relies on the fact
 * that `state` field stores the selected channel and looks for an alternative
 * with this channel.
 */
func (s *SelectInstance) findAlternative(channel *LFChan, alternatives []SelectAlternative) SelectAlternative {
	for _, alt := range alternatives {
		if alt.channel == channel {
			return alt
		}
	}
	panic("Impossible")
}

func (s *SelectInstance) cancelNonSelectedAlternatives(reginfos [2]RegInfo) {
	for _, ri := range reginfos {
		if ri.segment != nil {
			ri.segment.clean(ri.index)
		}
	}
}

var _nextSelectInstanceId uint64 = 0
func nextSelectInstanceId() uint64 {
	return atomic.AddUint64(&_nextSelectInstanceId, 1)
}

const SelectInstanceType int32 = 1098498093
type SelectInstance struct {
	__type 	     int32
	id 	         uint64
	state 	     unsafe.Pointer
	gp           unsafe.Pointer // goroutine
}

type RegInfo struct {
	segment *segment
	index   int32
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
	anotherCont := sd.cont
	anotherContType := IntType(anotherCont)
	sdp := unsafe.Pointer(sd)
	failed := false
	if anotherContType == SelectInstanceType {
		anotherSelectInstance := (*SelectInstance) (anotherCont)
		if selectInstance.id < anotherSelectInstance.id {
			if selectInstance.trySetDescriptor(sdp) {
				if !anotherSelectInstance.trySetDescriptor(sdp) {
					failed = true
					selectInstance.resetState(sdp)
				}
			} else { failed = true }
		} else {
			if anotherSelectInstance.trySetDescriptor(sdp) {
				if !selectInstance.trySetDescriptor(sdp) {
					failed = true
					anotherSelectInstance.resetState(sdp)
				}
			} else { failed = true }
		}
	} else {
		if !selectInstance.trySetDescriptor(sdp) {
			sd.setStatus(FAILED)
			return false
		}
	}
	// Phase 3 -- update descriptor's and selectInstance statuses
	if failed {
		sd.setStatus(FAILED)
		return false
	} else {
		sd.setStatus(SUCCEEDED)
		return true
	}
}

func (s *SelectInstance) resetState(desc unsafe.Pointer) {
	s.casState(desc, nil)
}


func (s *SelectInstance) readState(allowedUnprocessedDesc unsafe.Pointer) unsafe.Pointer {
	for {
		// Read state
		state := s.getState()
		// Check if state is `nil` or `*LFChan` and return it in this case
		if state == nil || state == allowedUnprocessedDesc {
			return state
		}
		stateType := IntType(state)
		if stateType != SelectDescType {
			return state
		}
		// If this SelectDesc is allowed return it
		// State is SelectDesc, help it
		desc := (*SelectDesc) (state)
		// Try to help with the found descriptor processing
		// and update state
		if desc.invoke() {
			return state
		} else {
			if s.casState(state, nil) { return nil }
		}
	}
}

func (s *SelectInstance) trySetDescriptor(desc unsafe.Pointer) bool {
	for {
		state := s.readState(desc)
		if state == desc { return true }
		if state != nil { return false }
		if s.casState(nil, desc) { return true }
	}
}

func (sd *SelectDesc) getStatus() int32 {
	return atomic.LoadInt32(&sd.status)
}

func (sd *SelectDesc) setStatus(status int32) {
	atomic.StoreInt32(&sd.status, status)
}

func (s *SelectInstance) isSelected() bool {
	return s.readState(nil) != nil
}

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