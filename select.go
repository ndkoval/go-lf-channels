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
	selectInstance := &SelectInstance {
		__type: SelectInstanceType,
		id: nextSelectInstanceId(),
		state: state_registering,
		gp: runtime.GetGoroutine(),
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

const try_select_fail = uint8(0)
const try_select_confirmed = uint8(1)
const try_select_sucess = uint8(2)
func (s *SelectInstance) trySelectFromSelect(selectFrom *SelectInstance, channel unsafe.Pointer, element unsafe.Pointer) uint8 {
	state := s.getState()
	x := 0
	for state == state_registering {
		x++; if x % 1000 == 0 { runtime.Gosched() }
		selectFrom.waitingFor = s
		state = s.getState()
		if shouldConfirm(selectFrom, selectFrom, selectFrom.id) {
			selectFrom.waitingFor = nil
			selectFrom.setState(state_spin_waiting)
			return try_select_confirmed
		}
	}
	selectFrom.waitingFor = nil
	if state == state_spin_waiting {
		if !s.casState(state_spin_waiting, state_reg_selected) { return try_select_fail }
		runtime.SetGParam(s.gp, element)
		s.setState(channel)
		return try_select_sucess
	}
	if state == state_waiting {
		if !s.casState(state, channel) { return try_select_fail }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return try_select_sucess
	} else {
		return try_select_fail // already selected
	}
}

func (s *SelectInstance) trySelectSimple(channel *LFChan, element unsafe.Pointer) bool {
	state := s.getState()
	for state == state_registering {
		state = s.getState()
	}
	if state == state_spin_waiting {
		if !s.casState(state_spin_waiting, state_reg_selected) { return false }
		runtime.SetGParam(s.gp, element)
		s.setState(unsafe.Pointer(channel))
		return true
	}
	if state == state_waiting {
		if !s.casState(state, unsafe.Pointer(channel)) { return false }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return true
	} else {
		return false // already selected
	}
}

func shouldConfirm(start *SelectInstance, cur *SelectInstance, min uint64) bool {
	next := cur.waitingFor
	if next == nil { return false}
	if next.id < min { min = next.id }
	if next == start { return min == start.id }
	return shouldConfirm(start, next, min)
}

func (s *SelectInstance) selectAlternative(alternatives []SelectAlternative) (result unsafe.Pointer, alternative SelectAlternative, reginfos [2]RegInfo) {
	reginfos = [2]RegInfo{}
	for i, alt := range alternatives {
		status, regInfo := alt.channel.regSelect(s, alt.element)
		switch status {
		case reg_added:
			reginfos[i] = regInfo
		case reg_confirmed:
			state := s.getState()
			x := 0
			for state == state_spin_waiting || state == state_reg_selected {
				x++; if x % 1000 == 0 { runtime.Gosched() }
				state = s.getState()
			}
			s.setState(state_done)
			channel := (*LFChan) (state)
			result = runtime.GetGParam(s.gp)
			runtime.SetGParam(s.gp, nil)
			alternative = s.findAlternative(channel, alternatives)
			return
		case reg_rendezvous:
			s.setState(state_done)
			alternative = alt
			result = runtime.GetGParam(s.gp)
			runtime.SetGParam(s.gp, nil)
			return
		}
	}
	atomic.StorePointer(&s.state, state_waiting)
	runtime.ParkUnsafe(s.gp)
	result = runtime.GetGParam(s.gp)
	runtime.SetGParam(s.gp, nil)
	channel := (*LFChan) (s.state)
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
	waitingFor   *SelectInstance
}
var state_registering = unsafe.Pointer(uintptr(0))
var state_waiting = unsafe.Pointer(uintptr(1))
var state_spin_waiting = unsafe.Pointer(uintptr(2))
var state_reg_selected = unsafe.Pointer(uintptr(3))
var state_done = unsafe.Pointer(uintptr(4))

type RegInfo struct {
	segment *segment
	index   uint32
}