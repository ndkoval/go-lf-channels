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
	gp := runtime.GetGoroutine()
	selectInstance := getSelectInstance(gp)
	selectInstance.doSelect(alternatives)
	returnSelectInstance(gp, selectInstance)
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
func (s *SelectInstance) trySelectFromSelect(sid uint64, selectFrom *SelectInstance, channel unsafe.Pointer, element unsafe.Pointer) uint8 {
	stateOffset := uintptr(sid % selectInstanceIdDelta)
	stateRegisteringS := unsafe.Pointer(state_registering_base + stateOffset)
	stateSpinWaitingS := unsafe.Pointer(state_spin_waiting_base + stateOffset)
	stateWaitingS := unsafe.Pointer(state_waiting_base + stateOffset)

	state := s.getState()
	x := 0
	for state == stateRegisteringS {
		x++; if x % 1000 == 0 { runtime.Gosched() }
		selectFrom.setWaitingFor(s)
		state = s.getState()
		if shouldConfirm(selectFrom, selectFrom, selectFrom.id) {
			selectFrom.setWaitingFor(nil)
			selectFrom.setState(spin_waiting_state(selectFrom))
			return try_select_confirmed
		}
	}
	selectFrom.setWaitingFor(nil)

	if state == stateSpinWaitingS {
		if !s.casState(stateSpinWaitingS, state_reg_selected) { return try_select_fail }
		runtime.SetGParam(s.gp, element)
		s.setState(channel)
		return try_select_sucess
	}
	if state == stateWaitingS {
		if !s.casState(stateWaitingS, channel) { return try_select_fail }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return try_select_sucess
	} else {
		return try_select_fail // already selected
	}
}

func (s *SelectInstance) trySelectSimple(sid uint64, channel *LFChan, element unsafe.Pointer) bool {
	stateOffset := uintptr(sid % selectInstanceIdDelta)
	stateRegisteringS := unsafe.Pointer(state_registering_base + stateOffset)
	stateSpinWaitingS := unsafe.Pointer(state_spin_waiting_base + stateOffset)
	stateWaitingS := unsafe.Pointer(state_waiting_base + stateOffset)

	state := s.getState()
	x := 0
	for state == stateRegisteringS {
		x++; if x % 1000 == 0 { runtime.Gosched() }
		state = s.getState()
	}

	if state == stateSpinWaitingS {
		if !s.casState(stateSpinWaitingS, state_reg_selected) { return false }
		runtime.SetGParam(s.gp, element)
		s.setState(unsafe.Pointer(channel))
		return true
	}
	if state == stateWaitingS {
		if !s.casState(stateWaitingS, unsafe.Pointer(channel)) { return false }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return true
	} else {
		return false // already selected
	}
}

func shouldConfirm(start *SelectInstance, cur *SelectInstance, min uint64) bool {
	next := cur.getWaitingFor()
	if next == nil { return false }
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
			stateSpinWaiting := spin_waiting_state(s)
			x := 0
			for state == stateSpinWaiting || state == state_reg_selected {
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
	atomic.StorePointer(&s.state, waiting_state(s))
	runtime.ParkUnsafe(s.gp)
	result = runtime.GetGParam(s.gp)
	runtime.SetGParam(s.gp, nil)
	channel := (*LFChan) (s.state)
	alternative = s.findAlternative(channel, alternatives)
	s.setState(state_done)
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

func getSelectInstance(gp unsafe.Pointer) *SelectInstance {
	sip := runtime.GetGSelect(gp)
	if sip == nil {
		return &SelectInstance {
			__type: SelectInstanceType,
			id: nextSelectInstanceId(),
			state: unsafe.Pointer(state_registering_base),
			gp: runtime.GetGoroutine(),
		}
	}
	runtime.SetGSelect(gp, nil)
	si := (*SelectInstance) (sip)
	return si
}

func returnSelectInstance(gp unsafe.Pointer, si *SelectInstance) {
	atomic.StoreUint64(&si.id, si.id + 1)
	if si.id % selectInstanceIdDelta == 0 { return } // enough with it :)
	si.state = unsafe.Pointer(state_registering_base + uintptr(si.id % selectInstanceIdDelta)) // reg state
	runtime.SetGSelect(gp, unsafe.Pointer(si))
}

func waiting_state(si *SelectInstance) unsafe.Pointer {
	return unsafe.Pointer(state_waiting_base + uintptr(si.id % selectInstanceIdDelta))
}

func spin_waiting_state(si *SelectInstance) unsafe.Pointer {
	return unsafe.Pointer(state_spin_waiting_base + uintptr(si.id % selectInstanceIdDelta))
}

const selectInstanceIdDelta = uint64(2)

var _nextSelectInstanceId uint64 = 0
func nextSelectInstanceId() uint64 {
	return atomic.AddUint64(&_nextSelectInstanceId, selectInstanceIdDelta)
}

const SelectInstanceType int32 = 1098498093
type SelectInstance struct {
	__type 	     int32
	id 	         uint64
	state 	     unsafe.Pointer
	gp           unsafe.Pointer // goroutine
	waitingFor   unsafe.Pointer
}
func (s *SelectInstance) getWaitingFor() *SelectInstance {
	return (*SelectInstance) (atomic.LoadPointer(&s.waitingFor))
}

func (s *SelectInstance) setWaitingFor(si *SelectInstance) {
	atomic.StorePointer(&s.waitingFor, unsafe.Pointer(si))
}

var state_registering_base = uintptr(0)
var state_waiting_base = uintptr(selectInstanceIdDelta)
var state_spin_waiting_base = uintptr(2 * selectInstanceIdDelta)

var state_reg_selected = unsafe.Pointer(uintptr(3 * selectInstanceIdDelta))
var state_done = unsafe.Pointer(uintptr(3 * selectInstanceIdDelta + 1))

type RegInfo struct {
	segment *segment
	index   uint32
}