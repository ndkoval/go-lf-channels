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

func (s *SelectInstance) trySelectFromSelect(selectFrom *SelectInstance, channel unsafe.Pointer, element unsafe.Pointer) bool {
	state := s.getState()
	if state == state_registering {
		if s.casState(state_registering, unsafe.Pointer(selectFrom)) {
			for {
				state = s.getState()
				if state != unsafe.Pointer(selectFrom) { break }
				if shouldConfirm(selectFrom, selectFrom, selectFrom.id) {
					selectFrom.setState(state_confirmed)
					selectFrom.confirmedAlready = true
				}
			}
			if state == state_confirmed || selectFrom.confirmedAlready {
				runtime.SetGParam(s.gp, element)
				s.setState(channel)
				return true
			}
		} else {
			state = s.getState()
		}
	}
	if state == state_waiting {
		if !s.casState(state_waiting, channel) { return false }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return true
	} else {
		return false // already selected
	}
}

func (s *SelectInstance) trySelectSimple(channel *LFChan, element unsafe.Pointer) bool {
	state := s.getState()
	if state == state_registering {
		if s.casState(state, unsafe.Pointer(channel)) {
			for {
				state = s.getState()
				if state != unsafe.Pointer(channel) { break }
			}
			if state == state_confirmed {
				runtime.SetGParam(s.gp, element)
				s.setState(state_done)
				return true
			}
		} else {
			state = s.getState()
		}
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
	curState := cur.state
	if curState == state_registering || curState == state_waiting || curState == state_confirmed || curState == state_done {
		return false
	}

	curStateType := IntType(curState)
	if curStateType != SelectInstanceType {
		return false
	}

	next := (*SelectInstance) (curState)
	if next.id < min { min = next.id }
	if next == start {
		return min == start.id
	}
	return shouldConfirm(start, next, min)
}

func (s *SelectInstance) selectAlternative(alternatives []SelectAlternative) (result unsafe.Pointer, alternative SelectAlternative, reginfos [2]RegInfo) {
	reginfos = [2]RegInfo{}
	for i, alt := range alternatives {
		state := s.getState()
		if state != state_registering {
			stateType := IntType(state)
			if stateType == SelectInstanceType {
				s.setState(state_confirmed)
				state = state_confirmed
				for {
					state = s.getState()
					if state != state_confirmed { break }
				}
				channel := (*LFChan) (state)
				result = runtime.GetGParam(s.gp)
				runtime.SetGParam(s.gp, nil)
				alternative = s.findAlternative(channel, alternatives)
				return
			} else {
				channel := (*LFChan) (state)
				s.setState(state_confirmed)
				for s.getState() != state_done {}
				result = runtime.GetGParam(s.gp)
				runtime.SetGParam(s.gp, nil)
				alternative = s.findAlternative(channel, alternatives)
				return
			}
		}
		added, regInfo := alt.channel.regSelect(s, alt.element)
		if added {
			reginfos[i] = regInfo
		} else {
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
	confirmedAlready bool
}
var state_registering = unsafe.Pointer(uintptr(0))
var state_waiting = unsafe.Pointer(uintptr(1))
var state_confirmed = unsafe.Pointer(uintptr(2))
var state_done = unsafe.Pointer(uintptr(3))

type RegInfo struct {
	segment *segment
	index   uint32
}