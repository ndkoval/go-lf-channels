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
		alternatives: alternatives,
		//regInfos: &([]RegInfo{}),
		state: nil,
		gp: runtime.GetGoroutine(),
	}
	selectInstance.doSelect()
}

// Performs select in 3-phase way. At first it selects
// an alternative atomically (suspending if needed),
// then it unregisters from unselected channels,
// and invokes the specified for the selected
// alternative action at last.
func (s *SelectInstance) doSelect() {
	result, alternative, reginfos := s.selectAlternative()
	s.cancelNonSelectedAlternatives(reginfos)
	alternative.action(result)
}

func (s *SelectInstance) trySetState(channel unsafe.Pointer, insideRegistration bool) (bool, bool) {
	state := s.getState()
	if insideRegistration {
		atomic.StorePointer(&s.state, channel)
		return true, false
	} else {
		if state == state_registering {
			if s.casState(state_registering, channel) {
				for {
					state = s.getState()
					if state != channel { break }
					runtime.Gosched()
				}
				if state == state_finished { return true, true }
			} else {
				state = s.getState()
			}
		}
		if state != state_waiting { return false, false }
		return s.casState(state_waiting, channel), false
	}
}

func (s *SelectInstance) selectAlternative() (result unsafe.Pointer, alternative SelectAlternative, reginfos [2]RegInfo) {
	reginfos = [2]RegInfo{}
	selected := false
	for i, alt := range s.alternatives {
		if s.getState() != state_registering {
			channel := (*LFChan) (s.state)
			atomic.StorePointer(&s.state, state_finished)
			for s.getState() != state_finished2 { }
			result = runtime.GetGParam(s.gp)
			alternative = s.findAlternative(channel)
			return
		}
		added, regInfo := alt.channel.regSelect(s, alt.element)
		if added {
			reginfos[i] = regInfo
		} else {
			selected = true
			break
		}
	}
	if !selected {
		atomic.StorePointer(&s.state, state_waiting)
	}
	runtime.ParkUnsafe(s.gp)
	result = runtime.GetGParam(s.gp)
	channel := (*LFChan) (s.state)
	alternative = s.findAlternative(channel)
	return
}

/**
 * Finds the selected alternative and returns it. This method relies on the fact
 * that `state` field stores the selected channel and looks for an alternative
 * with this channel.
 */
func (s *SelectInstance) findAlternative(channel *LFChan) SelectAlternative {
	for _, alt := range s.alternatives {
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

const SelectInstanceType int32 = 1298498092
type SelectInstance struct {
	__type 	     int32
	id 	         uint64
	alternatives []SelectAlternative
	//regInfos     *[]RegInfo
	state 	     unsafe.Pointer
	gp           unsafe.Pointer // goroutine
}
var state_registering = unsafe.Pointer(nil)
var state_waiting = unsafe.Pointer(uintptr(1))
var state_finished = unsafe.Pointer(uintptr(2))
var state_finished2 = unsafe.Pointer(uintptr(2))

type RegInfo struct {
	segment *segment
	index   uint32
}