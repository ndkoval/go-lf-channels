package main

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
type SelectAlternative struct {
	channel *LFChan
	element unsafe.Pointer
	action  func(result unsafe.Pointer)
}

func Select(alternatives ...SelectAlternative)  {
	selectImpl(&alternatives)
}

func SelectUnbiased(alternatives ...SelectAlternative) {
	shuffleAlternatives(&alternatives)
	selectImpl(&alternatives)
}

// Shuffles alternatives randomly for `SelectUnbiased`.
func shuffleAlternatives(alternatives *[]SelectAlternative) {
	alts := *alternatives
	rand.Shuffle(len(alts), func (i, j int) {
		alts[i], alts[j] = alts[j], alts[i]
	})
}

func selectImpl(alternatives *[]SelectAlternative) {
	selectInstance := &SelectInstance {
		__type: SelectInstanceType,
		id: nextSelectInstanceId(),
		alternatives: alternatives,
		regInfos: &([]RegInfo{}),
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
	result, alternative := s.selectAlternative()
	s.cancelNonSelectedAlternatives()
	alternative.action(result)
}

func (s *SelectInstance) trySetState(channel unsafe.Pointer, insideRegistration bool) bool {
	state := s.getState()
	if insideRegistration {
		if state != state_registering { panic("Invalid state in registration phase") }
		s.state = channel
		return true
	}
	for state == state_registering { time.Sleep(1); state = s.getState() }
	if state != state_waiting { return false }
	return s.casState(state_waiting, channel)
}

func (s *SelectInstance) selectAlternative() (result unsafe.Pointer, alternative SelectAlternative) {
	for _, alt := range *(s.alternatives) {
		added, regInfo := alt.channel.regSelect(s, alt.element)
		if added {
			c := make([]RegInfo, len(*s.regInfos))
			copy(c, *s.regInfos)
			c = append(c, regInfo)
			s.regInfos = &c
		} else { break }
	}
	if s.state == state_registering { atomic.StorePointer(&s.state, state_waiting) }
	runtime.ParkUnsafe()
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
	for _, alt := range *(s.alternatives) {
		if alt.channel == channel {
			return alt
		}
	}
	panic("Impossible")
}

func (s *SelectInstance) cancelNonSelectedAlternatives() {
	for _, ri := range *s.regInfos {
		ri.segment.clean(ri.index)
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
	alternatives *[]SelectAlternative
	regInfos     *[]RegInfo
	state 	     unsafe.Pointer
	gp           unsafe.Pointer // goroutine
}
var state_registering = unsafe.Pointer(nil)
var state_waiting = unsafe.Pointer(uintptr(1))

type RegInfo struct {
	segment *segment
	index   uint32
}