package main

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"unsafe"
)

var ReceiverElement = (unsafe.Pointer) ((uintptr) (4096))
type SelectPPoPPAlternative struct {
	channel *LFChanPPoPP
	element unsafe.Pointer
	action  func(result unsafe.Pointer)
}

func SelectPPoPP(alternatives ...SelectPPoPPAlternative)  {
	SelectPPoPPImpl(alternatives)
}

func SelectPPoPPUnbiased(alternatives ...SelectPPoPPAlternative) {
	alternatives = shuffleAlternatives(alternatives)
	SelectPPoPPImpl(alternatives)
}

// Shuffles alternatives randomly for `SelectPPoPPUnbiased`.
func shuffleAlternatives(alts []SelectPPoPPAlternative) []SelectPPoPPAlternative {
	rand.Shuffle(len(alts), func (i, j int) {
		alts[i], alts[j] = alts[j], alts[i]
	})
	return alts
}

func SelectPPoPPImpl(alternatives []SelectPPoPPAlternative) {
	SelectPPoPPInstance := &SelectPPoPPInstance{
		__type: SelectPPoPPInstanceType,
		id:		nextSelectPPoPPInstanceId(),
		state:  state_registering,
		gp:     runtime.GetGoroutine(),
	}
	SelectPPoPPInstance.doSelectPPoPP(alternatives)
}

// Performs SelectPPoPP in 3-phase way. At first it SelectPPoPPs
// an alternative atomically (suspending if needed),
// then it unregisters from unSelectPPoPPed channels,
// and invokes the specified for the SelectPPoPPed
// alternative action at last.
func (s *SelectPPoPPInstance) doSelectPPoPP(alternatives []SelectPPoPPAlternative) {
	result, alternative, RegInfoPPoPPs := s.SelectPPoPPAlternative(alternatives)
	s.cancelNonSelectPPoPPedAlternatives(RegInfoPPoPPs)
	alternative.action(result)
}

const try_SelectPPoPP_fail = uint8(0)
const try_SelectPPoPP_confirmed = uint8(1)
const try_SelectPPoPP_sucess = uint8(2)

func (s *SelectPPoPPInstance) trySelectPPoPPFromSelectPPoPP(sid uint64, SelectPPoPPFrom *SelectPPoPPInstance, channel unsafe.Pointer, element unsafe.Pointer) uint8 {
	state := s.getState()
	x := 0
	for state == state_registering {
		x++; if x % 1000 == 0 { runtime.Gosched() }
		SelectPPoPPFrom.setWaitingFor(s)
		state = s.getState()
		if shouldConfirm(SelectPPoPPFrom, SelectPPoPPFrom, SelectPPoPPFrom.id) {
			SelectPPoPPFrom.setWaitingFor(nil)
			SelectPPoPPFrom.setState(state_waiting)
			return try_SelectPPoPP_confirmed
		}
	}
	SelectPPoPPFrom.setWaitingFor(nil)
	if state == state_waiting {
		if !s.casState(state_waiting, channel) { return try_SelectPPoPP_fail }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return try_SelectPPoPP_sucess
	} else {
		return try_SelectPPoPP_fail // already SelectPPoPPed
	}
}

func (s *SelectPPoPPInstance) trySelectPPoPPSimple(sid uint64, channel *LFChanPPoPP, element unsafe.Pointer) bool {
	state := s.getState()
	x := 0
	for state == state_registering {
		x++; if x % 1000 == 0 { runtime.Gosched() }
		state = s.getState()
	}

	if state == state_waiting {
		if !s.casState(state_waiting, unsafe.Pointer(channel)) { return false }
		runtime.SetGParam(s.gp, element)
		runtime.UnparkUnsafe(s.gp)
		return true
	} else {
		return false // already SelectPPoPPed
	}
}

func shouldConfirm(start *SelectPPoPPInstance, cur *SelectPPoPPInstance, min uint64) bool {
	next := cur.getWaitingFor()
	if next == nil { return false }
	if next.id < min { min = next.id }
	if next == start { return min == start.id }
	return shouldConfirm(start, next, min)
}

func (s *SelectPPoPPInstance) SelectPPoPPAlternative(alternatives []SelectPPoPPAlternative) (result unsafe.Pointer, alternative SelectPPoPPAlternative, RegInfoPPoPPs [2]RegInfoPPoPP) {
	RegInfoPPoPPs = [2]RegInfoPPoPP{}
	for i, alt := range alternatives {
		status, RegInfoPPoPP := alt.channel.regSelectPPoPP(s, alt.element)
		switch status {
		case reg_added:
			RegInfoPPoPPs[i] = RegInfoPPoPP
		case reg_confirmed:
			runtime.ParkUnsafe(s.gp)
			result = runtime.GetGParam(s.gp)
			runtime.SetGParam(s.gp, nil)
			channel := (*LFChanPPoPP) (s.state)
			alternative = s.findAlternative(channel, alternatives)
			s.setState(state_done)
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
	channel := (*LFChanPPoPP) (s.state)
	alternative = s.findAlternative(channel, alternatives)
	s.setState(state_done)
	return
}

/**
 * Finds the SelectPPoPPed alternative and returns it. This method relies on the fact
 * that `state` field stores the SelectPPoPPed channel and looks for an alternative
 * with this channel.
 */
func (s *SelectPPoPPInstance) findAlternative(channel *LFChanPPoPP, alternatives []SelectPPoPPAlternative) SelectPPoPPAlternative {
	for _, alt := range alternatives {
		if alt.channel == channel {
			return alt
		}
	}
	panic("Impossible")
}

func (s *SelectPPoPPInstance) cancelNonSelectPPoPPedAlternatives(RegInfoPPoPPs [2]RegInfoPPoPP) {
	for _, ri := range RegInfoPPoPPs {
		if ri.segmentPPoPP != nil {
			ri.segmentPPoPP.clean(ri.index)
		}
	}
}

var _nextSelectPPoPPInstanceId uint64 = 0
func nextSelectPPoPPInstanceId() uint64 {
	return atomic.AddUint64(&_nextSelectPPoPPInstanceId, 1)
}

const SelectPPoPPInstanceType int32 = 1098498093
type SelectPPoPPInstance struct {
	__type 	     int32
	id 	         uint64
	state 	     unsafe.Pointer
	gp           unsafe.Pointer // goroutine
	waitingFor   unsafe.Pointer
}
func (s *SelectPPoPPInstance) getWaitingFor() *SelectPPoPPInstance {
	return (*SelectPPoPPInstance) (atomic.LoadPointer(&s.waitingFor))
}

func (s *SelectPPoPPInstance) setWaitingFor(si *SelectPPoPPInstance) {
	atomic.StorePointer(&s.waitingFor, unsafe.Pointer(si))
}

var state_registering = unsafe.Pointer(uintptr(0))
var state_waiting = unsafe.Pointer(uintptr(1))
var state_done = unsafe.Pointer(uintptr(2))

type RegInfoPPoPP struct {
	segmentPPoPP *segmentPPoPP
	index   uint32
}