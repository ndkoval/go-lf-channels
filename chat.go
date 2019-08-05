package main

import (
	"github.com/gonum/stat"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)
import _ "github.com/gonum/stat"

var USERS = 1000
var WORK = 100
var SECONDS = 2

func main() {
	for _, algo := range [...]int{2} {
		for _, parallelism := range [...]int{1, 2, 4, 8, 12} {
			var results= make([]float64, 10)
			for i := 0; i < 10; i++ {
				results[i] = float64(runBenchmark(algo, parallelism)) / float64(SECONDS)
			}
			mean, std := stat.MeanStdDev(results, nil)
			println("algo=" + strconv.FormatInt(int64(algo), 10) + ", parallelism=" + strconv.FormatInt(int64(parallelism), 10) + ", op/s=" + strconv.FormatFloat(mean, 'f', 0, 64) + ", std=" + strconv.FormatFloat(std/mean*100, 'f', 1, 64) + "%")
		}
	}
}

func runBenchmark(algo, parallelism int) int {
	runtime.GC()
	runtime.GC()
	runtime.GC()
	runtime.GOMAXPROCS(parallelism)
	status = 0
	cancelled = 0
	for i := 0; i < USERS; i++ {
		u := &User{
			id: i,
			activity: 1,
			messagesToSend: 1,
			r: rand.New(rand.NewSource(int64(i))),
			inputGo: make(chan uintptr),
			inputEuropar: NewLFChan(),
		}
		users[i] = u
		go func() {
			switch algo {
			case 1: u.workGo()
			case 2: u.workEuropar()
			//case 3: u.workPPoPP()
			}
		}()
	}
	startBenchmark()
	time.Sleep(time.Duration(SECONDS) * time.Second)
	stopBenchmark()
	totalMsg := 0
	for i := 0; i < USERS; i++ {
		totalMsg += users[i].msgSent
	}
	time.Sleep(1 * time.Millisecond)
	for i := 0; i < USERS; i++ {
		users[i] = nil
	}
	return totalMsg
}

type User struct {
	id int
	activity float64
	messagesToSend float64
	msgSent int
	inputGo chan uintptr
	inputEuropar *LFChan
	r *rand.Rand
}

func (u *User) workGo() {
	defer func() {
		if recover() != nil { cancel() }
	}()
	waitForStart()
	for !shouldStop() {
		if u.messagesToSend >= 1 {
			u.messagesToSend -= 1
			i := u.id; for i == u.id { i = u.r.Intn(USERS) }
			to := users[i]
			msgToSend := uintptr(u.r.Uint64())
			sent := false
			for !sent {
				select {
				case to.inputGo <- msgToSend: sent = true
				case m := <- u.inputGo: u.processMsg(m)
				}
			}
			u.msgSent++
		} else {
			u.processMsg(<-u.inputGo)
		}
	}
	cancel()
}

func (u *User) workEuropar() {
	defer func() {
		if recover() != nil { cancel() }
	}()
	sent := false
	alts := []SelectAlternative{
		{
			channel: nil,
			element: nil,
			action: func(result unsafe.Pointer) { sent = true },
		},
		{
			channel: u.inputEuropar,
			element: ReceiverElement,
			action: func(result unsafe.Pointer) { u.processMsg(uintptr(result)) },
		},
	}
	waitForStart()
	for !shouldStop() {
		if u.messagesToSend >= 1 {
			u.messagesToSend -= 1
			i := u.id; for i == u.id { i = u.r.Intn(USERS) }
			to := users[i]
			msgToSend := unsafe.Pointer(uintptr(u.r.Uint64()) + 6000)
			alts[0].channel = to.inputEuropar
			alts[0].element = msgToSend
			sent = false
			for !sent {
				SelectImpl(alts)
			}
			u.msgSent++
		} else {
			u.processMsg(uintptr(u.inputEuropar.Receive()))
		}
	}
	cancel()
}


func (u *User) processMsg(msg uintptr) {
	u.messagesToSend += u.activity
	ConsumeCPU(WORK)
}

var users = make([]*User, USERS)

var status uint32 = 0
func waitForStart() {
	for atomic.LoadUint32(&status) == 0 {
		time.Sleep(1)
	}
}
func shouldStop() bool {
	return atomic.LoadUint32(&status) == 2
}
func startBenchmark() {
	atomic.StoreUint32(&status, 1)
}
func stopBenchmark() {
	atomic.StoreUint32(&status, 2)
}


var cancelled uint32 = 0
func allCancelled() bool {
	return atomic.LoadUint32(&cancelled) == uint32(USERS)
}
func cancel() {
	atomic.AddUint32(&cancelled, 1)
}

var consumedCPU = int32(time.Now().Unix())
func ConsumeCPU(tokens int) {
	t := int(atomic.LoadInt32(&consumedCPU)) // volatile read
	for i := tokens; i > 0; i-- {
		t += (t * 0x5DEECE66D + 0xB + i) & (0xFFFFFFFFFFFF)
	}
	if t == 42 { atomic.StoreInt32(&consumedCPU, consumedCPU + int32(t)) }
}