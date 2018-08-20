package main

import (
	"testing"
	"sync"
	"fmt"
	"runtime"
	"unsafe"
	"os"
	"log"
	"runtime/pprof"
)

const useProfiler = false
const kovalAlgo = true
const approxBatchSize = 100000
var parallelism = []int{1}
var goroutines = []int{1000, 10000}
var work = []int{100}

func BenchmarkN1(b *testing.B) {
	for _, withSelect := range [2]bool{false, true} {
		for _, work := range work {
			for _, parallelism := range parallelism {
				consumers := 1
				producers := parallelism - 1
				if producers == 0 { producers = 1 }
				// Then run benchmarks
				for times := 0; times < 10; times++ {
					runBenchmark(b, producers, consumers, parallelism, withSelect, work)
				}
			}
		}
	}
}


func BenchmarkNN(b *testing.B) {
	for _, withSelect := range [1]bool{false} {
		for _, work := range work {
			for _, goroutines := range goroutines {
				for _, parallelism := range parallelism {
					var producers, consumers int
					if goroutines == 0 {
						producers = (parallelism + 1) / 2
					} else {
						producers = goroutines / 2
					}
					consumers = producers
					// Then run benchmarks
					for times := 0; times < 10; times++ {
						runBenchmark(b, producers, consumers, parallelism, withSelect, work)
					}
				}
			}
		}
	}
}

func runBenchmark(b *testing.B, producers int, consumers int, parallelism int, withSelect bool, work int) {
	if useProfiler {
		runtime.SetCPUProfileRate(1000)
		f, err := os.Create(fmt.Sprintf("cur_S%tT%dW%d.pprof", withSelect, parallelism, work))
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	runtime.GC()
	// Set benchmark parameters
	n := (approxBatchSize) / producers * producers
	// Do producer-consumer work in goroutines
	b.Run(fmt.Sprintf("withSelect=%t/work=%d/goroutines=%d/threads=%d",
		withSelect, work, producers + consumers, parallelism),
		func(b *testing.B) {
			//runtime.GOMAXPROCS(parallelism)
			b.N = n
			if kovalAlgo {
				runBenchmarkKoval(n, producers, consumers, withSelect, work)
			} else {
				runBenchmarkGo(n, producers, consumers, withSelect, work)
			}
		})
	// Wait until all goroutines are executed
}

func runBenchmarkGo(n int, producers int, consumers int, withSelect bool, work int) {
	wg := sync.WaitGroup{}
	wg.Add(producers + consumers)
	c := make(chan int)
	// Run producers
	for i := 0; i < producers; i++ {
		go func() {
			var dummyChan chan int
			if withSelect { dummyChan = make(chan int) }
			defer wg.Done()
			for j := 0; j < n / producers; j++ {
				if withSelect {
					select {
					case c <- j: { /* do nothing */ }
					case <- dummyChan: { /* do nothing */ }
					}
				} else {
					c <- j
				}
				ConsumeCPU(work)
			}
		}()
	}
	// Run consumers
	for i := 0; i < consumers; i++ {
		go func() {
			defer wg.Done()
			var dummyChan chan int
			if withSelect { dummyChan = make(chan int) }
			for j := 0; j < n / consumers; j++ {
				if withSelect {
					select {
					case <- c: { /* do nothing */ }
					case <- dummyChan: { /* do nothing */ }
					}
				} else {
					<- c
				}
				ConsumeCPU(work)
			}
		}()
	}
	wg.Wait()
}

func runBenchmarkKoval(n int, producers int, consumers int, withSelect bool, work int) {
	wg := sync.WaitGroup{}
	wg.Add(producers + consumers)
	c := NewLFChan()
	// Run producers
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			var dummyChan *LFChan
			if withSelect { dummyChan = NewLFChan() }
			for j := 0; j < n / producers; j++ {
				if withSelect {
					Select(
						SelectAlternative{
							channel: c,
							element: IntToUnsafePointer(j),
							action: func (result unsafe.Pointer) {},
						},
						SelectAlternative{
							channel: dummyChan,
							element: ReceiverElement,
							action: func (result unsafe.Pointer) {},
						},
					)
				} else {
					c.SendInt(j)
				}
				ConsumeCPU(work)
			}
		}()
	}
	// Run consumers
	for i := 0; i < consumers; i++ {
		go func() {
			defer wg.Done()
			var dummyChan *LFChan
			if withSelect { dummyChan = NewLFChan() }
			for j := 0; j < n / consumers; j++ {
				if withSelect {
					Select(
						SelectAlternative{
							channel: c,
							element: ReceiverElement,
							action: func (result unsafe.Pointer) {},
						},
						SelectAlternative{
							channel: dummyChan,
							element: ReceiverElement,
							action: func (result unsafe.Pointer) {},
						},
					)
				} else {
					c.Receive()
				}
				ConsumeCPU(work)
			}
		}()
	}
	wg.Wait()
}