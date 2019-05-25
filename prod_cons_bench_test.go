package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

const approxBatchSize = 100000

var parallelism = []int{1, 2, 4, 8, 16, 32, 64, 128, 144} // number of scheduler threads
var goroutines = []int{0, 1000} // 0 -- number of scheduler threads
var work = []int{100} // an additional work size (parameter for `consumeCPU`) for each operation

// Multiple producer single consumer
func BenchmarkN1(b *testing.B) {
	for _, newAlgo := range [...]bool{false, true} {
		for _, withSelect := range [...]bool{false, true} {
			for _, work := range work {
				for _, parallelism := range parallelism {
					consumers := 1
					producers := parallelism - 1
					if producers == 0 {
						producers = 1
					}
					// Then run benchmarks
					for times := 0; times < 10; times++ {
						runBenchmark(b, producers, consumers, parallelism, withSelect, work, newAlgo)
					}
				}
			}
		}
	}
}

// Multiple producer multiple consumer
func BenchmarkNN(b *testing.B) {
	for _, newAlgo := range [...]bool{false, true} {
		for _, withSelect := range [...]bool{false, true} {
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
							runBenchmark(b, producers, consumers, parallelism, withSelect, work, newAlgo)
						}
					}
				}
			}
		}
	}
}

func runBenchmark(b *testing.B, producers int, consumers int, parallelism int, withSelect bool, work int, newAlgo bool) {
	runtime.GC()
	// Set benchmark parameters
	n := (approxBatchSize) / producers * producers
	// Do producer-consumer work in goroutines
	b.Run(fmt.Sprintf("newAlgo=%t/withSelect=%t/work=%d/goroutines=%d/threads=%d",
		newAlgo, withSelect, work, producers + consumers, parallelism),
		func(b *testing.B) {
			runtime.GOMAXPROCS(parallelism)
			b.N = n
			if newAlgo {
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
	c := make(chan int, 0)
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
			alts := []SelectAlternative{
				{
					channel: c,
					element: IntToUnsafePointer(1),
					action:  dummy,
				},
				{
					channel: dummyChan,
					element: ReceiverElement,
					action:  dummy,
				},
			}
			for j := 0; j < n / producers; j++ {
				if withSelect {
					SelectImpl(alts)
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
			alts := []SelectAlternative{
				{
					channel: c,
					element: ReceiverElement,
					action:  dummy,
				},
				{
					channel: dummyChan,
					element: ReceiverElement,
					action:  dummy,
				},
			}
			for j := 0; j < n / consumers; j++ {
				if withSelect {
					SelectImpl(alts)
				} else {
					c.Receive()
				}
				ConsumeCPU(work)
			}
		}()
	}
	wg.Wait()
}

func dummy(result unsafe.Pointer) {}

var consumedCPU = int32(time.Now().Unix())
func ConsumeCPU(tokens int) {
	t := int(atomic.LoadInt32(&consumedCPU)) // volatile read
	for i := tokens; i > 0; i-- {
		t += (t * 0x5DEECE66D + 0xB + i) & (0xFFFFFFFFFFFF)
	}
	if t == 42 { atomic.StoreInt32(&consumedCPU, consumedCPU + int32(t)) }
}