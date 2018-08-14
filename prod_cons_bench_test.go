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

const useProfiler = true

func setupProfiler(channels int, threads int) {
	if useProfiler {
		runtime.SetCPUProfileRate(1000)
		runtime.GOMAXPROCS(1)
		f, err := os.Create(fmt.Sprintf("cur_ch%dt%d.pprof", channels, threads))
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
}

//func BenchmarkN1(b *testing.B) {
//
//}

const kovalAlgo = true
func BenchmarkNN(b *testing.B) {
	for _, spin := range spins {
		// Redirect output
		outFile, _ := os.Create(fmt.Sprintf("spin%dsegm%d.out", spin, segmentSize))
		os.Stdout = outFile
		for _, withSelect := range [2]bool{false, true} {
			for _, channels := range contentionFactor {
				for _, goroutines := range goroutines {
					for _, parallelism := range parallelism {
						producers := goroutines / (channels * 2)
						if producers == 0 {
							producers = (parallelism + 1) / 2 // round up
						}
						consumers := producers // N:N case
						//var algo string
						//if kovalAlgo {
						//	algo = fmt.Sprintf("k_spin%d_segm%d",spin, segmentSize)
						//} else {
						//	algo = "golang"
						//}
						// Warm-up at first
						for times := 0; times < 2; times++ {
							runBenchmark(b, producers, consumers, channels, parallelism, withSelect, kovalAlgo, spin)
						}
						// Then run benchmarks
						for times := 0; times < 10; times++ {
							runtime.GC()
							setupProfiler(channels, parallelism)
							b.Run(fmt.Sprintf("withSelect=%t/channels=%d/goroutines=%d/parallelism=%d",
								withSelect, channels, goroutines, parallelism),
								func(b *testing.B) {
									runBenchmark(b, producers, consumers, channels, parallelism, withSelect, kovalAlgo, spin)
								})
							if useProfiler {
								pprof.StopCPUProfile()
							}
						}
					}
				}
			}
		}
	}
}

func runBenchmark(b *testing.B, producers int, consumers int, channels int, parallelism int, withSelect bool, kovalAlgo bool, spin int) {
	b.StopTimer()
	// Set benchmark parameters
	runtime.GOMAXPROCS(parallelism)
	n := lcf(producers, consumers)
	if n < minBatchSize {
		n = minBatchSize / n * n
	}
	if n % producers != 0 || n % consumers != 0 {
		b.Fatal("n should be a common factor of producers and consumers")
	}
	b.N = n * channels
	wg := &sync.WaitGroup{}
	wg.Add((producers + consumers) * channels)
	b.StartTimer()
	// Do producer-consumer work in goroutines
	for channel := 0; channel < channels; channel++ {
		if kovalAlgo {
			runWithOneChannelKoval(b, wg, producers, consumers, n, withSelect, spin)
		} else {
			runWithOneChannelGo(b, wg, producers, consumers, n, withSelect)
		}
	}
	// Wait until all goroutines are executed
	wg.Wait()
}

func runWithOneChannelGo(b *testing.B, wg *sync.WaitGroup, producers int, consumers int, n int, withSelect bool) {
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
				ConsumeCPU(7)
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
				ConsumeCPU(7)
			}
		}()
	}
}

func runWithOneChannelKoval(b *testing.B, wg *sync.WaitGroup, producers int, consumers int, n int, withSelect bool, spin int) {
	c := NewLFChan(spin)
	// Run producers
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			var dummyChan *LFChan
			if withSelect { dummyChan = NewLFChan(spin) }
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
				ConsumeCPU(7)
			}
		}()
	}
	// Run consumers
	for i := 0; i < consumers; i++ {

		go func() {
			defer wg.Done()
			var dummyChan *LFChan
			if withSelect { dummyChan = NewLFChan(spin) }
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
				ConsumeCPU(7)
			}
		}()
	}
}

func gcd(a, b int) int {
	for a != b {
		if a > b {
			a -= b
		} else {
			b -= a
		}
	}
	return a
}

func lcf(a, b int) int {
	return a * b / gcd(a, b)
}

func goroutinesFactor() int {
	res := 1
	for _,e := range parallelism {
		res = lcf(res, e)
	}
	res *= 2
	return res
}

const minBatchSize = 100000
//var parallelism = []int{1, 2, 4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64, 72, 96, 108, 128, 144}
var parallelism = []int{1, 2, 4, 8, 16, 24, 32, 64, 96, 128, 144}
var contentionFactor = []int{1, 10}
//var contentionFactor = []int{1, 2, 4, 8, 16, 32}
var goroutines = []int{0}
//var goroutines = []int{0, goroutinesFactor()}
//var goroutines = []int{0, goroutinesFactor(), goroutinesFactor() * 10, goroutinesFactor() * 100}

var spins = []int{300}
//var spins = []int{0, 50, 100, 200, 300, 500, 700, 1000, 1300, 1600, 2000, 2500, 3000, 4000, 6000, 10000, 2147483647}
