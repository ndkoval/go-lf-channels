package go_lf_channels

import (
	"testing"
	"sync"
	"fmt"
	"unsafe"
	"runtime"
)

func BenchmarkN1(b *testing.B) {

}

const kovalAlgo = true
func BenchmarkNN(b *testing.B) {
	for _, withSelect := range [1]bool{false} {
		for _, channels := range contentionFactor {
			for _, goroutines := range goroutines {
				for _, parallelism := range parallelism {
					producers := goroutines / (channels * 2)
					if producers == 0 {
						producers = (parallelism + 1) / 2 // round up
					}
					consumers := producers // N:N case
					var algo string
					if kovalAlgo {
						algo = fmt.Sprintf("k_spin%d_segm%d",spin, segmentSize)
					} else {
						algo = "golang"
					}
					b.Run(fmt.Sprintf("algo=%s, withSelect=%t, channels=%d, goroutines=%d, parallelism=%d",
						algo, withSelect, channels, goroutines, parallelism),
						func(b *testing.B) {
							runBenchmark(b, producers, consumers, channels, parallelism, withSelect, true)
						})
				}
			}
		}
	}
}

func runBenchmark(b *testing.B, producers int, consumers int, channels int, parallelism int, withSelect bool, kovalAlgo bool) {
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
			runWithOneChannelKoval(b, wg, producers, consumers, n, withSelect)
		} else {
			runWithOneChannelGo(b, wg, producers, consumers, n, withSelect)
		}
	}
	// Wait until all goroutines are executed
	wg.Wait()
}

func runWithOneChannelGo(b *testing.B, wg *sync.WaitGroup, producers int, consumers int, n int, withSelect bool) {
	c := make(chan unsafe.Pointer)
	// Run producers
	for i := 0; i < producers; i++ {
		var dummyChan chan unsafe.Pointer
		if withSelect { dummyChan = make(chan unsafe.Pointer) }
		go func() {
			defer wg.Done()
			for j := 0; j < n / producers; j++ {
				if withSelect {
					select {
					case c <- (unsafe.Pointer)((uintptr)(j)): { /* do nothing */ }
					case <- dummyChan: { /* do nothing */ }
					}
				} else {
					c <- (unsafe.Pointer)((uintptr)(j))
				}
			}
		}()
	}
	// Run consumers
	for i := 0; i < consumers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < n / consumers; j++ {
				var dummyChan chan unsafe.Pointer
				if withSelect { dummyChan = make(chan unsafe.Pointer) }
				if withSelect {
					select {
					case <- c: { /* do nothing */ }
					case <- dummyChan: { /* do nothing */ }
					}
				} else {
					<- c
				}

			}
		}()
	}
}

const spin = 1000
func runWithOneChannelKoval(b *testing.B, wg *sync.WaitGroup, producers int, consumers int, n int, withSelect bool) {
	c := NewLFChan(spin)
	// Run producers
	for i := 0; i < producers; i++ {
		//var dummyChan *LFChan
		//if withSelect { dummyChan = NewLFChan(spin) }
		go func() {
			defer wg.Done()
			for j := 0; j < n / producers; j++ {
				if withSelect {
					b.Fatal("Unsupported")
				} else {
					c.SendInt(j)
				}
			}
		}()
	}
	// Run consumers
	for i := 0; i < consumers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < n / consumers; j++ {
				//var dummyChan *LFChan
				//if withSelect { dummyChan = NewLFChan(spin) }
				if withSelect {
					b.Fatal("Unsupported")
				} else {
					c.Receive()
				}

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
	//for _,e := range contentionFactor {
	//	res = lcf(res, e)
	//}
	res *= 2
	return res
}

const minBatchSize = 50000
//var parallelism = []int{1, 2, 4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64, 72, 96, 108, 128, 144}
var parallelism = []int{1, 2, 4, 6}
var contentionFactor = []int{1, 2, 4, 8, 16, 32}
//var goroutines = []int{0, goroutinesFactor(), goroutinesFactor() * 10, goroutinesFactor() * 100}
var goroutines = []int{0, goroutinesFactor(), goroutinesFactor() * 10}