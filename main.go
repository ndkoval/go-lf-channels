package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"unsafe"
)

func main() {
	runtime.SetCPUProfileRate(1000)
	f, err := os.Create("main.pprof")
	if err != nil { log.Fatal(err) }
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	CPU := 2
	n := 10000000
	runtime.GOMAXPROCS(1)
	c := NewLFChan(0)
	//c := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(CPU)
	for producer := 0; producer < CPU / 2; producer++ {
		go func() {
			alts := []SelectAlternative{{channel: c, element: IntToUnsafePointer(0), action: func(result unsafe.Pointer) {}}}
			defer wg.Done()
			for i := 0; i < n; i++ {
				//select { case c <- i: {} }
				SelectImpl(alts)
				//c.SendInt(i)
				//c <- i
			}
		}()
	}
	for consumer := 0; consumer < CPU / 2; consumer++ {
		go func() {
			alts := []SelectAlternative{{channel: c, element: ReceiverElement, action: func(result unsafe.Pointer) {}}}
			defer wg.Done()
			for i := 0; i < n; i++ {
				//select { case <- c: {} }
				SelectImpl(alts)
				//c.ReceiveInt()
				//<- c
			}
		}()
	}
	wg.Wait()
}

func IntToUnsafePointer(x int) unsafe.Pointer {
	return (unsafe.Pointer)((uintptr)(x + 6000))
}

func UnsafePointerToInt(p unsafe.Pointer) int {
	return (int) ((uintptr) (p)) - 6000
}

func (c *LFChan) SendInt(element int) {
	c.Send(IntToUnsafePointer(element))
}

func (c *LFChan) ReceiveInt() int {
	return UnsafePointerToInt(c.Receive())
}
