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
	n := 50000000
	runtime.GOMAXPROCS(2)
	c := NewLFChan(0)
	//c := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(CPU)
	for producer := 0; producer < CPU / 2; producer++ {
		go func() {
			//alts := []SelectAlternative{{channel: c, element: IntToUnsafePointer(0), action: func(result unsafe.Pointer) {}}}
			defer wg.Done()
			for i := 0; i < n; i++ {
				//for k := 0; k < 100; k++ {}
				//if true { atomic.StorePointer(&x, unsafe.Pointer(&SelectInstance{id:uint64(i)})) }
				//if rand.Int() == 0  { print(x) }
				//select { case c <- i: {} }
				//SelectImpl(alts)
				c.SendInt(i)
				//c <- i
			}
		}()
	}
	for consumer := 0; consumer < CPU / 2; consumer++ {
		go func() {
			//alts := []SelectAlternative{{channel: c, element: ReceiverElement, action: func(result unsafe.Pointer) {}}}
			defer wg.Done()
			for i := 0; i < n; i++ {
				//for k := 0; k < 100; k++ {}
				//if true { atomic.StorePointer(&x, unsafe.Pointer(&SelectInstance{id:uint64(i)})) }
				//if rand.Int() == 0  { print(x) }
				//select { case <- c: {} }
				//SelectImpl(alts)
				c.ReceiveInt()
				//<- c
			}
		}()
	}
	wg.Wait()

	//pprof.WriteHeapProfile(f)
	//defer f.Close()
}

var x unsafe.Pointer

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
