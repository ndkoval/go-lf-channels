package main

import (
	"unsafe"
	"runtime"
	"sync"
)

func main() {
	CPU := 144
	n := 100000
	runtime.GOMAXPROCS(CPU)
	c := NewLFChan()
	wg := sync.WaitGroup{}
	wg.Add(CPU)
	for producer := 0; producer < CPU / 2; producer++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.SendInt(i)
			}
		}()
	}
	for consumer := 0; consumer < CPU / 2; consumer++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.ReceiveInt()
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
