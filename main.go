package main

import (
	"unsafe"
	"runtime"
	"sync"
)

func main() {
	n := 10000000
	runtime.GOMAXPROCS(2)
	c := NewLFChan()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.SendInt(i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.ReceiveInt()
		}
	}()
	wg.Wait()
}

func IntToUnsafePointer(x int) unsafe.Pointer {
	return (unsafe.Pointer)((uintptr)(x + 4098))
}

func UnsafePointerToInt(p unsafe.Pointer) int {
	return (int) ((uintptr) (p)) - 4098
}

func (c *LFChan) SendInt(element int) {
	c.Send(IntToUnsafePointer(element))
}

func (c *LFChan) ReceiveInt() int {
	return UnsafePointerToInt(c.Receive())
}
