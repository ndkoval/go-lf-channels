package go_lf_channels

import (
	"testing"
	"sync"
	"unsafe"
)

func TestSimple(t *testing.T) {
	// Run sender
	c := NewLFChan(10)
	go func() {
		c.SendInt(10)
	}()
	// Receive
	x := c.ReceiveInt()
	if x != 10 { t.Fatal("Expected ", 10, ", found ", x) }
}

func TestStress(t *testing.T) {
	n := 5000000
	k := 1
	c := NewLFChan(300)
	wg := sync.WaitGroup{}
	// Run sender
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < n; i++ {
				c.SendInt(i)
			}
		}()
	}
	// Run receiver
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < n; i++ {
				x := c.ReceiveInt()
				if x != i {
					//t.Fatal("Expected ", i, ", found ", x)
				}
			}
		}()
	}
	wg.Wait()
}

func TestStressGo(t *testing.T) {
	n := 5000000
	k := 2
	c := make(chan unsafe.Pointer, 0)
	wg := sync.WaitGroup{}
	// Run sender
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < n; i++ {
				c <- ((unsafe.Pointer)((uintptr)(i)))
			}
		}()
	}
	// Run receiver
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < n; i++ {
				x := (int)((uintptr)(<-c))
				if x != i {
					//t.Fatal("Expected ", i, ", found ", x)
				}
			}
		}()
	}
	wg.Wait()
}

func IntToUnsafePointer(x int) unsafe.Pointer {
	return (unsafe.Pointer)((uintptr)(x + 100))
}

func (c *LFChan) SendInt(element int) {
	c.Send(IntToUnsafePointer(element))
}

func (c *LFChan) ReceiveInt() int {
	return (int) ((uintptr) (c.Receive())) - 100
}

