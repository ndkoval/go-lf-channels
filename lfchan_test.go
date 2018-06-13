package go_lf_channels

import (
	"testing"
	"sync"
	"unsafe"
)

func TestSimple(t *testing.T) {
	// Run sender
	c := NewLFChan(1, 10)
	go func() {
		c.sendInt(10)
	}()
	// Receive
	x := c.receiveInt()
	if (x != 10) { t.Fatal("Expected ", 10, ", found ", x) }
}

func TestStress(t *testing.T) {
	n := 100000
	c := NewLFChan(1, 10)
	wg := sync.WaitGroup{}
	// Run sender
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 100 + n; i++ {
			c.sendInt(i)
		}
	}()
	// Run receiver
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 100 + n; i++ {
			x := c.receiveInt()
			if (x != i) { t.Fatal("Expected ", i, ", found ", x) }
		}
	}()
	//go func() {
	//	for {
	//		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	//		time.Sleep(1000)
	//	}
	//}()М
	wg.Wait()
}

func (c *LFChan) sendInt(element int) {
	c.Send((unsafe.Pointer)((uintptr)(element)))
}

func (c *LFChan) receiveInt() int {
	return (int) ((uintptr) (c.Receive()))
}

