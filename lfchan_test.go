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
	if x != 10 { t.Fatal("Expected ", 10, ", found ", x) }
}

func TestStress(t *testing.T) {
	n := 5000000
	k := 2
	c := NewLFChan(128, 300)
	wg := sync.WaitGroup{}
	// Run sender
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < 100+n; i++ {
				c.sendInt(i)
			}
		}()
	}
	// Run receiver
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < 100+n; i++ {
				x := c.receiveInt()
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
			for i := 100; i < 100+n; i++ {
				c <- ((unsafe.Pointer)((uintptr)(i)))
			}
		}()
	}
	// Run receiver
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 100; i < 100+n; i++ {
				x := (int)((uintptr)(<-c))
				if x != i {
					//t.Fatal("Expected ", i, ", found ", x)
				}
			}
		}()
	}
	wg.Wait()
}

func (c *LFChan) sendInt(element int) {
	c.Send((unsafe.Pointer)((uintptr)(element)))
}

func (c *LFChan) receiveInt() int {
	return (int) ((uintptr) (c.Receive()))
}

