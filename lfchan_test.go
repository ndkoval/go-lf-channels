package main

import (
	"sync"
	"testing"
	"unsafe"
)

func TestSimple(t *testing.T) {
	// Run sender
	c := NewLFChan()
	go func() {
		c.SendInt(10)
	}()
	// Receive
	x := c.ReceiveInt()
	if x != 10 { t.Fatal("Expected ", 10, ", found ", x) }
}

func TestSimpleSendAndReceiveWithSelect(t *testing.T) {
	c1 := NewLFChan()
	dummy := NewLFChan()
	N := 1000
	// Run sender
	go func() {
		for i := 0; i < N; i++ {
			c1.SendInt(i)
		}
	}()
	// Receive
	for i := 0; i < N; i++ {
		Select(
			SelectAlternative{
				channel: dummy,
				element: ReceiverElement,
				action:  func(result unsafe.Pointer) { t.Fatal("Impossible") },
			},
			SelectAlternative{
				channel: c1,
				element: ReceiverElement,
				action: func(result unsafe.Pointer) {
					x := UnsafePointerToInt(result)
					if x != i {
						t.Fatal("Expected ", i, ", found ", x)
					}
				},
			},
		)
	}
}

func TestSimpleSelects(t *testing.T) {
	c1 := NewLFChan()
	c2 := NewLFChan()
	N := 1000
	// Run sender
	go func() {
		for i := 0; i < N; i++ {
			Select(
				SelectAlternative{
					channel: c1,
					element: IntToUnsafePointer(i),
					action:  func(result unsafe.Pointer) {},
				},
				SelectAlternative{
					channel: c2,
					element: ReceiverElement,
					action:  func(result unsafe.Pointer) { t.Fatal("Impossible") },
				},
			)
		}
	}()
	// Receive
	for i := 0; i < N; i++ {
		Select(
			SelectAlternative{
				channel: c2,
				element: ReceiverElement,
				action:  func(result unsafe.Pointer) { t.Fatal("Impossible") },
			},
			SelectAlternative{
				channel: c1,
				element: ReceiverElement,
				action: func(result unsafe.Pointer) {
					x := UnsafePointerToInt(result)
					if x != i {
						t.Fatal("Expected ", i, ", found ", x)
					}
				},
			},
		)
	}
}

func TestStress(t *testing.T) {
	n := 500000
	k := 10
	c := NewLFChan()
	wg := sync.WaitGroup{}
	// Run sender
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.SendInt(i)
			}
		}()
	}
	// Run receiver
	for xxx := 0; xxx < k; xxx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				x := c.ReceiveInt()
				if x != i {
					//t.Fatal("Expected ", i, ", found ", x)
				}
			}
		}()
	}
	wg.Wait()
}

func TestStressWithSelectOnReceive(t *testing.T) { // TODO
	n := 500000
	k := 1
	c := NewLFChan()
	//dummy := NewLFChan(capacity)
	wg := sync.WaitGroup{}
	// Run sender
	for sender := 0; sender < k; sender++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				c.SendInt(i)
			}
		}()
	}
	// Run receiver
	for receiver := 0; receiver < k; receiver++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				SelectUnbiased(
					SelectAlternative{
						channel: c,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) {
							x := UnsafePointerToInt(result)
							if  x != i {
								//t.Fatal("Expected ", i, ", found ", x)
							}
						},
					},
					//SelectAlternative{
					//	channel: dummy,
					//	element: ReceiverElement,
					//	action: func (result unsafe.Pointer) { t.Fatal("Impossible") },
					//},
				)
			}
		}()
	}
	wg.Wait()
}

func TestStressSelects(t *testing.T) {
	n := 500000
	k := 10
	c := NewLFChan()
	dummy := NewLFChan()
	wg := sync.WaitGroup{}
	// Run sender
	for sender := 0; sender < k; sender++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				SelectUnbiased(
					SelectAlternative{
						channel: c,
						element: IntToUnsafePointer(i),
						action: func (result unsafe.Pointer) {},
					},
					SelectAlternative{
						channel: dummy,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) { t.Fatal("Impossible") },
					},
				)
			}
		}()
	}
	// Run receiver
	for receiver := 0; receiver < k; receiver++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				SelectUnbiased(
					SelectAlternative{
						channel: c,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) {},
					},
					SelectAlternative{
						channel: dummy,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) { t.Fatal("Impossible") },
					},
				)
			}
		}()
	}
	wg.Wait()
}


func TestStressSelectsOver2Channels(t *testing.T) {
	n := 500000
	k := 10
	c1 := NewLFChan()
	c2 := NewLFChan()
	wg := sync.WaitGroup{}
	// Run sender
	for sender := 0; sender < k; sender++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				SelectUnbiased(
					SelectAlternative{
						channel: c1,
						element: IntToUnsafePointer(i),
						action: func (result unsafe.Pointer) {},
					},
					SelectAlternative{
						channel: c2,
						element: IntToUnsafePointer(i),
						action: func (result unsafe.Pointer) {},
					},
				)
			}
		}()
	}
	// Run receiver
	for receiver := 0; receiver < k; receiver++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				SelectUnbiased(
					SelectAlternative{
						channel: c1,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) {},
					},
					SelectAlternative{
						channel: c2,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) {},
					},
				)
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