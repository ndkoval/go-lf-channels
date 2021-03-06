package main

import (
	"sync"
	"testing"
	"unsafe"
)

func TestSimple(t *testing.T) {
	// Run sender
	c := NewLFChan(capacity)
	go func() {
		c.SendInt(10)
	}()
	// Receive
	x := c.ReceiveInt()
	if x != 10 { t.Fatal("Expected ", 10, ", found ", x) }
}

func TestSimpleSendAndReceiveWithSelect(t *testing.T) {
	c1 := NewLFChan(capacity)
	dummy := NewLFChan(capacity)
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
	c1 := NewLFChan(capacity)
	c2 := NewLFChan(capacity)
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
	c := NewLFChan(capacity)
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
	k := 10
	c := NewLFChan(capacity)
	dummy := NewLFChan(capacity)
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

func TestStressSelects(t *testing.T) {
	n := 500000
	k := 10
	c := NewLFChan(capacity)
	dummy := NewLFChan(capacity)
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
	c1 := NewLFChan(capacity)
	c2 := NewLFChan(capacity)
	wg := sync.WaitGroup{}
	// Run sender
	for sender := 0; sender < k; sender++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				Select(
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

func testCancellation(t *testing.T) {
	c := NewLFChan(capacity)
	dummy := NewLFChan(capacity)
	n := 100000
	k := 50
	// Add first element to the dummy channel
	go func() {
		dummy.Receive()
	}()
	// Run parallel receiver with selecting on dummy channel as well.
	for i := 0; i < k; i++ {
		go func() {
			for j := 0; j < n; j++ {
				Select(
					SelectAlternative{
						channel: dummy,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) { t.Fatal("Impossible") },
					},
					SelectAlternative{
						channel: c,
						element: ReceiverElement,
						action: func (result unsafe.Pointer) {},
					},
				)
			}
		}()
	}
	// Send n*k elements to the channel
	for i := 0; i < n * k; i++ {
		c.SendInt(1)
	}
	// After this all nodes except for head and tail should be removed from
	// the dummy channel. Wait for a bit at first.
	head := dummy.head()
	headNext := (*segment) (head.next())
	tail := dummy.tail()
	if head == tail || headNext == tail { return }

	cur := head
	for cur.next() != tail {
		println(cur.id)
		cur = cur.next()
	}
	println(tail.id)

	t.Fatal("Channel contains empy nodes which are niether head or tail:")
}

