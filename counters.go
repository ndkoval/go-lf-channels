package main

import "sync/atomic"

//type counters struct {
//	lock    uint32
//	highest uint64
//	lowest  uint64
//}

const _counterOffset = 32 // 32
const _counterMask = (1 << _counterOffset) - 1
const _minOverflowedValue = 1 << (_counterOffset - 1)

// if the `lock` field is equals or greater than this value, the write lock is acquired.
const _wLocked = 1 << 30

func (c *LFChan) tryAcquireWriteLock() bool {
	// Check if no readers holds the lock
	if atomic.LoadUint32(&c.lock) != 0 { return false }
	// Acquire the write lock if no readers holds it. This strategy is very unfair, but the write lock is used for
	// fixing overflowing only, therefore all other threads will try to acquire it as well.
	return atomic.CompareAndSwapUint32(&c.lock, 0, _wLocked)
}

func (c *LFChan) releaseWriteLock() {
	// Decrement the `lock` field by `_wLocked`
	atomic.AddUint32(&c.lock, ^uint32(_wLocked - 1))
}

func (c *LFChan) incSendersAndGetSnapshot() (senders uint64, receivers uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, 1 << _counterOffset)
	h := c.highest

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	if (l >> _counterOffset) > _minOverflowedValue {
		c.fixOverflow(_counterOffset, l, counterPart(h, _counterOffset))
	}

	// == STEP 5. Return the snapshot
	return countCounters(l, h)
}

func (c *LFChan) incReceiversAndGetSnapshot() (senders uint64, receivers uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, 1)
	h := c.highest

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	if (l & _counterMask) > _minOverflowedValue {
		c.fixOverflow(0, l, counterPart(h, 0))
	}

	// == STEP 5. Return the snapshot
	return countCounters(l, h)
}

func (c *LFChan) fixOverflow(counterOffset uint32, curLowest uint64, curHighestPart uint64) {
	//if counterPart(curLowest, counterOffset) < _minOverflowedValue { return }
	for {
		if c.tryAcquireWriteLock() {
			if counterPart(c.highest, counterOffset) == curHighestPart {
				c.lowest -= _minOverflowedValue << counterOffset
				c.highest += 1 << counterOffset
			}
			c.releaseWriteLock()
			return
		} else {
			if counterPart(c.highest, counterOffset) != curHighestPart { return }
		}
	}
}

func counterPart(counters uint64, counterOffset uint32) uint64 {
	return (counters >> counterOffset) & _counterMask
}

//go:nosplit
func countCounters(lowest uint64, highest uint64) (senders uint64, receivers uint64) {
	ls := lowest >> _counterOffset
	lr := lowest & _counterMask
	hs := highest >> _counterOffset
	hr := highest & _counterMask
	senders = ls + (hs << (_counterOffset - 1))
	receivers = lr + (hr << (_counterOffset - 1))
	return
}