package main

import "sync/atomic"

type counters struct {
	lock    uint32
	lowest  uint64
	highest uint64
}

const sendersOffset = 32
const receiversMask = (1 << sendersOffset) - 1
const sendersInc = 1 << sendersOffset

const minOverflowedValue = 1 << 31

// if the `lock` field is equals or greater than this value, the write lock is acquired.
const wLocked = 1 << 30

func (c *counters) acquireReadLock() {
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > wLocked { lock = atomic.LoadUint32(&c.lock) }
}

func (c *counters) releaseReadLock() {
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))
}

func (c *counters) tryAcquireWriteLock() bool {
	// Check if no readers holds the lock
	if atomic.LoadUint32(&c.lock) != 0 { return false }
	// Acquire the write lock if no readers holds it. This strategy is very unfair, but the write lock is used for
	// fixing overflowing only, therefore all other threads will try to acquire it as well.
	return atomic.CompareAndSwapUint32(&c.lock, 0, wLocked)
}

func (c *counters) releaseWriteLock() {
	// Decrement the `lock` field by `wLocked`
	atomic.AddUint32(&c.lock, ^uint32(wLocked - 1))
}


func countCounters(lowest uint64, highest uint64) (senders uint64, receivers uint64) {
	ls := lowest >> sendersOffset
	lr := lowest & receiversMask
	hs := highest >> sendersOffset
	hr := highest & receiversMask
	senders = ls + (hs << 31)
	receivers = lr + (hr << 31)
	return
}

func (c *counters) getSnapshot() (senders uint64, receivers uint64) {
	c.acquireReadLock()
	h := c.h()
	l := c.l()
	c.releaseReadLock()
	return countCounters(l, h)
}

func (c *counters) tryIncSendersFrom(sendersFrom uint64, receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.highest
		l := c.l()
		s, r := countCounters(l, h)
		if s == sendersFrom && r == receiversFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + sendersInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}


func (c *counters) incSendersFrom(sendersFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.highest
		l := c.l()
		s, _ := countCounters(l, h)
		if s == sendersFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + sendersInc) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *counters) tryIncReceiversFrom(sendersFrom uint64, receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.highest
		l := c.l()
		s, r := countCounters(l, h)
		if r == receiversFrom && s == sendersFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + 1) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *counters) incReceiversFrom(receiversFrom uint64) bool {
	res := false
	c.acquireReadLock()
	for {
		h := c.highest
		l := c.l()
		_, r := countCounters(l, h)
		if r == receiversFrom {
			if atomic.CompareAndSwapUint64(&c.lowest, l, l + 1) {
				res = true
				break
			}
		} else {
			break
		}
	}
	c.releaseReadLock()
	return res
}

func (c *counters) incSendersAndGetSnapshot() (senders uint64, receivers uint64) {
	c.acquireReadLock()
	l := atomic.AddUint64(&c.lowest, sendersInc)
	h := c.highest
	c.releaseReadLock()
	if (l >> sendersOffset) >= minOverflowedValue {
		for {
			if c.tryAcquireWriteLock() {
				if c.highest == h {
					c.lowest -= minOverflowedValue << sendersOffset
					c.highest = h + 1 // todo hl, hr
				}
				c.releaseWriteLock()
			} else {
				if c.highest != h { break }
			}
		}
	}
	return countCounters(l, h)
}

func (c *counters) incReceiversAndGetSnapshot() (senders uint64, receivers uint64) {
	c.acquireReadLock()
	l := atomic.AddUint64(&c.lowest, 1)
	h := c.highest
	c.releaseReadLock()
	if (l & receiversMask) >= minOverflowedValue {
		for {
			if c.tryAcquireWriteLock() {
				if c.highest == h {
					c.lowest -= minOverflowedValue
					c.highest = h + 1
				}
				c.releaseWriteLock()
			} else {
				if c.highest != h { break }
			}
		}
	}
	return countCounters(l, h)
}
