package main

import "sync/atomic"

type counters struct {
	lock    uint32
	lowest  uint64
	highest uint64
}

const _counterOffset = 32 // 32
const _counterMask = (1 << _counterOffset) - 1
const _minOverflowedValue = 1 << (_counterOffset - 1)

// if the `lock` field is equals or greater than this value, the write lock is acquired.
const _wLocked = 1 << 30

func (c *counters) acquireReadLock() {
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }
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
	return atomic.CompareAndSwapUint32(&c.lock, 0, _wLocked)
}

func (c *counters) releaseWriteLock() {
	// Decrement the `lock` field by `_wLocked`
	atomic.AddUint32(&c.lock, ^uint32(_wLocked - 1))
}

func (c *counters) getSnapshot() (senders uint64, receivers uint64) {
	// Read both `l` and `h` under the read lock
	c.acquireReadLock()
	h := c.h()
	l := c.l()
	c.releaseReadLock()
	return countCounters(l, h)
}

// Increments senders counter if both senders and receivers counters have not been changed
func (c *counters) tryIncSendersFrom(sendersFrom uint64, receiversFrom uint64) bool {
	return c.tryIncFrom(_counterOffset, sendersFrom, receiversFrom)
}

// Increments senders counter if both senders and receivers counters have not been changed
func (c *counters) tryIncReceiversFrom(sendersFrom uint64, receiversFrom uint64) bool {
	return c.tryIncFrom(0, sendersFrom, receiversFrom)
}

func (c *counters) tryIncFrom(counterOffset uint32, sendersFrom uint64, receiversFrom uint64) bool {
	updated := false
	c.acquireReadLock()
	h := c.highest
	var l uint64
	for {
		l = c.l()
		s, r := countCounters(l, h)
		if r != receiversFrom || s != sendersFrom { break }
		if atomic.CompareAndSwapUint64(&c.lowest, l, l + 1 << counterOffset) {
			updated = true
			break
		}
	}
	c.releaseReadLock()
	if updated { c.fixOverflow(counterOffset, l, h) }
	return updated
}

func (c *counters) incSendersFrom(sendersFrom uint64) bool {
	return c.incFrom(_counterOffset, sendersFrom)
}

func (c *counters) incReceiversFrom(receiversFrom uint64) bool {
	return c.incFrom(0, receiversFrom)
}

func (c *counters) incFrom(counterOffset uint32, from uint64) bool {
	updated := false
	c.acquireReadLock()
	h := c.highest
	counterHPart := counterPart(h, counterOffset)
	var l uint64
	for {
		l = c.l()
		counterLPart := counterPart(l, counterOffset)
		counter := counterLPart + (counterHPart << (_counterOffset - 1))
		if counter != from { break }
		if atomic.CompareAndSwapUint64(&c.lowest, l, l + 1 << counterOffset) {
			updated = true
			break
		}
	}
	c.releaseReadLock()
	if updated { c.fixOverflow(counterOffset, l, h) }
	return updated
}

func (c *counters) incSendersAndGetSnapshot() (senders uint64, receivers uint64) {
	return c.incAndGetSnapshot(_counterOffset)
}

func (c *counters) incReceiversAndGetSnapshot() (senders uint64, receivers uint64) {
	return c.incAndGetSnapshot(0)
}

func (c *counters) incAndGetSnapshot(counterOffset uint32) (senders uint64, receivers uint64) {
	c.acquireReadLock()
	l := atomic.AddUint64(&c.lowest, 1 << counterOffset)
	h := c.highest
	c.releaseReadLock()
	c.fixOverflow(counterOffset, l, counterPart(h, counterOffset))
	return countCounters(l, h)
}

func (c *counters) fixOverflow(counterOffset uint32, curLowest uint64, curHighestPart uint64) {
	if counterPart(curLowest, counterOffset) < _minOverflowedValue { return }
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

func countCounters(lowest uint64, highest uint64) (senders uint64, receivers uint64) {
	ls := lowest >> _counterOffset
	lr := lowest & _counterMask
	hs := highest >> _counterOffset
	hr := highest & _counterMask
	senders = ls + (hs << (_counterOffset - 1))
	receivers = lr + (hr << (_counterOffset - 1))
	return
}