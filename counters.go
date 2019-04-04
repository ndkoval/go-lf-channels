package main

import "sync/atomic"

//type counters struct {
//	lock    uint32
//	highest uint64
//	lowest  uint64
//}

const _counterOffset = 20
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

func (c *LFChan) incSendersAndGetSnapshot() (senders uint64, receivers uint64, bufferEnd uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, 1)
	hSenders := c.hSenders
	hReceivers := c.hReceivers
	hBufferEnd := c.hBufferEnd

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	c.fixOverflow(l, hSenders, hReceivers, hBufferEnd)

	// == STEP 5. Return the snapshot
	return countCounters(l, hSenders, hReceivers, hBufferEnd)
}

func (c *LFChan) incReceiversAndGetSnapshot() (senders uint64, receivers uint64, bufferEnd uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, 1 << _counterOffset)
	hSenders := c.hSenders
	hReceivers := c.hReceivers
	hBufferEnd := c.hBufferEnd

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	c.fixOverflow(l, hSenders, hReceivers, hBufferEnd)

	// == STEP 5. Return the snapshot
	return countCounters(l, hSenders, hReceivers, hBufferEnd)
}

func (c *LFChan) incBufferEndAndGetSnapshot() (senders uint64, receivers uint64, bufferEnd uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, 1 << (_counterOffset * 2))
	hSenders := c.hSenders
	hReceivers := c.hReceivers
	hBufferEnd := c.hBufferEnd

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	c.fixOverflow(l, hSenders, hReceivers, hBufferEnd)

	// == STEP 5. Return the snapshot
	return countCounters(l, hSenders, hReceivers, hBufferEnd)
}

func (c *LFChan) incReceiversAndBufferEndAndGetSnapshot() (senders uint64, receivers uint64, bufferEnd uint64) {
	// == STEP 1. Acquire the read lock ==
	// Increment the number of readers
	lock := atomic.AddUint32(&c.lock, 1)
	// Wait until write lock is released. It can't be acquired again until we decrement the number of readers.
	for lock > _wLocked { lock = atomic.LoadUint32(&c.lock) }

	// == STEP 2. Perform the increment and get the snapshot
	l := atomic.AddUint64(&c.lowest, (1 << _counterOffset) + (1 << (_counterOffset * 2)))
	hSenders := c.hSenders
	hReceivers := c.hReceivers
	hBufferEnd := c.hBufferEnd

	// == STEP 3. Release the read lock
	// Decrement the number of readers
	atomic.AddUint32(&c.lock, ^uint32(0))

	// == STEP 4. Fix overflow if needed
	c.fixOverflow(l, hSenders, hReceivers, hBufferEnd)

	// == STEP 5. Return the snapshot
	return countCounters(l, hSenders, hReceivers, hBufferEnd)
}

func (c *LFChan) fixOverflow(lowest, hSenders, hReceivers, hBufferEnd uint64) {
	fixSenders := (lowest & _counterMask) >= _minOverflowedValue
	fixReceivers := ((lowest >> _counterOffset) & _counterMask) >= _minOverflowedValue
	fixBufferEnd := ((lowest >> (_counterOffset * 2)) & _counterMask) >= _minOverflowedValue

	for c.needToFixOverflow(fixSenders, fixReceivers, fixBufferEnd, hSenders, hReceivers, hBufferEnd) {
		if c.tryAcquireWriteLock() {
			if fixSenders && atomic.LoadUint64(&c.hSenders) == hSenders {
				c.lowest -= _minOverflowedValue
				c.hSenders++
			}
			if fixReceivers && atomic.LoadUint64(&c.hReceivers) == hReceivers {
				c.lowest -= _minOverflowedValue << _counterOffset
				c.hReceivers++
			}
			if fixBufferEnd && atomic.LoadUint64(&c.hBufferEnd) == hBufferEnd {
				c.lowest -= _minOverflowedValue << (_counterOffset * 2)
				c.hBufferEnd++
			}
			c.releaseWriteLock()
			break
		}
	}
}

func (c *LFChan) needToFixOverflow(fixSenders bool, fixReceivers bool, fixBufferEnd bool, hSenders uint64, hReceivers uint64, hBufferEnd uint64) bool {
	if fixSenders && atomic.LoadUint64(&c.hSenders) == hSenders { return true }
	if fixReceivers && atomic.LoadUint64(&c.hReceivers) == hReceivers { return true }
	if fixBufferEnd && atomic.LoadUint64(&c.hBufferEnd) == hBufferEnd { return true }
	return false
}

//go:nosplit
func countCounters(lowest uint64, hSenders uint64, hReceivers uint64, hBufferEnd uint64) (senders uint64, receivers uint64, bufferEnd uint64) {
	senders = (lowest & _counterMask) + (hSenders << (_counterOffset - 1))
	receivers = ((lowest >> _counterOffset) & _counterMask) + (hReceivers << (_counterOffset - 1))
	bufferEnd = ((lowest >> (_counterOffset * 2)) & _counterMask) + (hBufferEnd << (_counterOffset - 1))
	return
}