/*
 *  Copyright 2011-2017 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.cojen.tupl.io.UnsafeAccess;

/**
 * Scalable non-reentrant read-write lock, with writer bias.
 * 
 * An exclusive write lock (after acquiring the latch) is wait-free, bounded by
 * the number of read slots. An exclusive writer spins while waiting for
 * ongoing readers to release which may be expensive if shared locks are held
 * for a long time.
 */
public final class RWLock implements ReadWriteLock {
    private final ReadIndicator readers;
    private final Latch latch;

    private final Lock readLock;
    private final Lock writeLock;

    public RWLock() {
        readers = new ReadIndicator();
        latch = new Latch();

        // Early initialize the read/write lock views. This lets callers use
        // reference equality to compare the views.
        readLock = new ReadLock();
        writeLock = new WriteLock();
    }

    /** Acquires the read lock. */
    public void lock() { acquireShared(); }

    /** Releases the read lock. */
    public void unlock() { releaseShared(); }

    /**
     * Acquires the read lock.
     */
    public void acquireShared() {
        int idx = readers.increment();
        if (!latch.isHeldExclusive()) {
            // Acquired lock in read-only mode
            return;
        } else {
            // Rollback counter to avoid blocking a Writer
            readers.decrementAt(idx);

            // Wait for exclusive writer to finish.
            latch.acquireShared();
            // Bump the read indicator with the shared latch held.
            readers.incrementAt(idx);
            latch.releaseShared();
        }
    }

    /**
     * Release the read lock.
     */
    public void releaseShared() {
        readers.decrement();
    }

    /**
     * Acquires the write lock.
     */
    public void acquireExclusive() {
        latch.acquireExclusive();
        while (true) {
            if (readers.isZero()) return;
            // Spin until existing readers are done. New readers will see that
            // the latch is held exclusive and backoff, waiting for release of
            // the exclusive latch.
            Thread.yield();
        }
    }

    /**
     * Attempts to release the write lock.
     * 
     * @throws IllegalMonitorStateException if the write lock is not held.
     */
    public void releaseExclusive() {
        if (!latch.isHeldExclusive()) {
            // Tried to unlock a non write-locked instance
            throw new IllegalMonitorStateException();
        }
        latch.releaseExclusive();
    }

    /**
     * Downgrades a write lock to a read lock.
     * 
     * @throws IllegalMonitorStateException if the write lock is not held.
     */
    public void downgrade() {
        if (!latch.isHeldExclusive()) {
            // Tried to downgrade a non write-locked instance
            throw new IllegalMonitorStateException();
        }
        readers.increment();
        latch.releaseExclusive();
    }

    /**
     * Acquires the read lock only if the write lock is not held by another
     * thread at the time of invocation.
     */
    public boolean tryAcquireShared() {
        int idx = readers.increment();
        if (!latch.isHeldExclusive()) {
            // Acquired lock in read-only mode
            return true;
        } 
        readers.decrementAt(idx);
        return false;
    }

    /**
     * Acquires the read lock if the write lock is not held by another thread
     * within the given waiting time.
     */
    public boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        int idx = readers.increment();
        if (!latch.isHeldExclusive()) {
            // Acquired lock in read-only mode
            return true;
        } 

        // Rollback and wait when there's a writer.
        readers.decrementAt(idx);
        if (latch.tryAcquireSharedNanos(nanosTimeout)) {
            readers.incrementAt(idx);
            latch.releaseShared();
            return true;
        }

        // Time has expired and there is still a writer so give up.
        return false;
    }

    /**
     * Acquires the write lock only if it is not held by another thread at the
     * time of invocation.
     */   
    public boolean tryAcquireExclusive() {
        if (!latch.tryAcquireExclusive()) {
            return false;
        }
        if (readers.isZero()) return true;
        latch.releaseExclusive();
        return false;
    }

    /**
     * Acquires the write lock if it is not held by another thread within the
     * given waiting time.
     */    
    public boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        if (!latch.tryAcquireExclusiveNanos(nanosTimeout)) {
            return false;
        }

        final long deadline = System.nanoTime() + nanosTimeout;
        while (true) {
            if (readers.isZero()) return true;
            if (deadline - System.nanoTime() > 0) {
                Thread.yield();
            } else { 
                // Time has expired and there is still at least one reader so give up.
                latch.releaseExclusive();
                return false;
            }  
        }
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    /**
     * Read indicator representing the count of shared lock owners. May over represent
     * the number of owners transiently as new readers first optimistically increment
     * the indicator before checking that there's either an exclusive waiter or owner.
     * In those cases they immediately decrement to rollback their shared acquisition.
     *
     * Uses a padded striped array of longs to reduce contention among concurrent
     * shared lock attempts.
     */
    private static final class ReadIndicator {
        private static final int NCPU = Runtime.getRuntime().availableProcessors();
        private static final int SLOTS = NCPU <= 1 ? 2 : Integer.highestOneBit(NCPU - 1) << 2;

        private volatile long relaxed = 0;
        private final long[] array;

        ReadIndicator() { 
            // Cache-line padded for each slot, and at the beginning and end of the array.
            array = new long[16 + 8 * SLOTS];
        }

        private static final int idxFor(long probe) {
            probe ^= probe << 13;   // xorshift rng
            probe ^= probe >>> 17;
            probe ^= probe << 5;
            return (int) (probe & (SLOTS - 1));
        }

        public boolean isZero() {
            // Volatile read prevents reordering the relaxed slot gets combined
            // with the loadFence() below.
            long sum = relaxed;
            for (int i = 0; i < SLOTS && sum == 0; i++) {
                sum += get(i);
            }
            UNSAFE.loadFence();
            return sum == 0;
        }

        private long get(int idx) {
            return UNSAFE.getLong(array, byteOffset(idx));
        }

        public int increment() {
            return add(1L);
        }

        public int decrement() {
            return add(-1L);
        }

        public void incrementAt(int idx) {
            addAt(1L, idx);
        }

        public void decrementAt(int idx) {
            addAt(-1L, idx);
        }

        private int add(long x) {
            int idx = idxFor(Thread.currentThread().getId());
            addAt(x, idx);
            return idx;
        }

        private void addAt(long x, int idx) {
            UNSAFE.getAndAddLong(array, byteOffset(idx), x);
        }

        private static long byteOffset(int idx) {
            return 64L + (((long) (idx << 3) << SHIFT)) + BASE;
        }

        // Unsafe mechanics
        private static final sun.misc.Unsafe UNSAFE;
        private static final long BASE;
        private static final long SHIFT;

        static {
            try {
                UNSAFE = UnsafeAccess.obtain(); 
                BASE = UNSAFE.arrayBaseOffset(long[].class);
                int scale = UNSAFE.arrayIndexScale(long[].class);
                if ((scale & (scale - 1)) != 0)
                    throw new Error("data type scale not a power of two");
                SHIFT = 31 - Integer.numberOfLeadingZeros(scale);
            } catch (Exception e) {
                throw new Error(e);
            }
        }
    }

    /** Read lock view. */
    private final class ReadLock implements Lock {

        @Override
        public void lock() {
            acquireShared();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            acquireShared();
        }

        @Override
        public boolean tryLock() {
            return tryAcquireShared();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return tryAcquireSharedNanos(unit.toNanos(time));
        }

        @Override
        public void unlock() {
            releaseShared();
        }

        @Override
        public Condition newCondition() {
            return null;
        }
    }

    /** Write lock view. */
    private final class WriteLock implements Lock {

        @Override
        public void lock() {
            acquireExclusive();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            acquireExclusive();
        }

        @Override
        public boolean tryLock() {
            return tryAcquireExclusive();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return tryAcquireExclusiveNanos(unit.toNanos(time));
        }

        @Override
        public void unlock() {
            releaseExclusive();
        }

        @Override
        public Condition newCondition() {
            return null;
        }
    }
}
