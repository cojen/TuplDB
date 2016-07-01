/*
 *  Copyright 2016 Cojen.org
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

package org.cojen.tupl;

import java.io.InterruptedIOException;

import java.lang.ref.WeakReference;

import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

/**
 * Lock implementation which supports highly concurrent shared requests, but exclusive requests
 * are a little more expensive. Shared lock acquisition is reentrant, but exclusive is not.
 *
 * @author Brian S O'Neill
 */
final class CommitLock {
    private final LongAdder mSharedCount = new LongAdder();

    private final Latch mExclusiveLatch = new Latch();
    private final Latch mSharedLatch = new Latch();
    private final LatchCondition mSharedCondition = new LatchCondition();

    private volatile boolean mExclusive;

    private Lock mReadLock;

    static final class Reentrant extends WeakReference<Thread> {
        int count;

        Reentrant() {
            super(Thread.currentThread());
        }
    }

    private final ThreadLocal<Reentrant> mReentant = ThreadLocal.withInitial(Reentrant::new);
    private Reentrant mRecentReentant;

    private Reentrant reentrant() {
        Reentrant reentrant = mRecentReentant;
        if (reentrant == null || reentrant.get() != Thread.currentThread()) {
            reentrant = mReentant.get();
            mRecentReentant = reentrant;
        }
        return reentrant;
    }

    public boolean tryAcquireShared() {
        return tryAcquireShared(reentrant());
    }

    private boolean tryAcquireShared(Reentrant reentrant) {
        mSharedCount.increment();

        if (mExclusive && reentrant.count <= 0) {
            doReleaseShared();
            return false;
        } else {
            reentrant.count++;
            return true;
        }
    }

    public void acquireShared() {
        mSharedCount.increment();
        Reentrant reentrant = reentrant();
        if (mExclusive && reentrant.count <= 0) {
            doReleaseShared();
            mExclusiveLatch.acquireShared();
            mSharedCount.increment();
            mExclusiveLatch.releaseShared();
        }
        reentrant.count++;
    }

    public boolean tryAcquireShared(long time, TimeUnit unit) throws InterruptedException {
        mSharedCount.increment();
        Reentrant reentrant = reentrant();
        if (mExclusive && reentrant.count <= 0) {
            doReleaseShared();
            if (time < 0) {
                mExclusiveLatch.acquireShared();
            } else if (time == 0 || !mExclusiveLatch.tryAcquireSharedNanos(unit.toNanos(time))) {
                return false;
            }
            mSharedCount.increment();
            mExclusiveLatch.releaseShared();
        }
        reentrant.count++;
        return true;
    }

    public void releaseShared() {
        reentrant().count--;
        doReleaseShared();
    }

    private void doReleaseShared() {
        mSharedCount.decrement();

        if (mExclusive) {
            mSharedLatch.acquireShared();
            if (mExclusive && mSharedCount.sum() == 0) {
                mSharedCondition.signal();
            }
            mSharedLatch.releaseShared();
        }
    }

    public void acquireExclusive() throws InterruptedIOException {
        // If the exclusive lock cannot be immediately obtained, it's due to a shared lock
        // being held for a long time. While waiting for the exclusive lock, all other shared
        // requests are queued. By waiting a timed amount and giving up, the exclusive lock
        // request is effectively de-prioritized. For each retry, the timeout is doubled, to
        // ensure that the exclusive request is not starved.

        long nanosTimeout = 1000; // 1 microsecond
        while (!tryAcquireExclusive(nanosTimeout)) {
            nanosTimeout <<= 1;
        }
    }

    public boolean tryAcquireExclusive(long nanosTimeout) throws InterruptedIOException {
        // Only one thread can obtain exclusive lock.
        mExclusiveLatch.acquireExclusive();

        // Prepare to wait for shared locks to be released, using a condition variable. This
        // also handles race conditions when negative shared counts are observed. The thread
        // which caused a negative count to be observed did so when releasing a shared lock.
        // The thread will acquire the shared latch and check the count again.
        mSharedLatch.acquireExclusive();

        // Signal that shared locks cannot be granted anymore.
        mExclusive = true;

        if (mSharedCount.sum() != 0) {
            // Wait for shared locks to be released.

            long nanosEnd = System.nanoTime() + nanosTimeout;

            while (true) {
                int result = mSharedCondition.await(mSharedLatch, nanosTimeout, nanosEnd);

                if (mSharedCount.sum() == 0) {
                    break;
                }

                if (result <= 0 || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                    mExclusive = false;
                    mSharedLatch.releaseExclusive();
                    mExclusiveLatch.releaseExclusive();
                    if (result < 0) {
                        throw new InterruptedIOException();
                    } else {
                        return false;
                    }
                }
            }
        }

        mSharedLatch.releaseExclusive();
        reentrant().count++;
        return true;
    }

    public void releaseExclusive() {
        reentrant().count--;
        mExclusive = false;
        mExclusiveLatch.releaseExclusive();
    }

    public boolean hasQueuedThreads() {
        return mExclusiveLatch.hasQueuedThreads();
    }

    public Lock readLock() {
        Lock lock = mReadLock;

        if (lock == null) {
            // Double checked locking is not really harmful here.
            mExclusiveLatch.acquireExclusive();
            try {
                lock = mReadLock;
                if (lock == null) {
                    mReadLock = lock = new ReadLock(this);
                }
            } finally {
                mExclusiveLatch.releaseExclusive();
            }
        }

        return lock;
    }

    @Override
    public String toString() {
        return mSharedCount + ", " + mExclusiveLatch + ", " + mSharedLatch + ", " + mExclusive;
    }

    static final class ReadLock implements Lock {
        private final CommitLock mCommitLock;

        ReadLock(CommitLock commitLock) {
            mCommitLock = commitLock;
        }

        @Override
        public void lock() {
            mCommitLock.acquireShared();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            mCommitLock.acquireShared();
        }

        @Override
        public boolean tryLock() {
            return mCommitLock.tryAcquireShared();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return mCommitLock.tryAcquireShared(time, unit);
        }

        @Override
        public void unlock() {
            mCommitLock.releaseShared();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
