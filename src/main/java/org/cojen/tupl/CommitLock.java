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
import java.util.concurrent.locks.LockSupport;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.util.Latch;

/**
 * Lock implementation which supports highly concurrent shared requests, but exclusive requests
 * are a little more expensive. Shared lock acquisition is reentrant, but exclusive is not.
 *
 * @author Brian S O'Neill
 */
final class CommitLock implements Lock {
    private final LongAdder mSharedCount = new LongAdder();

    private final Latch mExclusiveLatch = new Latch();
    private final Latch mSharedLatch = new Latch();

    private volatile Thread mExclusiveThread;

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

    /**
     * Acquire shared lock.
     */
    @Override
    public boolean tryLock() {
        return tryLock(reentrant());
    }

    private boolean tryLock(Reentrant reentrant) {
        mSharedCount.increment();

        if (mExclusiveThread != null && reentrant.count <= 0) {
            doReleaseShared();
            return false;
        } else {
            reentrant.count++;
            return true;
        }
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public void lock() {
        mSharedCount.increment();
        Reentrant reentrant = reentrant();
        if (mExclusiveThread != null && reentrant.count <= 0) {
            doReleaseShared();
            mExclusiveLatch.acquireShared();
            mSharedCount.increment();
            mExclusiveLatch.releaseShared();
        }
        reentrant.count++;
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        mSharedCount.increment();
        Reentrant reentrant = reentrant();
        if (mExclusiveThread != null && reentrant.count <= 0) {
            doReleaseShared();
            mExclusiveLatch.acquireSharedInterruptibly();
            mSharedCount.increment();
            mExclusiveLatch.releaseShared();
        }
        reentrant.count++;
    }

    /**
     * Acquire shared lock.
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        mSharedCount.increment();
        Reentrant reentrant = reentrant();
        if (mExclusiveThread != null && reentrant.count <= 0) {
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

    /**
     * Release shared lock.
     */
    @Override
    public void unlock() {
        reentrant().count--;
        doReleaseShared();
    }

    private void doReleaseShared() {
        mSharedCount.decrement();

        if (mExclusiveThread != null) {
            mSharedLatch.acquireShared();
            Thread t = mExclusiveThread;
            if (t != null && mSharedCount.sum() == 0) {
                LockSupport.unpark(t);
            }
            mSharedLatch.releaseShared();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    void acquireExclusive() throws InterruptedIOException {
        // Only one thread can obtain exclusive lock.
        try {
            mExclusiveLatch.acquireExclusiveInterruptibly();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        // If full exclusive lock cannot be immediately obtained, it's due to a shared lock
        // being held for a long time. While waiting for the exclusive lock, all other shared
        // requests are queued. By waiting a timed amount and giving up, the exclusive lock
        // request is effectively de-prioritized. For each retry, the timeout is doubled, to
        // ensure that the exclusive request is not starved.

        try {
            long nanosTimeout = 1000; // 1 microsecond
            while (!finishAcquireExclusive(nanosTimeout)) {
                nanosTimeout <<= 1;
            }
        } catch (Throwable e) {
            mExclusiveLatch.releaseExclusive();
            throw e;
        }
    }

    private boolean finishAcquireExclusive(long nanosTimeout) throws InterruptedIOException {
        // Prepare to wait for shared locks to be released, using a second latch. This also
        // handles race conditions when negative shared counts are observed. The thread which
        // caused a negative count to be observed did so when releasing a shared lock. The
        // thread will acquire the shared latch and check the count again.
        mSharedLatch.acquireExclusive();

        // Signal that shared locks cannot be granted anymore.
        mExclusiveThread = Thread.currentThread();

        if (mSharedCount.sum() != 0) {
            // Wait for shared locks to be released.

            mSharedLatch.releaseExclusive();
            long nanosEnd = nanosTimeout <= 0 ? 0 : System.nanoTime() + nanosTimeout;

            while (true) {
                if (nanosTimeout < 0) {
                    LockSupport.park();
                } else {
                    LockSupport.parkNanos(nanosTimeout);
                }

                if (Thread.interrupted()) {
                    mExclusiveThread = null;
                    throw new InterruptedIOException();
                }

                mSharedLatch.acquireExclusive();

                if (mSharedCount.sum() == 0) {
                    break;
                }

                mSharedLatch.releaseExclusive();

                if (nanosTimeout >= 0 &&
                    (nanosTimeout == 0 || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0))
                {
                    mExclusiveThread = null;
                    return false;
                }
            }
        }

        mSharedLatch.releaseExclusive();
        reentrant().count++;
        return true;
    }

    void releaseExclusive() {
        reentrant().count--;
        mExclusiveThread = null;
        mExclusiveLatch.releaseExclusive();
    }

    boolean hasQueuedThreads() {
        return mExclusiveLatch.hasQueuedThreads();
    }

    @Override
    public String toString() {
        return mSharedCount + ", " + mExclusiveLatch + ", " + mSharedLatch + ", " +
            mExclusiveThread;
    }
}
