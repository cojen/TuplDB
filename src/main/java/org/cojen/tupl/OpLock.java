/*
 *  Copyright 2017 Cojen.org
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

import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.util.Latch;

/**
 * Non-reentrant variant of CommitLock which also permits acquire/release pairs to be called
 * from different threads.
 *
 * @author Brian S O'Neill
 */
class OpLock {
    private final LongAdder mSharedAcquire = new LongAdder();
    private final LongAdder mSharedRelease = new LongAdder();

    private final Latch mFullLatch = new Latch();

    private volatile Thread mExclusiveThread;

    void acquireShared() {
        mSharedAcquire.increment();
        if (mExclusiveThread != null) {
            releaseShared();
            mFullLatch.acquireShared();
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
    }

    void releaseShared() {
        mSharedRelease.increment();
        Thread t = mExclusiveThread;
        if (t != null && !hasSharedLockers()) {
            LockSupport.unpark(t);
        }
    }

    void acquireExclusive() throws InterruptedIOException {
        try {
            mFullLatch.acquireExclusiveInterruptibly();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        // Signal that shared locks cannot be granted anymore.
        mExclusiveThread = Thread.currentThread();

        try {
            if (hasSharedLockers()) {
                // Wait for shared locks to be released.
                while (true) {
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        throw new InterruptedIOException();
                    }
                    if (!hasSharedLockers()) {
                        return;
                    }
                }
            }
        } catch (Throwable e) {
            mExclusiveThread = null;
            mFullLatch.releaseExclusive();
            throw e;
        }
    }

    void releaseExclusive() {
        mExclusiveThread = null;
        mFullLatch.releaseExclusive();
    }

    boolean hasQueuedThreads() {
        return mFullLatch.hasQueuedThreads();
    }

    private boolean hasSharedLockers() {
        // Ordering is important here. It prevents observing a release too soon.
        return mSharedRelease.sum() != mSharedAcquire.sum();
    }
}
