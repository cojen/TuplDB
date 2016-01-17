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

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared lock with a "try" variant which fails when an exclusive lock is requested.
 *
 * @author Brian S O'Neill
 */
final class CommitLock extends ReentrantReadWriteLock {
    private static final AtomicIntegerFieldUpdater<CommitLock> cExclusiveUpdater =
        AtomicIntegerFieldUpdater.newUpdater(CommitLock.class, "mExclusiveRequested");

    private volatile int mExclusiveRequested;

    public boolean tryAcquireShared() {
        return mExclusiveRequested == 0 ? readLock().tryLock() : false;
    }

    public void acquireShared() {
        readLock().lock();
    }

    public void releaseShared() {
        readLock().unlock();
    }

    public void acquireExclusive() throws InterruptedIOException {
        cExclusiveUpdater.incrementAndGet(this);
        try {
            // If the commit lock cannot be immediately obtained, it's due to a shared lock
            // being held for a long time. While waiting for the exclusive lock, all other
            // shared requests are queued. By waiting a timed amount and giving up, the
            // exclusive lock request is effectively de-prioritized. For each retry, the
            // timeout is doubled, to ensure that the exclusive request is not starved.

            Lock writeLock = writeLock();
            try {
                long timeoutMillis = 1;
                while (!writeLock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    timeoutMillis <<= 1;
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        } finally {
            cExclusiveUpdater.decrementAndGet(this);
        }
    }

    public void releaseExclusive() {
        writeLock().unlock();
    }
}
