/*
 *  Copyright (C) 2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.util;

import java.util.Date;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Scalable non-reentrant read-write lock.
 */
public final class RWLock extends Clutch implements ReadWriteLock {
    private final Pack mPack;
    private final Lock mReadLock;
    private final Lock mWriteLock;

    public RWLock() {
        // Use the minimum recommended number of slots for now. Consider using a larger shared
        // instance at some point.
        mPack = new Pack(16);
        // Early initialize the read/write lock views. This lets callers use
        // reference equality to compare the views.
        mReadLock = new ReadLock();
        mWriteLock = new WriteLock();
    }

    /** Acquires the read lock. */
    public void lock() {
        acquireShared();
    }

    /** Releases the read lock. */
    public void unlock() {
        releaseShared();
    }

    @Override
    public Lock readLock() {
        return mReadLock;
    }

    @Override
    public Lock writeLock() {
        return mWriteLock;
    }

    @Override
    protected Pack getPack() {
        return mPack;
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
            throw new UnsupportedOperationException();
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
            return new WriteCondition();
        }
    }

    private final class WriteCondition extends LatchCondition implements Condition {
        @Override
        public void await() throws InterruptedException {
            if (await(RWLock.this, -1, 0) <= 0) {
                throw new InterruptedException();
            }
        }

        @Override
        public void awaitUninterruptibly() {
            while (await(RWLock.this, -1, 0) <= 0);
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            long end = System.nanoTime();
            if (nanosTimeout > 0) {
                end += nanosTimeout;
            } else {
                nanosTimeout = 0;
            }
            if (await(RWLock.this, nanosTimeout, end) < 0) {
                throw new InterruptedException();
            }
            return end - System.nanoTime();
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            return awaitNanos(unit.toNanos(time)) > 0;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            return await(deadline.getTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
