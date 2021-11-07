/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.atomic.LongAdder;

import org.cojen.tupl.io.Utils;

/**
 * Non-reentrant latch implementation which supports highly concurrent shared requests, but
 * exclusive requests are more expensive. The implementation distributes shared state over
 * multiple cells, and so it can consume more memory than a regular latch.
 *
 * @author Brian S O'Neill
 * @see Latch
 */
public class WideLatch {
    // See: "Using LongAdder to make a Reader-Writer Lock" by Concurrency Freaks, and also
    // "NUMA-Aware Reader Writer Locks".
    //
    // Also see CommitLock class, which is reentrant. Consider moving it to this package and
    // renaming it to WideLock, and define an abstract base class to reduce duplication.

    private final LongAdder mSharedAcquire = new LongAdder();
    private final LongAdder mSharedRelease = new LongAdder();

    private final Latch mFullLatch = new Latch();

    private volatile Thread mExclusiveThread;

    private static final VarHandle cExclusiveThreadHandle;

    static {
        try {
            cExclusiveThreadHandle = MethodHandles.lookup().findVarHandle
                (WideLatch.class, "mExclusiveThread", Thread.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    public final boolean tryAcquireShared() {
        mSharedAcquire.increment();
        if (cExclusiveThreadHandle.getAcquire(this) != null) {
            releaseShared();
            return false;
        } else {
            return true;
        }
    }

    /**
     * @param nanosTimeout pass negative for infinite timeout
     */
    public final boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        mSharedAcquire.increment();
        if (cExclusiveThreadHandle.getAcquire(this) != null) {
            releaseShared();
            if (nanosTimeout < 0) {
                mFullLatch.acquireSharedInterruptibly();
            } else if (nanosTimeout == 0 || !mFullLatch.tryAcquireSharedNanos(nanosTimeout)) {
                return false;
            }
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
        return true;
    }

    public final void acquireShared() {
        mSharedAcquire.increment();
        if (cExclusiveThreadHandle.getAcquire(this) != null) {
            releaseShared();
            mFullLatch.acquireShared();
            try {
                mSharedAcquire.increment();
            } finally {
                mFullLatch.releaseShared();
            }
        }
    }

    public final void acquireSharedInterruptibly() throws InterruptedException {
        tryAcquireSharedNanos(-1);
    }

    public final void releaseShared() {
        mSharedRelease.increment();
        var t = (Thread) cExclusiveThreadHandle.getAcquire(this);
        if (t != null && !hasSharedLockers()) {
            Parker.unpark(t);
        }
    }

    public final void acquireExclusive() {
        // If full exclusive lock cannot be immediately obtained, it's due to a shared lock
        // being held for a long time. While waiting for the exclusive lock, all other shared
        // requests are queued. By waiting a timed amount and giving up, the exclusive lock
        // request is effectively de-prioritized. For each retry, the timeout is doubled, to
        // ensure that the exclusive request is not starved.

        long nanosTimeout = 1000; // 1 microsecond
        while (!finishAcquireExclusive(nanosTimeout)) {
            nanosTimeout <<= 1;
        }
    }

    private boolean finishAcquireExclusive(long nanosTimeout) {
        mFullLatch.acquireExclusive();

        // Signal that shared locks cannot be granted anymore.
        cExclusiveThreadHandle.setRelease(this, Thread.currentThread());

        try {
            if (hasSharedLockers()) {
                // Wait for shared locks to be released.

                long nanosEnd = nanosTimeout <= 0 ? 0 : System.nanoTime() + nanosTimeout;

                while (true) {
                    if (nanosTimeout < 0) {
                        Parker.park(this);
                    } else {
                        Parker.parkNanos(this, nanosTimeout);
                    }

                    // Ignore and clear the interrupted status.
                    Thread.interrupted();

                    if (!hasSharedLockers()) {
                        break;
                    }

                    if (nanosTimeout >= 0 &&
                        (nanosTimeout == 0
                         || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0))
                    {
                        releaseExclusive();
                        return false;
                    }
                }
            }

            return true;
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }
    }

    public final void releaseExclusive() {
        cExclusiveThreadHandle.setRelease(this, null);
        mFullLatch.releaseExclusive();
    }

    public final boolean hasQueuedThreads() {
        return mFullLatch.hasQueuedThreads();
    }

    private boolean hasSharedLockers() {
        // Ordering is important here. It prevents observing a release too soon.
        return mSharedRelease.sum() != mSharedAcquire.sum();
    }
}
