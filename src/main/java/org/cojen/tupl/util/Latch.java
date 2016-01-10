/*
 *  Copyright 2011-2015 Cojen.org
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

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Non-reentrant read/write latch, using unfair acquisition. Implementation
 * also does not track thread ownership or check for illegal usage. As a
 * result, it typically outperforms ReentrantLock and Java synchronization.
 *
 * @author Brian S O'Neill
 * @see LatchCondition
 */
@SuppressWarnings("serial")
public class Latch extends AbstractQueuedSynchronizer {
    // In the inherited state field, high bit is set if exclusive latch is
    // held, and low bits count shared latches. Limited to (2^31)-1 shared latches.

    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    public Latch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE or SHARED
     */
    public Latch(int initialState) {
        setState(initialState);
    }

    // Note: When acquiring the exclusive latch, a check is made first before
    // calling compareAndSetState. Considering that compareAndSetState will
    // check again, this is not strictly required. However, under high
    // contention, performance improves when this check is made. Under low
    // contention, the extra check doesn't appear to slow things down. So it's
    // a performance win overall, for some reason.

    /**
     * Attempt to acquire the exclusive latch, barging ahead of any waiting
     * threads if possible.
     */
    public final boolean tryAcquireExclusive() {
        return getState() == 0 ? compareAndSetState(0, EXCLUSIVE) : false;
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     */
    public final boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        return tryAcquireNanos(0, nanosTimeout);
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if
     * possible.
     */
    public final void acquireExclusive() {
        if (getState() != 0 || !compareAndSetState(0, EXCLUSIVE)) {
            acquire(0);
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public final void acquireExclusiveInterruptibly() throws InterruptedException {
        acquireInterruptibly(0);
    }

    /**
     * Downgrade the held exclusive latch into a shared latch. Caller must later
     * call releaseShared instead of releaseExclusive.
     */
    public final void downgrade() {
        // Release exclusive latch and leave a shared latch.
        release(1);
    }

    /**
     * Release the held exclusive latch.
     */
    public final void releaseExclusive() {
        // Release exclusive latch and leave no shared latch.
        release(0);
    }

    /**
     * Convenience method, which releases the held exclusive or shared latch.
     *
     * @param exclusive call releaseExclusive if true, else call releaseShared.
     */
    public final void release(boolean exclusive) {
        if (exclusive) {
            release(0);
        } else {
            releaseShared(0);
        }
    }

    /**
     * Releases exclusive or shared latch.
     */
    public final void releaseEither() {
        if (getState() < 0) {
            release(0);
        } else {
            releaseShared(0);
        }
    }

    /**
     * Attempt to acquire a shared latch, barging ahead of any waiting threads
     * if possible.
     */
    public final boolean tryAcquireShared() {
        int state;
        while ((state = getState()) >= 0) {
            if (compareAndSetState(state, state + 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempt to acquire a shared latch, aborting if interrupted.
     */
    public final boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        return tryAcquireSharedNanos(0, nanosTimeout);
    }

    /**
     * Acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireShared() {
        acquireShared(0);
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public final void acquireSharedInterruptibly() throws InterruptedException {
        acquireSharedInterruptibly(0);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade
     * fails if shared latch is held by more than one thread. If successful,
     * caller must later call releaseExclusive instead of releaseShared.
     */
    public final boolean tryUpgrade() {
        return getState() == 1 ? compareAndSetState(1, EXCLUSIVE) : false;
    }

    /**
     * Release a held shared latch.
     */
    public final void releaseShared() {
        releaseShared(0);
    }

    @Override
    protected final boolean tryAcquire(int x) {
        return getState() == 0 ? compareAndSetState(0, EXCLUSIVE) : false;
    }

    @Override
    protected final int tryAcquireShared(int x) {
        int state;
        while ((state = getState()) >= 0) {
            if (compareAndSetState(state, state + 1)) {
                return 1;
            }
            if (hasQueuedPredecessors()) {
                return -1;
            }
        }
        return -1;
    }

    @Override
    protected final boolean tryReleaseShared(int x) {
        while (true) {
            int state = getState();
            if (compareAndSetState(state, state - 1)) {
                return state == 1;
            }
        }
    }

    @Override
    protected final boolean tryRelease(int newState) {
        setState(newState);
        return true;
    }
}
