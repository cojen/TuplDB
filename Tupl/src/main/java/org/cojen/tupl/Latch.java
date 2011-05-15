/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Non-reentrant read/write latch, supporting fair and unfair acquisition methods.
 *
 * @author Brian S O'Neill
 */
class Latch extends AbstractQueuedSynchronizer {
    // In the state field, high bit is set if exclusive latch is held, and low
    // bits count shared latches. Limited to 2^31 shared latches.

    private static final int FAIR = 0, UNFAIR = 1;

    /**
     * Attempt to acquire the exclusive latch.
     */
    public final boolean tryAcquireExclusive() {
        return !hasQueuedThreads() && compareAndSetState(0, 0x80000001);
    }

    /**
     * Attempt to acquire the exclusive latch, barging ahead of any waiting
     * threads if possible.
     */
    public final boolean tryAcquireExclusiveUnfair() {
        return compareAndSetState(0, 0x80000001);
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     */
    public final boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        return tryAcquireNanos(FAIR, nanosTimeout);
    }

    /**
     * Acquire the exclusive latch.
     */
    public final void acquireExclusive() {
        acquire(FAIR);
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if
     * possible.
     */
    public final void acquireExclusiveUnfair() {
        acquire(UNFAIR);
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public final void acquireExclusiveInterruptibly() throws InterruptedException {
        acquireInterruptibly(FAIR);
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
     * Attempt to acquire a shared latch.
     */
    public final boolean tryAcquireShared() {
        return tryAcquireShared(FAIR) >= 0;
    }

    /**
     * Attempt to acquire a shared latch, barging ahead of any waiting threads
     * if possible.
     */
    public final boolean tryAcquireSharedUnfair() {
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
        return tryAcquireSharedNanos(FAIR, nanosTimeout);
    }

    /**
     * Acquire a shared latch.
     */
    public final void acquireShared() {
        acquireShared(FAIR);
    }

    /**
     * Acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireSharedUnfair() {
        acquireShared(UNFAIR);
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public final void acquireSharedInterruptibly() throws InterruptedException {
        acquireSharedInterruptibly(FAIR);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade
     * fails if shared latch is held by more than one thread. If successful,
     * caller must later call releaseExclusive instead of releaseShared.
     */
    public final boolean tryUpgrade() {
        return compareAndSetState(1, 0x80000001);
    }

    /**
     * Release a held shared latch.
     */
    public final void releaseShared() {
        releaseShared(0);
    }

    @Override
    protected final boolean tryAcquire(int mode) {
        return (mode != FAIR || (getState() == 0 && !shouldWait()))
            && compareAndSetState(0, 0x80000001);
    }

    @Override
    protected final int tryAcquireShared(int mode) {
        int state = getState();
        if (state >= 0 && (mode != FAIR || !shouldWait())) {
            do {
                if (compareAndSetState(state, state + 1)) {
                    return 1;
                }
            } while ((state = getState()) >= 0);
        }
        return -1;
    }

    @Override
    protected final boolean tryReleaseShared(int arg) {
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

    private boolean shouldWait() {
        // TODO: How to support calling hasQueuedPredecessors in jdk 1.7?
        return hasQueuedThreads() && getFirstQueuedThread() != Thread.currentThread();
    }
}
