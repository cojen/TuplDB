/*
 *  Copyright 2011-2016 Cojen.org
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

import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.io.UnsafeAccess;

/**
 * Alternative latch implementation which maintains less state.
 *
 * @author Brian S O'Neill
 */
class AltLatch {
    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    // TODO: Switch to VarHandle when available and utilize specialized operations. 

    static final Unsafe UNSAFE = UnsafeAccess.obtain();

    static final long STATE_OFFSET, FIRST_OFFSET, LAST_OFFSET;
    static final long WAITER_OFFSET;

    static {
        try {
            Class clazz = AltLatch.class;
            STATE_OFFSET = UNSAFE.objectFieldOffset(clazz.getDeclaredField("mLatchState"));
            FIRST_OFFSET = UNSAFE.objectFieldOffset(clazz.getDeclaredField("mLatchFirst"));
            LAST_OFFSET = UNSAFE.objectFieldOffset(clazz.getDeclaredField("mLatchLast"));

            clazz = WaitNode.class;
            WAITER_OFFSET = UNSAFE.objectFieldOffset(clazz.getDeclaredField("mWaiter"));
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /*
      unlatched:           0               latch is available
      shared:              1..0x7fffffff   latch is held shared
      exclusive:  0x80000000               latch is held exclusively
      illegal:    0x80000001..0xffffffff   illegal exclusive state
     */ 
    volatile int mLatchState;

    // Queue of waiting threads.
    private transient volatile WaitNode mLatchFirst;
    private transient volatile WaitNode mLatchLast;

    AltLatch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    AltLatch(int initialState) {
        // Assume that this latch instance is published to other threads safely, and so a
        // volatile store isn't required.
        UNSAFE.putInt(this, STATE_OFFSET, initialState);
    }

    /**
     * Try to acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final boolean tryAcquireExclusive() {
        return mLatchState == 0 && UNSAFE.compareAndSwapInt(this, STATE_OFFSET, 0, EXCLUSIVE);
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public final boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireExclusiveNanos(nanosTimeout);
    }

    private boolean doTryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        if (tryAcquireExclusive() || tryAcquireExclusiveSpin()) {
            return true;
        }

        if (nanosTimeout == 0) {
            return false;
        }

        boolean result = acquire(new Timed(nanosTimeout));

        if (!result && (Thread.interrupted() || nanosTimeout < 0)) {
            throw new InterruptedException();
        }

        return result;
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireExclusive() {
        if (!tryAcquireExclusive() && !tryAcquireExclusiveSpin()) {
            acquire(new WaitNode());
        }
    }

    /**
     * Caller should have already called tryAcquireExclusive.
     */
    private boolean tryAcquireExclusiveSpin() {
        // Try a few more times, avoiding an expensive enqueue.
        for (int i=1; i<SPIN_LIMIT; i++) {
            if (tryAcquireExclusive()) {
                return true;
            }
        }
        return false;
    }

    private boolean tryAcquireExclusiveAfterParking() {
        // Try many times before requesting fair handoff and parking again.
        int i = 0;
        while (true) {
            if (tryAcquireExclusive() || tryAcquireExclusiveSpin()) {
                return true;
            }
            if (++i >= SPIN_LIMIT >> 1) {
                return false;
            }
            Thread.yield();
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public final void acquireExclusiveInterruptibly() throws InterruptedException {
        doTryAcquireExclusiveNanos(-1);
    }

    /**
     * Downgrade the held exclusive latch into a shared latch. Caller must later call
     * releaseShared instead of releaseExclusive.
     */
    public final void downgrade() {
        mLatchState = 1;

        while (true) {
            // Sweep through the queue, waking up a contiguous run of shared waiters.
            final WaitNode first = first();
            if (first == null) {
                return;
            }

            WaitNode node = first;
            while (true) {
                Thread waiter = node.mWaiter;
                if (waiter != null) {
                    if (node instanceof Shared) {
                        UNSAFE.getAndAddInt(this, STATE_OFFSET, 1);
                        if (UNSAFE.compareAndSwapObject(node, WAITER_OFFSET, waiter, null)) {
                            LockSupport.unpark(waiter);
                        } else {
                            // Already unparked, so fix the share count.
                            UNSAFE.getAndAddInt(this, STATE_OFFSET, -1);
                        }
                    } else {
                        if (node != first) {
                            // Advance the queue past any shared waiters that were encountered.
                            mLatchFirst = node;
                        }
                        return;
                    }
                }

                WaitNode next = node.get();

                if (next == null) {
                    // Queue is now empty, unless an enqueue is in progress.
                    if (UNSAFE.compareAndSwapObject(this, LAST_OFFSET, node, null)) {
                        UNSAFE.compareAndSwapObject(this, FIRST_OFFSET, first, null);
                        return;
                    }
                    // Sweep from the start again.
                    break;
                }

                node = next;
            }
        }
    }

    /**
     * Release the held exclusive latch.
     */
    public final void releaseExclusive() {
        int trials = 0;
        while (true) {
            WaitNode last = mLatchLast;

            if (last == null) {
                // No waiters, so release the latch.
                mLatchState = 0;

                // Need to check if any waiters again, due to race with enqueue. If cannot
                // immediately re-acquire the latch, then let the new owner (which barged in)
                // unpark the waiters when it releases the latch.
                last = mLatchLast;
                if (last == null || !UNSAFE.compareAndSwapInt(this, STATE_OFFSET, 0, EXCLUSIVE)) {
                    return;
                }
            }

            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            WaitNode first = mLatchFirst;

            unpark: if (first != null) {
                Thread waiter = first.mWaiter;

                if (waiter != null) {
                    if (first instanceof Shared) {
                        // TODO: can this be combined into one downgrade step?
                        downgrade();
                        doReleaseShared(mLatchState);
                        return;
                    }

                    if (!first.mDenied) {
                        // Unpark the waiter, but allow another thread to barge in.
                        mLatchState = 0;
                        LockSupport.unpark(waiter);
                        return;
                    }
                }

                // Remove first from the queue.
                {
                    WaitNode next = first.get();
                    if (next != null) {
                        mLatchFirst = next;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (last != first ||
                            !UNSAFE.compareAndSwapObject(this, LAST_OFFSET, last, null))
                        {
                            break unpark;
                        }
                        UNSAFE.compareAndSwapObject(this, FIRST_OFFSET, last, null);
                    }
                }

                if (waiter != null &&
                    UNSAFE.compareAndSwapObject(first, WAITER_OFFSET, waiter, null))
                {
                    // Fair handoff to waiting thread.
                    LockSupport.unpark(waiter);
                    return;
                }
            }

            trials = spin(trials);
        }
    }

    /**
     * Convenience method, which releases the held exclusive or shared latch.
     *
     * @param exclusive call releaseExclusive if true, else call releaseShared
     */
    public final void release(boolean exclusive) {
        if (exclusive) {
            releaseExclusive();
        } else {
            doReleaseShared(mLatchState);
        }
    }

    /**
     * Releases an exclusive or shared latch.
     */
    public final void releaseEither() {
        int state = mLatchState;
        if (state == EXCLUSIVE) {
            releaseExclusive();
        } else {
            doReleaseShared(state);
        }
    }

    /**
     * Try to acquire the shared latch, barging ahead of any waiting threads if possible.
     */
    public final boolean tryAcquireShared() {
        WaitNode first = mLatchFirst;
        if (first != null && !(first instanceof Shared)) {
            return false;
        }
        int state = mLatchState;
        return state >= 0 && UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1);
    }

    /**
     * Attempt to acquire a shared latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public final boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireSharedNanos(nanosTimeout);
    }

    private final boolean doTryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1)) {
                    return true;
                }
                // Spin even if timeout is zero. The timeout applies to a blocking acquire.
                trials = spin(trials);
            }
        }

        if (nanosTimeout == 0) {
            return false;
        }

        boolean result = acquire(new TimedShared(nanosTimeout));

        if (!result && (Thread.interrupted() || nanosTimeout < 0)) {
            throw new InterruptedException();
        }

        return result;
    }

    /**
     * Like tryAcquireShared, except blocks if an exclusive latch is held.
     *
     * @return false if not acquired due to contention with other shared requests
     */
    public final boolean weakAcquireShared() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int state = mLatchState;
            if (state >= 0) {
                return UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1);
            }
        }

        return acquire(new Shared());
    }

    /**
     * Acquire the shared latch, barging ahead of any waiting threads if possible.
     */
    public final void acquireShared() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1)) {
                    return;
                }
                trials = spin(trials);
            }
        }

        acquire(new Shared());
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public final void acquireSharedInterruptibly() throws InterruptedException {
        doTryAcquireSharedNanos(-1);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade fails if shared
     * latch is held by more than one thread. If successful, caller must later call
     * releaseExclusive instead of releaseShared.
     */
    public final boolean tryUpgrade() {
        return doTryUpgrade();
    }

    private boolean doTryUpgrade() {
        while (true) {
            int state = mLatchState;
            if ((state & ~EXCLUSIVE) != 1) {
                return false;
            }
            if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, EXCLUSIVE)) {
                return true;
            }
            // Try again if exclusive bit flipped. Don't bother with spin yielding, because the
            // exclusive bit usually switches to 1, not 0.
        }
    }

    /**
     * Release a held shared latch.
     */
    public final void releaseShared() {
        doReleaseShared(mLatchState);
    }

    private void doReleaseShared(int state) {
        int trials = 0;
        while (true) {
            WaitNode last = mLatchLast;
            if (last == null) {
                // No waiters, so release the latch.
                if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, --state)) {
                    if (state == 0) {
                        // Need to check if any waiters again, due to race with enqueue. If
                        // cannot immediately re-acquire the latch, then let the new owner
                        // (which barged in) unpark the waiters when it releases the latch.
                        last = mLatchLast;
                        if (last != null &&
                            UNSAFE.compareAndSwapInt(this, STATE_OFFSET, 0, EXCLUSIVE))
                        {
                            releaseExclusive();
                        }
                    }
                    return;
                }
            } else if (state == 1) {
                // Try to switch to exclusive, and then let releaseExclusive deal with
                // unparking the waiters.
                if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, 1, EXCLUSIVE) || doTryUpgrade()) {
                    releaseExclusive();
                    return;
                }
            } else if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, --state)) {
                return;
            }

            trials = spin(trials);
            state = mLatchState;
        }
    }

    private boolean acquire(final WaitNode node) {
        // Enqueue the node.
        WaitNode prev;
        {
            node.mWaiter = Thread.currentThread();
            prev = (WaitNode) UNSAFE.getAndSetObject(this, LAST_OFFSET, node);
            if (prev == null) {
                mLatchFirst = node;
            } else {
                prev.set(node);
                WaitNode pp = prev.mPrev;
                if (pp != null) {
                    // The old last node was intended to be removed, but the last node cannot
                    // be removed unless it's also the first. Bypass it now that a new last
                    // node has been enqueued.
                    pp.lazySet(node);
                }
            }
        }

        int acquireResult = node.acquire(this);

        if (acquireResult < 0) {
            int denied = 0;
            while (true) {
                boolean parkAbort = node.park(this);

                if (node.mWaiter == null) {
                    // Fair handoff, and so node is no longer in the queue.
                    return true;
                }

                acquireResult = node.acquire(this);

                if (acquireResult >= 0) {
                    // Latch acquired after parking.
                    break;
                }

                if (parkAbort) {
                    if (!UNSAFE.compareAndSwapObject
                        (node, WAITER_OFFSET, Thread.currentThread(), null))
                    {
                        // Fair handoff just occurred.
                        return true;
                    }

                    // Remove the node from the queue. If it's the first, it cannot be safely
                    // removed without the latch having been properly acquired. So let it
                    // linger around until the latch is released.
                    if (prev != null) {
                        remove(node, prev);
                    }

                    return false;
                }

                // Lost the race. Request fair handoff.

                if (denied++ == 0) {
                    node.mDenied = true;
                }
            }
        }

        if (acquireResult != 0) {
            // Only remove the node if requested to do so.
            return true;
        }

        // Remove the node now, releasing memory.

        if (mLatchFirst != node) {
            remove(node, prev);
            return true;
        }

        // Removing the first node requires special attention. Because the latch is now held by
        // the current thread, no other dequeues are in progress, but enqueues still are.

        while (true) {
            WaitNode next = node.get();
            if (next != null) {
                mLatchFirst = next;
                return true;
            } else {
                // Queue is now empty, unless an enqueue is in progress.
                WaitNode last = mLatchLast;
                if (last == node && UNSAFE.compareAndSwapObject(this, LAST_OFFSET, last, null)) {
                    UNSAFE.compareAndSwapObject(this, FIRST_OFFSET, last, null);
                    return true;
                }
            }
        }
    }

    /**
     * @param node node to remove, not null
     * @param prev previous node, not null
     */
    private void remove(final WaitNode node, final WaitNode prev) {
        WaitNode next = node.get();

        if (next == null) {
            // Removing the last node creates race conditions with enqueues. Instead, stash a
            // reference to the previous node and let the enqueue deal with it after a new node
            // has been enqueued.
            node.mPrev = prev;
            next = node.get();
            // Double check in case an enqueue just occurred that may have failed to notice the
            // previous node assignment.
            if (next == null) {
                return;
            }
        }

        while (next.mWaiter == null) {
            // Skip more nodes if possible.
            WaitNode nextNext = next.get();
            if (nextNext == null) {
                break;
            }
            next = nextNext;
        }

        // Bypass the removed node, allowing it to be released.
        prev.lazySet(next);
    }

    private WaitNode first() {
        int trials = 0;
        while (true) {
            WaitNode last = mLatchLast;
            if (last == null) {
                return null;
            }
            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            WaitNode first = mLatchFirst;
            if (first != null) {
                return first;
            }
            trials = spin(trials);
        }
    }

    public final boolean hasQueuedThreads() {
        return mLatchLast != null;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        Utils.appendMiniString(b, this);
        b.append(" {state=");

        int state = mLatchState;
        if (state == 0) {
            b.append("unlatched");
        } else if (state == EXCLUSIVE) {
            b.append("exclusive");
        } else if (state >= 0) {
            b.append("shared:").append(state);
        } else {
            b.append("illegal:").append(state);
        }

        WaitNode last = mLatchLast;

        if (last != null) {
            b.append(", ");
            WaitNode first = mLatchFirst;
            if (first == last) {
                b.append("firstQueued=").append(last);
            } else if (first == null) {
                b.append("lastQueued=").append(last);
            } else {
                b.append("firstQueued=").append(first)
                    .append(", lastQueued=").append(last);
            }
        }

        return b.append('}').toString();
    }

    /**
     * @return new trials value
     */
    static int spin(int trials) {
        trials++;
        if (trials >= SPIN_LIMIT) {
            Thread.yield();
            trials = 0;
        }
        return trials;
    }

    /**
     * Atomic reference is to the next node in the chain.
     */
    static class WaitNode extends AtomicReference<WaitNode> {
        volatile Thread mWaiter;
        volatile boolean mDenied;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile WaitNode mPrev;

        /**
         * @return true if timed out or interrupted
         */
        boolean park(AltLatch latch) {
            LockSupport.park(latch);
            return false;
        }

        /**
         * @return <0 if thread should park; 0 if acquired and node should also be removed; >0
         * if acquired and node should not be removed
         */
        int acquire(AltLatch latch) {
            if (latch.tryAcquireExclusiveAfterParking()) {
                // Acquired, so no need to reference the thread anymore.
                UNSAFE.putOrderedObject(this, WAITER_OFFSET, null);
                return 0;
            } else {
                return -1;
            }
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            Utils.appendMiniString(b, this);
            b.append(" {waiter=").append(mWaiter);
            b.append(", denied=").append(mDenied);
            b.append(", next="); Utils.appendMiniString(b, get());
            b.append(", prev="); Utils.appendMiniString(b, mPrev);
            return b.append('}').toString();
        }
    }

    static class Timed extends WaitNode {
        private long mNanosTimeout;
        private long mEndNanos;

        Timed(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean park(AltLatch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }

    static class Shared extends WaitNode {
        @Override
        final int acquire(AltLatch latch) {
            int trials = 0;
            while (true) {
                int state = latch.mLatchState;
                if (state < 0) {
                    return state;
                }

                if (UNSAFE.compareAndSwapInt(latch, STATE_OFFSET, state, state + 1)) {
                    // Acquired, so no need to reference the thread anymore.
                    Thread waiter = mWaiter;
                    if (waiter == null ||
                        !UNSAFE.compareAndSwapObject(this, WAITER_OFFSET, waiter, null))
                    {
                        // Handoff was actually fair, and now an extra shared latch must be
                        // released.
                        if (state < 1) {
                            throw new AssertionError(state);
                        }
                        if (!UNSAFE.compareAndSwapInt(latch, STATE_OFFSET, state + 1, state)) {
                            UNSAFE.getAndAddInt(latch, STATE_OFFSET, -1);
                        }
                        // Already removed from the queue.
                        return 1;
                    }

                    // Only remove node if this thread is the first shared latch owner. This
                    // guarantees that no other thread will be concurrently removing nodes.
                    // Nodes for other threads will have their nodes removed later, as latches
                    // are released. Early removal is a garbage collection optimization.
                    return state;
                }

                trials = spin(trials);
            }
        }
    }

    static class TimedShared extends Shared {
        private long mNanosTimeout;
        private long mEndNanos;

        TimedShared(long nanosTimeout) {
            mNanosTimeout = nanosTimeout;
            if (nanosTimeout >= 0) {
                mEndNanos = System.nanoTime() + nanosTimeout;
            }
        }

        @Override
        final boolean park(AltLatch latch) {
            if (mNanosTimeout < 0) {
                LockSupport.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                LockSupport.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }
}
