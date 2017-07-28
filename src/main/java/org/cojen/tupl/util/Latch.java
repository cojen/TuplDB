/*
 *  Copyright (C) 2011-2017 Cojen.org
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

import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.io.UnsafeAccess;
import org.cojen.tupl.io.Utils;

/**
 * Non-reentrant read-write latch, designed for throughout over fairness. Implementation
 * doesn't track thread ownership or check for illegal usage. As a result, it typically
 * outperforms ReentrantLock and built-in Java synchronization. Although latch acquisition is
 * typically unfair, waiting threads aren't starved indefinitely.
 *
 * @author Brian S O'Neill
 * @see LatchCondition
 */
@SuppressWarnings("restriction")
public class Latch {
    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors() > 1 ? 1 << 10 : 1;

    // TODO: Switch to VarHandle when available and utilize specialized operations. 

    static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();

    static final long STATE_OFFSET, FIRST_OFFSET, LAST_OFFSET;
    static final long WAITER_OFFSET;

    static {
        try {
            // Reduce the risk of "lost unpark" due to classloading.
            // https://bugs.openjdk.java.net/browse/JDK-8074773
            Class<?> clazz = LockSupport.class;

            clazz = Latch.class;
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

    public Latch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    public Latch(int initialState) {
        // Assume that this latch instance is published to other threads safely, and so a
        // volatile store isn't required.
        UNSAFE.putInt(this, STATE_OFFSET, initialState);
    }

    boolean isHeldExclusive() {
        return mLatchState == EXCLUSIVE;
    }

    /**
     * Try to acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public boolean tryAcquireExclusive() {
        return doTryAcquireExclusive();
    }

    private boolean doTryAcquireExclusive() {
        return mLatchState == 0 && UNSAFE.compareAndSwapInt(this, STATE_OFFSET, 0, EXCLUSIVE);
    }

    /**
     * Attempt to acquire the exclusive latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireExclusiveNanos(nanosTimeout);
    }

    private boolean doTryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        if (doTryAcquireExclusive()) {
            return true;
        }

        if (nanosTimeout == 0) {
            return false;
        }

        boolean result;
        try {
            result = acquire(new Timed(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                while (!doTryAcquireExclusive());
                return true;
            }
            return false;
        }

        return checkTimedResult(result, nanosTimeout);
    }

    /**
     * Acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public void acquireExclusive() {
        if (!doTryAcquireExclusive()) {
            doAcquireExclusive();
        }
    }

    /**
     * Caller should have already called tryAcquireExclusive.
     */
    private void doAcquireExclusive() {
        try {
            acquire(new WaitNode());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
            while (!doTryAcquireExclusive());
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public void acquireExclusiveInterruptibly() throws InterruptedException {
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
                        doReleaseShared();
                        return;
                    }

                    if (!first.mFair) {
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
            releaseShared();
        }
    }

    /**
     * Releases an exclusive or shared latch.
     */
    public final void releaseEither() {
        // TODO: can be non-volatile read
        if (mLatchState == EXCLUSIVE) {
            releaseExclusive();
        } else {
            releaseShared();
        }
    }

    /**
     * Try to acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public boolean tryAcquireShared() {
        return doTryAcquireShared();
    }

    private boolean doTryAcquireShared() {
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
    public boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
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

        boolean result;
        try {
            result = acquire(new TimedShared(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                while (!doTryAcquireShared());
                return true;
            }
            return false;
        }

        return checkTimedResult(result, nanosTimeout);
    }

    private static boolean checkTimedResult(boolean result, long nanosTimeout)
        throws InterruptedException
    {
        if (!result && (Thread.interrupted() || nanosTimeout < 0)) {
            InterruptedException e;
            try {
                e = new InterruptedException();
            } catch (Throwable e2) {
                // Possibly an OutOfMemoryError.
                if (nanosTimeout < 0) {
                    throw e2;
                }
                return false;
            }
            throw e;
        }

        return result;
    }

    /**
     * Like tryAcquireShared, except blocks if an exclusive latch is held.
     *
     * @return false if not acquired due to contention with other shared requests
     */
    public boolean acquireSharedUncontended() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int state = mLatchState;
            if (state >= 0) {
                return UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1);
            }
        }

        try {
            acquire(new Shared());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
            while (!doTryAcquireShared());
        }

        return true;
    }

    /**
     * Like tryAcquireSharedNanos, except blocks if an exclusive latch is held.
     *
     * @param nanosTimeout pass negative for infinite timeout
     * @return -1 if not acquired due to contention with other shared requests, 0 if timed out,
     * or 1 if acquired
     */
    public int acquireSharedUncontendedNanos(long nanosTimeout) throws InterruptedException {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int state = mLatchState;
            if (state >= 0) {
                return UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1) ? 1 : -1;
            }
        }

        boolean result;
        try {
            result = acquire(new TimedShared(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                while (!doTryAcquireShared());
                return 1;
            }
            return 0;
        }

        return checkTimedResult(result, nanosTimeout) ? 1 : 0;
    }

    /**
     * Acquire a shared latch, barging ahead of any waiting threads if possible.
     */
    public void acquireShared() {
        if (!tryAcquireSharedSpin()) {
            try {
                acquire(new Shared());
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
                while (!doTryAcquireShared());
            }
        }
    }

    private boolean tryAcquireSharedSpin() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, state, state + 1)) {
                    return true;
                }
                trials = spin(trials);
            }
        }
        return false;
    }

    /**
     * Acquire a shared latch, aborting if interrupted.
     */
    public void acquireSharedInterruptibly() throws InterruptedException {
        doTryAcquireSharedNanos(-1);
    }

    /**
     * Attempt to upgrade a held shared latch into an exclusive latch. Upgrade fails if shared
     * latch is held by more than one thread. If successful, caller must later call
     * releaseExclusive instead of releaseShared.
     */
    public boolean tryUpgrade() {
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
    public void releaseShared() {
        doReleaseShared();
    }

    private void doReleaseShared() {
        int trials = 0;
        while (true) {
            int state = mLatchState;

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
        }
    }

    private boolean acquire(final WaitNode node) {
        node.mWaiter = Thread.currentThread();
        WaitNode prev = enqueue(node);
        int acquireResult = node.tryAcquire(this);

        if (acquireResult < 0) {
            int denied = 0;
            while (true) {
                boolean parkAbort = node.park(this);

                acquireResult = node.tryAcquire(this);

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
                    node.mFair = true;
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

    private WaitNode enqueue(final WaitNode node) {
        WaitNode prev = (WaitNode) UNSAFE.getAndSetObject(this, LAST_OFFSET, node);

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

        return prev;
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
        appendMiniString(b, this);
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

    static void appendMiniString(StringBuilder b, Object obj) {
        if (obj == null) {
            b.append("null");
            return;
        }
        b.append(obj.getClass().getName()).append('@').append(Integer.toHexString(obj.hashCode()));
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
    @SuppressWarnings("serial")
    static class WaitNode extends AtomicReference<WaitNode> {
        volatile Thread mWaiter;
        volatile boolean mFair;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile WaitNode mPrev;

        /**
         * @return true if timed out or interrupted
         */
        boolean park(Latch latch) {
            LockSupport.park(latch);
            return false;
        }

        /**
         * @return <0 if thread should park; 0 if acquired and node should also be removed; >0
         * if acquired and node should not be removed
         */
        int tryAcquire(Latch latch) {
            int trials = 0;
            while (true) {
                for (int i=0; i<SPIN_LIMIT; i++) {
                    boolean acquired = latch.doTryAcquireExclusive();
                    Object waiter = mWaiter;
                    if (waiter == null) {
                        // Fair handoff, and so node is no longer in the queue.
                        return 1;
                    }
                    if (!acquired) {
                        continue;
                    }
                    // Acquired, so no need to reference the waiter anymore.
                    if (!mFair) {
                        UNSAFE.putOrderedObject(this, WAITER_OFFSET, null);
                    } else if (!UNSAFE.compareAndSwapObject(this, WAITER_OFFSET, waiter, null)) {
                        return 1;
                    }
                    return 0;
                }
                if (++trials >= SPIN_LIMIT >> 1) {
                    return -1;
                }
                // Yield to avoid parking.
                Thread.yield();
            }
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            appendMiniString(b, this);
            b.append(" {waiter=").append(mWaiter);
            b.append(", fair=").append(mFair);
            b.append(", next="); appendMiniString(b, get());
            b.append(", prev="); appendMiniString(b, mPrev);
            return b.append('}').toString();
        }
    }

    @SuppressWarnings("serial")
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
        final boolean park(Latch latch) {
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

    @SuppressWarnings("serial")
    static class Shared extends WaitNode {
        /**
         * @return <0 if thread should park; 0 if acquired and node should also be removed; >0
         * if acquired and node should not be removed
         */
        @Override
        final int tryAcquire(Latch latch) {
            // Note: If mWaiter is null, then handoff was fair. The shared count should already
            // be correct, and this node won't be in the queue anymore.

            WaitNode first = latch.mLatchFirst;
            if (first != null && !(first instanceof Shared)) {
                return mWaiter == null ? 1 : -1;
            }

            int trials = 0;
            while (true) {
                if (mWaiter == null) {
                    return 1;
                }

                int state = latch.mLatchState;
                if (state < 0) {
                    return state;
                }

                if (UNSAFE.compareAndSwapInt(latch, STATE_OFFSET, state, state + 1)) {
                    // Acquired, so no need to reference the thread anymore.
                    Object waiter = mWaiter;
                    if (waiter == null ||
                        !UNSAFE.compareAndSwapObject(this, WAITER_OFFSET, waiter, null))
                    {
                        if (!UNSAFE.compareAndSwapInt(latch, STATE_OFFSET, state + 1, state)) {
                            UNSAFE.getAndAddInt(latch, STATE_OFFSET, -1);
                        }
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

    @SuppressWarnings("serial")
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
        final boolean park(Latch latch) {
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
