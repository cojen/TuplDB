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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.io.Utils;

/**
 * Non-reentrant read-write latch, designed for throughput over fairness. Implementation
 * doesn't track thread ownership or check for illegal usage. As a result, it typically
 * outperforms ReentrantLock and built-in Java synchronization. Although latch acquisition is
 * typically unfair, waiting threads aren't starved indefinitely.
 *
 * @author Brian S O'Neill
 * @see Latch.Condition
 */
public class Latch {
    public static final int UNLATCHED = 0, EXCLUSIVE = 0x80000000, SHARED = 1;

    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors() > 1 ? 1 << 10 : 1;

    static final VarHandle cStateHandle, cFirstHandle, cLastHandle,
        cWaiterHandle, cWaitStateHandle, cPrevHandle, cNextHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cStateHandle = lookup.findVarHandle(Latch.class, "mLatchState", int.class);
            cFirstHandle = lookup.findVarHandle(Latch.class, "mLatchFirst", WaitNode.class);
            cLastHandle = lookup.findVarHandle(Latch.class, "mLatchLast", WaitNode.class);
            cWaiterHandle = lookup.findVarHandle(WaitNode.class, "mWaiter", Object.class);
            cWaitStateHandle = lookup.findVarHandle(WaitNode.class, "mWaitState", int.class);
            cPrevHandle = lookup.findVarHandle(WaitNode.class, "mPrev", WaitNode.class);
            cNextHandle = lookup.findVarHandle(WaitNode.class, "mNext", WaitNode.class);
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
    private volatile WaitNode mLatchFirst;
    private volatile WaitNode mLatchLast;

    public Latch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    public Latch(int initialState) {
        // Assume that this latch instance is published to other threads safely, and so a
        // volatile store isn't required.
        cStateHandle.set(this, initialState);
    }

    /**
     * Try to acquire the exclusive latch, barging ahead of any waiting threads if possible.
     */
    public boolean tryAcquireExclusive() {
        return doTryAcquireExclusive();
    }

    private boolean doTryAcquireExclusive() {
        return mLatchState == 0 && cStateHandle.compareAndSet(this, 0, EXCLUSIVE);
    }

    private void doAcquireExclusiveSpin() {
        while (!doTryAcquireExclusive()) {
            Thread.onSpinWait();
        }
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
                doAcquireExclusiveSpin();
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
            doAcquireExclusiveSpin();
        }
    }

    /**
     * Acquire the exclusive latch, aborting if interrupted.
     */
    public void acquireExclusiveInterruptibly() throws InterruptedException {
        doTryAcquireExclusiveNanos(-1);
    }

    /**
     * Invokes the given continuation upon the latch being acquired exclusively. When acquired,
     * the continuation is run by the current thread, or it's enqueued to be run by a thread
     * which releases the latch. The releasing thread actually retains the latch and runs the
     * continuation, effectively transferring latch ownership. The continuation must not
     * explicitly release the latch, although it can downgrade the latch. Any exception thrown
     * by the continuation is passed to the uncaught exception handler of the running thread,
     * and then the latch is released.
     *
     * @param cont called with latch held
     */
    public void uponExclusive(Runnable cont) {
        if (!doTryAcquireExclusive()) enqueue: {
            WaitNode node;
            try {
                node = new WaitNode(cont, WaitNode.SIGNALED);
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
                doAcquireExclusiveSpin();
                break enqueue;
            }

            WaitNode prev = enqueue(node);

            boolean acquired = doTryAcquireExclusive();

            if (node.mWaiter == null) {
                // Continuation already ran or is running right now.
                if (acquired) {
                    releaseExclusive();
                }
                return;
            }

            if (!acquired) {
                return;
            }

            node.mWaiter = null;

            // Acquired while still in the queue. Remove the node now, releasing memory.
            if (mLatchFirst != node) {
                remove(node, prev);
            } else {
                removeFirst(node);
            }
        }

        try {
            cont.run();
        } catch (Throwable e) {
            Utils.uncaught(e);
        }

        releaseExclusive();
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
                Object waiter = node.mWaiter;
                if (waiter != null) {
                    if (node instanceof Shared) {
                        cStateHandle.getAndAdd(this, 1);
                        if (cWaiterHandle.compareAndSet(node, waiter, null)) {
                            Parker.unpark((Thread) waiter);
                        } else {
                            // Already unparked, so fix the share count.
                            cStateHandle.getAndAdd(this, -1);
                        }
                    } else {
                        if (node != first) {
                            // Advance the queue past any shared waiters that were encountered.
                            mLatchFirst = node;
                        }
                        return;
                    }
                }

                WaitNode next = node.mNext;

                if (next == null) {
                    // Queue is now empty, unless an enqueue is in progress.
                    if (cLastHandle.compareAndSet(this, node, null)) {
                        cFirstHandle.compareAndSet(this, first, null);
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
                if (last == null || !cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                    return;
                }
            }

            // Although the last waiter has been observed to exist, the first waiter field
            // might not be set yet.
            WaitNode first = mLatchFirst;

            unpark: if (first != null) {
                Object waiter = first.mWaiter;

                if (waiter != null) {
                    if (first instanceof Shared) {
                        downgrade();
                        if (doReleaseShared()) {
                            return;
                        }
                        trials = 0;
                        continue;
                    }

                    if (first.mWaitState != WaitNode.SIGNALED) {
                        // Unpark the waiter, but allow another thread to barge in.
                        mLatchState = 0;
                        Parker.unpark((Thread) waiter);
                        return;
                    }
                }

                // Remove first from the queue.
                {
                    WaitNode next = first.mNext;
                    if (next != null) {
                        mLatchFirst = next;
                    } else {
                        // Queue is now empty, unless an enqueue is in progress.
                        if (last != first || !cLastHandle.compareAndSet(this, last, null)) {
                            break unpark;
                        }
                        cFirstHandle.compareAndSet(this, last, null);
                    }
                }

                if (waiter != null && cWaiterHandle.compareAndSet(first, waiter, null)) {
                    // Fair handoff to waiting thread or continuation.
                    if (waiter instanceof Thread t) {
                        Parker.unpark(t);
                        return;
                    }
                    try {
                        ((Runnable) waiter).run();
                    } catch (Throwable e) {
                        Utils.uncaught(e);
                    }
                    if (mLatchState != EXCLUSIVE) {
                        if (mLatchState <= 0) {
                            throw new IllegalStateException
                                ("Illegal latch state: " + mLatchState + ", caused by " + waiter);
                        }
                        if (doReleaseShared()) {
                            return;
                        }
                    }
                    trials = 0;
                    continue;
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
        if (((int) cStateHandle.get(this)) == EXCLUSIVE) {
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
        return state >= 0 && cStateHandle.compareAndSet(this, state, state + 1);
    }

    private void doAcquireSharedSpin() {
        while (!doTryAcquireShared()) {
            Thread.onSpinWait();
        }
    }

    /**
     * Attempt to acquire a shared latch, aborting if interrupted.
     *
     * @param nanosTimeout pass negative for infinite timeout
     */
    public boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        return doTryAcquireSharedNanos(nanosTimeout);
    }

    private boolean doTryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (cStateHandle.compareAndSet(this, state, state + 1)) {
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
                doAcquireSharedSpin();
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
                return cStateHandle.compareAndSet(this, state, state + 1);
            }
        }

        try {
            acquire(new Shared());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Caller isn't expecting an exception, so spin.
            doAcquireSharedSpin();
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
                return cStateHandle.compareAndSet(this, state, state + 1) ? 1 : -1;
            }
        }

        boolean result;
        try {
            result = acquire(new TimedShared(nanosTimeout));
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            if (nanosTimeout < 0) {
                // Caller isn't expecting an exception, so spin.
                doAcquireSharedSpin();
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
                doAcquireSharedSpin();
            }
        }
    }

    private boolean tryAcquireSharedSpin() {
        WaitNode first = mLatchFirst;
        if (first == null || first instanceof Shared) {
            int trials = 0;
            int state;
            while ((state = mLatchState) >= 0) {
                if (cStateHandle.compareAndSet(this, state, state + 1)) {
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
            if (mLatchState != 1) {
                return false;
            }
            if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE)) {
                return true;
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Release a held shared latch.
     */
    public void releaseShared() {
        int trials = 0;
        while (true) {
            int state = mLatchState;

            WaitNode last = mLatchLast;
            if (last == null) {
                // No waiters, so release the latch.
                if (cStateHandle.compareAndSet(this, state, --state)) {
                    if (state == 0) {
                        // Need to check if any waiters again, due to race with enqueue. If
                        // cannot immediately re-acquire the latch, then let the new owner
                        // (which barged in) unpark the waiters when it releases the latch.
                        last = mLatchLast;
                        if (last != null && cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                            releaseExclusive();
                        }
                    }
                    return;
                }
            } else if (state == 1) {
                // Try to switch to exclusive, and then let releaseExclusive deal with
                // unparking the waiters.
                if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE) || doTryUpgrade()) {
                    releaseExclusive();
                    return;
                }
            } else if (cStateHandle.compareAndSet(this, state, --state)) {
                return;
            }

            trials = spin(trials);
        }
    }

    /**
     * @return false if latch is held exclusive now
     */
    private boolean doReleaseShared() {
        // Note: Same as regular releaseShared, except doesn't recurse into the
        // releaseExclusive method.

        int trials = 0;
        while (true) {
            int state = mLatchState;

            WaitNode last = mLatchLast;
            if (last == null) {
                if (cStateHandle.compareAndSet(this, state, --state)) {
                    if (state == 0) {
                        last = mLatchLast;
                        if (last != null && cStateHandle.compareAndSet(this, 0, EXCLUSIVE)) {
                            return false;
                        }
                    }
                    return true;
                }
            } else if (state == 1) {
                if (cStateHandle.compareAndSet(this, 1, EXCLUSIVE) || doTryUpgrade()) {
                    return false;
                }
            } else if (cStateHandle.compareAndSet(this, state, --state)) {
                return true;
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
                    if (!cWaiterHandle.compareAndSet(node, Thread.currentThread(), null)) {
                        // Fair handoff just occurred.
                        return true;
                    }

                    int state = mLatchState;
                    if (state >= 0) {
                        // Unpark any waiters that queued behind this request.
                        WaitNode wnode = node;
                        while ((wnode = wnode.mNext) != null) {
                            Object waiter = wnode.mWaiter;
                            if (waiter instanceof Thread t) {
                                if (wnode instanceof Shared) {
                                    Parker.unpark(t);
                                } else {
                                    if (state == 0) {
                                        Parker.unpark(t);
                                    }
                                    // No need to iterate past an exclusive waiter.
                                    break;
                                }
                            }
                        }
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
                    node.mWaitState = WaitNode.SIGNALED;
                }
            }
        }

        if (acquireResult == 0) {
            // Remove the node now, releasing memory.
            if (mLatchFirst != node) {
                remove(node, prev);
            } else {
                removeFirst(node);
            }
        }

        return true;
    }

    private void removeFirst(WaitNode node) {
        // Removing the first node requires special attention. Because the latch is now held by
        // the current thread, no other dequeues are in progress, but enqueues still are.

        while (true) {
            WaitNode next = node.mNext;
            if (next != null) {
                mLatchFirst = next;
                return;
            } else {
                // Queue is now empty, unless an enqueue is in progress.
                WaitNode last = mLatchLast;
                if (last == node && cLastHandle.compareAndSet(this, last, null)) {
                    cFirstHandle.compareAndSet(this, last, null);
                    return;
                }
            }
        }
    }

    private WaitNode enqueue(final WaitNode node) {
        var prev = (WaitNode) cLastHandle.getAndSet(this, node);

        if (prev == null) {
            mLatchFirst = node;
        } else {
            prev.mNext = node;
            WaitNode pp = prev.mPrev;
            if (pp != null) {
                // The old last node was intended to be removed, but the last node cannot
                // be removed unless it's also the first. Bypass it now that a new last
                // node has been enqueued.
                cNextHandle.setRelease(pp, node);
                // Return a more correct previous node, although it might be stale. Node
                // removal is somewhat lazy, and accurate removal is performed when the
                // exclusive latch is released.
                prev = pp;
            }
        }

        return prev;
    }

    /**
     * Should only be called after clearing the mWaiter field. Ideally, multiple threads
     * shouldn't be calling this method, because it can cause nodes to be resurrected and
     * remain in the queue longer than necessary. They'll get cleaned out eventually. The
     * problem is caused by the prev node reference, which might have changed or have been
     * removed by the time this method is called.
     *
     * @param node node to remove, not null
     * @param prev previous node, not null
     */
    private void remove(final WaitNode node, final WaitNode prev) {
        WaitNode next = node.mNext;

        if (next == null) {
            // Removing the last node creates race conditions with enqueues. Instead, stash a
            // reference to the previous node and let the enqueue deal with it after a new node
            // has been enqueued.
            node.mPrev = prev;
            next = node.mNext;
            // Double check in case an enqueue just occurred that may have failed to notice the
            // previous node assignment.
            if (next == null) {
                return;
            }
        }

        while (next.mWaiter == null) {
            // Skip more nodes if possible.
            WaitNode nextNext = next.mNext;
            if (nextNext == null) {
                break;
            }
            next = nextNext;
        }

        // Bypass the removed node, allowing it to be released.
        cNextHandle.setRelease(prev, next);
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

    /**
     * Returns the first waiter in the queue that's actually still waiting.
     */
    private WaitNode firstWaiter() {
        WaitNode first = mLatchFirst;
        WaitNode next;
        if (first == null || first.mWaiter != null || (next = first.mNext) == null) {
            return first;
        }
        if (next.mWaiter != null) {
            return next;
        }
        // Clean out some stale nodes. Note that removing the first node isn't safe.
        remove(next, first);
        return null;
    }

    public final boolean hasQueuedThreads() {
        return mLatchLast != null;
    }

    @Override
    public String toString() {
        var b = new StringBuilder();
        appendMiniString(b, this);
        b.append('{').append("state=");

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
        } else {
            Thread.onSpinWait();
        }
        return trials;
    }

    /**
     * Manages a queue of waiting threads, associated with a {@link Latch} instance. Unlike the
     * built-in Java Condition class, spurious wakeup does not occur when waiting.
     */
    public static class Condition {
        WaitNode mHead;
        WaitNode mTail;

        /**
         * Returns true if no waiters are enqueued. Caller must hold shared or exclusive latch.
         */
        public final boolean isEmpty() {
            return mHead == null;
        }

        /**
         * Blocks the current thread indefinitely until a signal is received. Exclusive latch must
         * be acquired by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @return -1 if interrupted, or 1 if signaled
         */
        public final int await(Latch latch) {
            return await(latch, -1, 0);
        }

        /**
         * Blocks the current thread until a signal is received. Exclusive latch must be acquired
         * by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param timeout relative time to wait; infinite if {@literal <0}
         * @param unit timeout unit
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         */
        public final int await(Latch latch, long timeout, TimeUnit unit) {
            long nanosTimeout, nanosEnd;
            if (timeout <= 0) {
                nanosTimeout = timeout;
                nanosEnd = 0;
            } else {
                nanosTimeout = unit.toNanos(timeout);
                nanosEnd = System.nanoTime() + nanosTimeout;
            }
            return await(latch, nanosTimeout, nanosEnd);
        }

        /**
         * Blocks the current thread until a signal is received. Exclusive latch must be acquired
         * by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         */
        public final int await(Latch latch, long nanosTimeout) {
            long nanosEnd = nanosTimeout <= 0 ? 0 : (System.nanoTime() + nanosTimeout);
            return await(latch, nanosTimeout, nanosEnd);
        }

        /**
         * Blocks the current thread until a signal is received. Exclusive latch must be acquired
         * by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0}
         * timeout
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         */
        public final int await(Latch latch, long nanosTimeout, long nanosEnd) {
            return await(latch, WaitNode.COND_WAIT, nanosTimeout, nanosEnd);
        }

        /**
         * Blocks the current thread until a signal is received. Exclusive latch must be acquired
         * by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         * @see #signalTagged
         */
        public final int awaitTagged(Latch latch, long nanosTimeout) {
            long nanosEnd = nanosTimeout <= 0 ? 0 : (System.nanoTime() + nanosTimeout);
            return awaitTagged(latch, nanosTimeout, nanosEnd);
        }

        /**
         * Blocks the current thread until a signal is received. Exclusive latch must be acquired
         * by the caller, which is released and then re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0}
         * timeout
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         * @see #signalTagged
         */
        public final int awaitTagged(Latch latch, long nanosTimeout, long nanosEnd) {
            return await(latch, WaitNode.COND_WAIT_TAGGED, nanosTimeout, nanosEnd);
        }

        private int await(Latch latch, int waitState, long nanosTimeout, long nanosEnd) {
            final WaitNode node;
            try {
                node = new WaitNode(Thread.currentThread(), waitState);
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Latch must still be held.
                return -1;
            }

            WaitNode tail = mTail;
            if (tail == null) {
                mHead = node;
            } else {
                cNextHandle.set(tail, node);
                cPrevHandle.set(node, tail);
            }
            mTail = node;

            return node.condAwait(latch, this, nanosTimeout, nanosEnd);
        }

        /**
         * Blocks the current thread until a signal is received. This method behaves like regular
         * {@code await} method except the thread is signaled ahead of all the other waiting
         * threads. Exclusive latch must be acquired by the caller, which is released and then
         * re-acquired by this method.
         *
         * @param latch latch being used by this condition
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0}
         * timeout
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         */
        public final int priorityAwait(Latch latch, long nanosTimeout, long nanosEnd) {
            return priorityAwait(latch, WaitNode.COND_WAIT, nanosTimeout, nanosEnd);
        }

        private int priorityAwait(Latch latch, int waitState, long nanosTimeout, long nanosEnd) {
            final WaitNode node;
            try {
                node = new WaitNode(Thread.currentThread(), waitState);
            } catch (Throwable e) {
                // Possibly an OutOfMemoryError. Latch must still be held.
                return -1;
            }

            WaitNode head = mHead;
            if (head == null) {
                mTail = node;
            } else {
                cPrevHandle.set(head, node);
                cNextHandle.set(node, head);
            }
            mHead = node;

            return node.condAwait(latch, this, nanosTimeout, nanosEnd);
        }

        /**
         * Invokes the given continuation upon the condition being signaled. The exclusive
         * latch must be acquired by the caller, which is retained. When the condition is
         * signaled, the continuation is enqueued to be run by a thread which releases the
         * exclusive latch. The releasing thread actually retains the latch and runs the
         * continuation, effectively transferring latch ownership. The continuation must not
         * explicitly release the latch, and any exception thrown by the continuation is passed
         * to the uncaught exception handler of the running thread.
         *
         * @param cont called with latch held
         */
        public final void uponSignal(Runnable cont) {
            upon(cont, WaitNode.COND_WAIT);
        }

        /**
         * Invokes the given continuation upon the condition being signaled. The exclusive
         * latch must be acquired by the caller, which is retained. When the condition is
         * signaled, the continuation is enqueued to be run by a thread which releases the
         * exclusive latch. The releasing thread actually retains the latch and runs the
         * continuation, effectively transferring latch ownership. The continuation must not
         * explicitly release the latch, and any exception thrown by the continuation is passed
         * to the uncaught exception handler of the running thread.
         *
         * @param cont called with latch held
         * @see #signalTagged
         */
        public final void uponSignalTagged(Runnable cont) {
            upon(cont, WaitNode.COND_WAIT_TAGGED);
        }

        private void upon(Runnable cont, int waitState) {
            final var node = new WaitNode(cont, waitState);

            WaitNode tail = mTail;
            if (tail == null) {
                mHead = node;
            } else {
                cNextHandle.set(tail, node);
                cPrevHandle.set(node, tail);
            }
            mTail = node;
        }

        /**
         * Signals the first waiter, of any type. Caller must hold exclusive latch.
         */
        public final void signal(Latch latch) {
            WaitNode head = mHead;
            if (head != null) {
                head.condSignal(latch, this);
            }
        }

        /**
         * Signals all waiters, of any type. Caller must hold exclusive latch.
         */
        public final void signalAll(Latch latch) {
            while (true) {
                WaitNode head = mHead;
                if (head == null) {
                    return;
                }
                head.condSignal(latch, this);
            }
        }

        /**
         * Signals the first waiter, but only if it's a tagged waiter. Caller must hold
         * exclusive latch.
         */
        public final void signalTagged(Latch latch) {
            WaitNode head = mHead;
            if (head != null && ((int) cWaitStateHandle.get(head)) == WaitNode.COND_WAIT_TAGGED) {
                head.condSignal(latch, this);
            }
        }

        /**
         * Clears out all waiters and interrupts those that are threads. Caller must hold
         * exclusive latch.
         */
        public final void clear() {
            WaitNode node = mHead;
            while (node != null) {
                Object waiter = cWaiterHandle.get(node);
                if (waiter instanceof Thread t) {
                    t.interrupt();
                }
                cPrevHandle.set(node, null);
                var next = (WaitNode) cNextHandle.get(node);
                cNextHandle.set(node, null);
                node = next;
            }
            mHead = null;
            mTail = null;
        }
    }

    static class WaitNode {
        volatile Object mWaiter;

        /*
          The COND states are only used when the node is associated with a Condition,
          and when such a node moves into the Latch, the state is always SIGNALED.
          
          0 -> SIGNALED
          COND_WAIT -> SIGNALED
          COND_WAIT_TAGGED -> SIGNALED
         */
        static final int SIGNALED = 1, COND_WAIT = 2, COND_WAIT_TAGGED = 3;
        volatile int mWaitState;

        // Only set if node was deleted and must be bypassed when a new node is enqueued.
        volatile WaitNode mPrev;
        volatile WaitNode mNext;

        /**
         * Constructor for latch wait.
         */
        WaitNode() {
        }

        /**
         * Constructor for condition wait. Caller must hold exclusive latch.
         */
        WaitNode(Object waiter, int waitState) {
            cWaiterHandle.set(this, waiter);
            cWaitStateHandle.set(this, waitState);
        }

        /**
         * @return true if timed out or interrupted
         */
        boolean park(Latch latch) {
            Parker.park(latch);
            // Never report interrupted status, so clear it. Only the timed nodes handle thread
            // interruption.
            Thread.interrupted();
            return false;
        }

        /**
         * @return {@literal <0 if thread should park; 0 if acquired and node should also be
         * removed; >0 if acquired and node should not be removed}
         */
        int tryAcquire(Latch latch) {
            for (int i=0; i<SPIN_LIMIT; i++) {
                boolean acquired = latch.doTryAcquireExclusive();
                Object waiter = mWaiter;
                if (waiter == null) {
                    // Fair handoff, and so node is no longer in the queue.
                    return 1;
                }
                if (acquired) {
                    // Acquired, so no need to reference the waiter anymore.
                    if (((int) cWaitStateHandle.get(this)) != SIGNALED) {
                        mWaiter = null;
                    } else if (!cWaiterHandle.compareAndSet(this, waiter, null)) {
                        return 1;
                    }
                    return 0;
                }
                Thread.onSpinWait();
            }
            return -1;
        }

        /**
         * Used for latch condition. Caller must hold exclusive latch.
         */
        final void condSignal(Latch latch, Condition queue) {
            condRemove(queue);
            cWaitStateHandle.set(this, SIGNALED);
            latch.enqueue(this);
        }

        /**
         * Called by thread which is holding exclusive latch, and has enqueued this node into
         * the condition queue. When method returns, exclusive latch is held again.
         *
         * @return -1 if interrupted, 0 if timed out, 1 if signaled
         */
        final int condAwait(Latch latch, Condition queue, long nanosTimeout, long nanosEnd) {
            latch.releaseExclusive();

            while (true) {
                if (nanosTimeout < 0) {
                    Parker.park(queue);
                } else {
                    Parker.parkNanos(queue, nanosTimeout);
                }

                boolean acquired;
                for (int i = SPIN_LIMIT;;) {
                    acquired = latch.doTryAcquireExclusive();
                    if (mWaiter == null) {
                        // Signaled, and so the node is no longer in the queue.
                        return 1;
                    }
                    if (acquired) {
                        if (((int) cWaitStateHandle.get(this)) == SIGNALED) {
                            mWaiter = null;
                            return 1;
                        }
                        break;
                    }
                    if (--i <= 0) {
                        break;
                    }
                    Thread.onSpinWait();
                }

                // Check if interrupted, or spurious unpark, or timed out.

                int result;
                if (Thread.interrupted()) {
                    result = -1;
                } else {
                    if (nanosTimeout < 0 ||
                        (nanosTimeout != 0 && (nanosTimeout = nanosEnd - System.nanoTime()) > 0))
                    {
                        // Spurious unpark, so start over.
                        if (acquired) {
                            latch.releaseExclusive();
                        }
                        continue;
                    }
                    // Timed out.
                    result = 0;
                }

                // If interrupted or timed out, the latch is still required to remove the
                // waiter from the queue, or to even return from this method.
                if (!acquired) doAcquire: {
                    Object waiter = mWaiter;
                    if (waiter != null && cWaiterHandle.compareAndSet(this, waiter, null)) {
                        latch.acquireExclusive();
                        if (((int) cWaitStateHandle.get(this)) != SIGNALED) {
                            // Break out to remove from queue and return -1 or 0.
                            break doAcquire;
                        }
                    }
                    // Signaled, and so the node is no longer in the queue.
                    if (result < 0) {
                        Thread.currentThread().interrupt(); // restore the status
                    }
                    return 1;
                }

                condRemove(queue);
                return result;
            }
        }

        /**
         * Used for latch condition. Caller must hold exclusive latch.
         */
        private void condRemove(Condition queue) {
            var prev = (WaitNode) cPrevHandle.get(this);
            var next = (WaitNode) cNextHandle.get(this);
            if (prev == null) {
                if ((queue.mHead = next) == null) {
                    queue.mTail = null;
                } else {
                    cPrevHandle.set(next, null);
                }
            } else {
                cNextHandle.set(prev, next);
                if (next == null) {
                    queue.mTail = prev;
                } else {
                    cPrevHandle.set(next, prev);
                }
                cPrevHandle.set(this, null);
            }
            cNextHandle.set(this, null);
        }

        @Override
        public String toString() {
            var b = new StringBuilder();
            appendMiniString(b, this);
            b.append('{').append("waiter=").append(mWaiter);
            b.append(", state=").append(mWaitState);
            b.append(", next="); appendMiniString(b, mNext);
            b.append(", prev="); appendMiniString(b, mPrev);
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
        final boolean park(Latch latch) {
            if (mNanosTimeout < 0) {
                Parker.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                Parker.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }

    static class Shared extends WaitNode {
        /**
         * @return {@literal <0 if thread should park; 0 if acquired and node should also be
         * removed; >0 if acquired and node should not be removed}
         */
        @Override
        final int tryAcquire(Latch latch) {
            // Note: If mWaiter is null, then handoff was fair. The shared count should already
            // be correct, and this node won't be in the queue anymore.

            WaitNode first = latch.firstWaiter();
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

                if (cStateHandle.compareAndSet(latch, state, state + 1)) {
                    // Acquired, so no need to reference the thread anymore.
                    Object waiter = mWaiter;
                    if (waiter == null || !cWaiterHandle.compareAndSet(this, waiter, null)) {
                        if (!cStateHandle.compareAndSet(latch, state + 1, state)) {
                            cStateHandle.getAndAdd(latch, -1);
                        }
                        return 1;
                    }

                    // Only instruct the caller to remove this node if this is the first shared
                    // latch owner (the returned state value will be 0). This guarantees that
                    // no other thread will be concurrently calling removeFirst. The node will
                    // be removed after an exclusive latch is released, or when firstWaiter is
                    // called again. Note that it's possible to return 0 every time, but only
                    // if the caller is also instructed to never call removeFirst.
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
        final boolean park(Latch latch) {
            if (mNanosTimeout < 0) {
                Parker.park(latch);
                return Thread.currentThread().isInterrupted();
            } else {
                Parker.parkNanos(latch, mNanosTimeout);
                if (Thread.currentThread().isInterrupted()) {
                    return true;
                }
                return (mNanosTimeout = mEndNanos - System.nanoTime()) <= 0;
            }
        }
    }
}
