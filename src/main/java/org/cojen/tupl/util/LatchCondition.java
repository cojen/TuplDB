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

import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.LockSupport;

import static org.cojen.tupl.util.Latch.*;

/**
 * Manages a queue of waiting threads, associated with a {@link Latch} instance. Unlike the
 * built-in Java Condition class, spurious wakeup does not occur when waiting.
 *
 * @author Brian S O'Neill
 */
public class LatchCondition {
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
     * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0} timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    public final int await(Latch latch, long nanosTimeout, long nanosEnd) {
        return await(latch, WaitNode.COND_WAIT, nanosTimeout, nanosEnd);
    }

    /**
     * Blocks the current thread until a signal is received. Exclusive latch must be acquired
     * by the caller, which is released and then re-acquired by this method.
     *
     * <p>A shared waiter intends to access a resource with shared access, and it can be
     * signaled specially. After waiting, the caller is responsible for signaling the next
     * shared waiter.
     *
     * @param latch latch being used by this condition
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0} timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    public final int awaitShared(Latch latch, long nanosTimeout, long nanosEnd) {
        return await(latch, WaitNode.COND_WAIT_SHARED, nanosTimeout, nanosEnd);
    }

    private int await(Latch latch, int waitState, long nanosTimeout, long nanosEnd) {
        final WaitNode node;
        try {
            node = new WaitNode(waitState);
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
     * Signals the first waiter, but only if it's a shared waiter. Caller must hold exclusive
     * latch.
     */
    public final void signalShared(Latch latch) {
        WaitNode head = mHead;
        if (head != null && ((int) cWaitStateHandle.get(head)) == WaitNode.COND_WAIT_SHARED) {
            head.condSignal(latch, this);
        }
    }

    /**
     * Clears out all waiting threads and interrupts them. Caller must hold exclusive latch.
     */
    public final void clear() {
        WaitNode node = mHead;
        while (node != null) {
            if (((int) cWaitStateHandle.get(node)) >= WaitNode.COND_WAIT) {
                ((Thread) cWaiterHandle.get(node)).interrupt();
            }
            cPrevHandle.set(node, null);
            WaitNode next = (WaitNode) cNextHandle.get(node);
            cNextHandle.set(node, null);
            node = next;
        }
        mHead = null;
        mTail = null;
    }
}
