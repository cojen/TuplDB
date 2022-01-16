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

import java.util.Objects;

import java.util.concurrent.TimeUnit;

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
        final WaitNode node;
        try {
            node = new WaitNode(Thread.currentThread());
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Latch must still be held.
            return -1;
        }

        enqueue(node);

        return node.condAwait(latch, this, nanosTimeout, nanosEnd);
    }

    /**
     * Blocks the current thread until a signal is received. Exclusive latch must be acquired
     * by the caller, which is released and then re-acquired by this method. If null is
     * returned, the thread's interrupt status must be checked to determine if the wait was
     * interupted or if it timed out.
     *
     * @param latch latch being used by this condition
     * @param tag non-null tag to register
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @return null if interrupted or timed out, or else the tag or replacement object
     * @see signalTagged
     */
    public final Object awaitTagged(Latch latch, Object tag, long nanosTimeout) {
        long nanosEnd = nanosTimeout <= 0 ? 0 : (System.nanoTime() + nanosTimeout);
        return awaitTagged(latch, tag, nanosTimeout, nanosEnd);
    }

    /**
     * Blocks the current thread until a signal is received. Exclusive latch must be acquired
     * by the caller, which is released and then re-acquired by this method. If null is
     * returned, the thread's interrupt status must be checked to determine if the wait was
     * interupted or if it timed out.
     *
     * @param latch latch being used by this condition
     * @param tag non-null tag to register
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0} timeout
     * @return null if interrupted or timed out, or else the tag or replacement object
     * @see signalTagged
     */
    public final Object awaitTagged(Latch latch, Object tag, long nanosTimeout, long nanosEnd) {
        Objects.requireNonNull(tag);

        final Tagged node;
        try {
            node = new Tagged(Thread.currentThread(), tag);
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Latch must still be held.
            return -1;
        }

        enqueue(node);

        int result = node.condAwait(latch, this, nanosTimeout, nanosEnd);

        if (result <= 0) {
            if (result < 0) {
                Thread.currentThread().interrupt();
            }
            return null;
        }

        return node.mTag;
    }

    private void enqueue(WaitNode node) {
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
     * Blocks the current thread until a signal is received. This method behaves like regular
     * {@code await} method except the thread is signaled ahead of all the other waiting
     * threads. Exclusive latch must be acquired by the caller, which is released and then
     * re-acquired by this method.
     *
     * @param latch latch being used by this condition
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0} timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    public final int priorityAwait(Latch latch, long nanosTimeout, long nanosEnd) {
        final WaitNode node;
        try {
            node = new WaitNode(Thread.currentThread());
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
     * Invokes the given continuation upon the condition being signaled. The exclusive latch
     * must be acquired by the caller, which is retained. When the condition is signaled, the
     * continuation is enqueued to be run by a thread which releases the exclusive latch. The
     * releasing thread actually retains the latch and runs the continuation, effectively
     * transferring latch ownership. The continuation must not explicitly release the latch,
     * and any exception thrown by the continuation is passed to the uncaught exception handler
     * of the running thread.
     *
     * @param cont called with latch held
     */
    public final void uponSignal(Runnable cont) {
        enqueue(new WaitNode(cont));
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
     * Signals the first waiter, but only if it's a tagged waiter. Caller must hold exclusive
     * latch.
     *
     * @param latch must be exclusively held
     */
    public final void signalTagged(Latch latch) {
        WaitNode head = mHead;
        if (head instanceof Tagged) {
            head.condSignal(latch, this);
        }
    }

    /**
     * Signals the first waiter which matches the given tag. A thread which is blocked on
     * {@link #awaitTagged awaitTagged} receives the original tag or a replacement, if one is
     * provided. Because the exclusive latch is held the whole time, the caller can choose to
     * update the state of the original tag before the signaled thread wakes up.
     *
     * @param latch must be exclusively held
     * @param tag non-null tag to match on, based on object equality
     * @param replacement if non-null, replace the tag with this object
     * @return the original tag that matched, or else null if nothing was found
     */
    public final Object signalTagged(Latch latch, Object tag, Object replacement) {
        for (WaitNode node = mHead; node != null; node = (WaitNode) cNextHandle.get(node)) {
            Object original;
            if (node instanceof Tagged tagged && (original = tagged.mTag).equals(tag)) {
                if (replacement != null) {
                    tagged.mTag = replacement;
                }
                node.condSignal(latch, this);
                return original;
            }
        }
        return null;
    }

    /**
     * Clears out all waiters and interrupts those that are threads. Caller must hold exclusive
     * latch.
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
