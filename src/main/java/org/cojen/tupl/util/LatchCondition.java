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

/**
 * Manages a queue of waiting threads, associated with a {@link Latch} instance. Unlike the
 * built-in Java Condition class, spurious wakeup does not occur when waiting.
 *
 * @author Brian S O'Neill
 */
public class LatchCondition {
    private static final ThreadLocal<Node> cLocalNode = new ThreadLocal<>();

    Node mHead;
    Node mTail;

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
        try {
            return await(latch, localNode(Node.WAITING), nanosTimeout, nanosEnd);
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Latch must still be held.
            return -1;
        }
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
        try {
            return await(latch, localNode(Node.WAITING_SHARED), nanosTimeout, nanosEnd);
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError. Latch must still be held.
            return -1;
        }
    }

    private Node localNode(int waitState) {
        Node node = cLocalNode.get();
        if (node == null) {
            node = new Node(Thread.currentThread());
            cLocalNode.set(node);
        }
        node.mWaitState = waitState;
        return node;
    }

    private int await(Latch latch, Node node, long nanosTimeout, long nanosEnd) {
        enqueue(node);

        if (nanosTimeout < 0) {
            while (true) {
                latch.releaseExclusive();
                LockSupport.park(this);
                latch.acquireExclusive();
                int result = node.resumed(this);
                if (result != 0) {
                    return result;
                }
            }
        } else {
            while (true) {
                latch.releaseExclusive();
                LockSupport.parkNanos(this, nanosTimeout);
                latch.acquireExclusive();
                int result = node.resumed(this);
                if (result != 0) {
                    return result;
                }
                if (nanosTimeout == 0 || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                    node.remove(this);
                    return 0;
                }
            }
        }
    }

    private void enqueue(Node node) {
        Node tail = mTail;
        if (tail == null) {
            mHead = node;
        } else {
            tail.mNext = node;
            node.mPrev = tail;
        }
        mTail = node;
    }

    /**
     * Signals the first waiter, of any type. Caller must hold shared or exclusive latch.
     */
    public final void signal() {
        Node head = mHead;
        if (head != null) {
            head.signal();
        }
    }

    /**
     * If a first waiter exists, it's removed, the held exclusive latch is released, and then
     * the waiter is signaled.
     *
     * @return false if no waiter and latch wasn't released
     */
    public final boolean signalRelease(Latch latch) {
        Node head = mHead;
        if (head != null) {
            if (head == mTail) {
                // Don't permit the queue to go completely empty. The Lock class depends on the
                // queue having at least one waiter in it until the waiter acquires the latch
                // again. Without this, the Lock object can be removed from the LockManager too
                // soon and the Lock is orphaned. This behavior can change if direct latch
                // ownership transfer is supported.
                head.signal();
                latch.releaseExclusive();
            } else {
                head.remove(this);
                latch.releaseExclusive();
                LockSupport.unpark(head.mWaiter);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Signals the first waiter, of any type. Caller must hold shared or exclusive latch.
     *
     * @return false if no waiters of any type exist
     */
    public final boolean signalNext() {
        Node head = mHead;
        if (head == null) {
            return false;
        }
        head.signal();
        return true;
    }

    /**
     * Signals all waiters, of any type. Caller must hold shared or exclusive latch.
     */
    public final void signalAll() {
        Node node = mHead;
        while (node != null) {
            node.signal();
            node = node.mNext;
        }
    }

    /**
     * Signals the first waiter, but only if it's a shared waiter. Caller must hold shared or
     * exclusive latch.
     */
    public final void signalShared() {
        Node head = mHead;
        if (head != null && head.mWaitState == Node.WAITING_SHARED) {
            head.signal();
        }
    }

    /**
     * If a first shared waiter exists, it's removed, the held exclusive latch is released, and
     * then the waiter is signaled.
     *
     * @return false if no shared waiter and latch wasn't released
     */
    public final boolean signalSharedRelease(Latch latch) {
        Node head = mHead;
        if (head != null && head.mWaitState == Node.WAITING_SHARED) {
            if (head == mTail) {
                // See comments in signalRelease method.
                head.signal();
                latch.releaseExclusive();
            } else {
                head.remove(this);
                latch.releaseExclusive();
                LockSupport.unpark(head.mWaiter);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Signals the first waiter, but only if it's a shared waiter. Caller must hold shared or
     * exclusive latch.
     *
     * @return false if no waiters of any type exist
     */
    public final boolean signalNextShared() {
        Node head = mHead;
        if (head == null) {
            return false;
        }
        if (head.mWaitState == Node.WAITING_SHARED) {
            head.signal();
        }
        return true;
    }

    /**
     * Clears out all waiting threads and interrupts them. Caller must hold exclusive latch.
     */
    public final void clear() {
        Node node = mHead;
        while (node != null) {
            if (node.mWaitState >= Node.WAITING) {
                if (node.mWaiter instanceof Thread) {
                    ((Thread) node.mWaiter).interrupt();
                }
            }
            node.mPrev = null;
            Node next = node.mNext;
            node.mNext = null;
            node = next;
        }
        mHead = null;
        mTail = null;
    }

    static class Node {
        final Thread mWaiter;

        static final int REMOVED = 0, SIGNALED = 1, WAITING = 2, WAITING_SHARED = 3;
        int mWaitState;

        Node mPrev;
        Node mNext;

        Node(Thread waiter) {
            mWaiter = waiter;
        }

        /**
         * Called by thread which was parked after it resumes. Caller is
         * responsible for removing node from queue if return value is 0.
         *
         * @return -1 if interrupted, 0 if not signaled, 1 if signaled
         */
        final int resumed(LatchCondition queue) {
            if (mWaitState < WAITING) {
                if (mWaitState != REMOVED) {
                    remove(queue);
                }
                return 1;
            }

            if (mWaiter.isInterrupted()) {
                Thread.interrupted();
                remove(queue);
                return -1;
            }

            return 0;
        }

        final void signal() {
            mWaitState = SIGNALED;
            LockSupport.unpark(mWaiter);
        }

        final void remove(LatchCondition queue) {
            Node prev = mPrev;
            Node next = mNext;
            if (prev == null) {
                if ((queue.mHead = next) == null) {
                    queue.mTail = null;
                } else {
                    next.mPrev = null;
                }
            } else {
                if ((prev.mNext = next) == null) {
                    queue.mTail = prev;
                } else {
                    next.mPrev = prev;
                }
                mPrev = null;
            }
            mNext = null;

            mWaitState = REMOVED;
        }
    }
}
