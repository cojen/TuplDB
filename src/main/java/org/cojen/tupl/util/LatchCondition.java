/*
 *  Copyright 2016 Cojen.org
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

import java.util.concurrent.locks.LockSupport;

/**
 * Manages a queue of waiting threads, associated with a {@link Latch} instance. Unlike the
 * built-in Java Condition class, spurious wakeup does not occur when waiting.
 *
 * @author Brian S O'Neill
 */
public final class LatchCondition {
    private static final ThreadLocal<Node> cLocalNode = new ThreadLocal<>();

    Node mHead;
    Node mTail;

    /**
     * Returns true if no waiters are enqueued. Caller must hold shared or exclusive latch.
     */
    public boolean isEmpty() {
        return mHead == null;
    }

    /**
     * Blocks the current thread until a signal is received. Exclusive latch must be acquired
     * by the caller, which is released and then re-acquired by this method.
     *
     * @param latch latch being used by this condition
     * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
     * @param nanosEnd absolute nanosecond time to wait until; used only with &gt;0 timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    public int await(Latch latch, long nanosTimeout, long nanosEnd) {
        try {
            return await(latch, localNode(Node.WAITING), nanosTimeout, nanosEnd);
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            latch.releaseExclusive();
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
     * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
     * @param nanosEnd absolute nanosecond time to wait until; used only with &gt;0 timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    public int awaitShared(Latch latch, long nanosTimeout, long nanosEnd) {
        try {
            return await(latch, localNode(Node.WAITING_SHARED), nanosTimeout, nanosEnd);
        } catch (Throwable e) {
            // Possibly an OutOfMemoryError.
            latch.releaseExclusive();
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
    public void signal() {
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
    public boolean signalRelease(Latch latch) {
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
    public boolean signalNext() {
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
    public void signalAll() {
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
    public void signalShared() {
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
    public boolean signalSharedRelease(Latch latch) {
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
    public boolean signalNextShared() {
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
    public void clear() {
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
