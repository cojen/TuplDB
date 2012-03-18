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

import java.util.concurrent.locks.LockSupport;

/**
 * FIFO queue of waiters, which behaves like a condition variable.
 *
 * @author Brian S O'Neill
 * @see Lock
 */
class WaitQueue {
    Node mHead;
    Node mTail;

    boolean isEmpty() {
        return mHead == null;
    }

    /**
     * Wait for signal with no spurious wakeup. Exclusive latch must be held,
     * which is still held when method returns.
     *
     * @param node newly allocated node
     * @param nanosTimeout relative nanosecond time to wait; infinite if <0
     * @param nanosEnd absolute nanosecond time to wait until; used only with >0 timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     */
    int await(Latch latch, Node node, long nanosTimeout, long nanosEnd) {
        node.mWaiter = Thread.currentThread();

        Node tail = mTail;
        if (tail == null) {
            mHead = node;
        } else {
            tail.mNext = node;
            node.mPrev = tail;
        }
        mTail = node;

        if (nanosTimeout < 0) {
            while (true) {
                latch.releaseExclusive();
                LockSupport.park();
                latch.acquireExclusive();
                int state = node.resumed(this);
                if (state != 0) {
                    return state;
                }
            }
        } else {
            while (true) {
                latch.releaseExclusive();
                LockSupport.parkNanos(nanosTimeout);
                latch.acquireExclusive();
                int state = node.resumed(this);
                if (state != 0) {
                    return state;
                }
                if (nanosTimeout == 0 || (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                    node.remove(this);
                    return 0;
                }
            }
        }
    }

    /**
     * Signals the first waiter, unless queue is empty. Exclusive latch must be
     * held, which is still held when method returns.
     */
    void signal() {
        Node head = mHead;
        if (head != null) {
            head.signal();
        }
    }

    /**
     * Signals the first waiter if it is a shared waiter. Exclusive latch must
     * be held, which is still held when method returns.
     */
    void signalShared() {
        Node head = mHead;
        if (head instanceof Shared) {
            head.signal();
        }
    }

    /**
     * Same as signalShared, except returns false if queue is empty.
     */
    boolean signalNextShared() {
        Node head = mHead;
        if (head == null) {
            return false;
        }
        if (head instanceof Shared) {
            head.signal();
        }
        return true;
    }

    static class Node {
        Thread mWaiter;
        Node mPrev;
        Node mNext;

        /**
         * Called by thread which was parked after it resumes. Caller is
         * responsible for removing node from queue if return value is 0.
         *
         * @return -1 if interrupted, 0 if not signaled, 1 if signaled
         */
        final int resumed(WaitQueue queue) {
            Thread thread = mWaiter;
            if (thread == null) {
                remove(queue);
                return 1;
            }
            if (thread.isInterrupted()) {
                Thread.interrupted();
                remove(queue);
                return -1;
            }
            return 0;
        }

        final void signal() {
            LockSupport.unpark(mWaiter);
            mWaiter = null;
        }

        final void remove(WaitQueue queue) {
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
        }
    }

    static final class Shared extends Node {
    }
}
