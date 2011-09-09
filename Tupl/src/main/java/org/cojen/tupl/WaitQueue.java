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
     * @param nanosTimeout negative for infinite
     * @return -1 if interrupted, 0 if timed out, 1 if signalled
     */
    int await(Latch latch, Node node, long nanosTimeout) {
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
                latch.acquireExclusiveUnfair();
                int state = node.state();
                if (state > 0) {
                    return 1;
                } else if (state < 0) {
                    node.remove(this);
                    return -1;
                }
            }
        } else if (nanosTimeout == 0) {
            latch.releaseExclusive();
            LockSupport.parkNanos(0);
            latch.acquireExclusiveUnfair();
            return node.state();
        } else {
            long end = System.nanoTime() + nanosTimeout;
            while (true) {
                latch.releaseExclusive();
                LockSupport.parkNanos(nanosTimeout);
                latch.acquireExclusiveUnfair();
                int state = node.state();
                if (state > 0) {
                    return 1;
                } else if (state < 0) {
                    node.remove(this);
                    return -1;
                } else if ((nanosTimeout = end - System.nanoTime()) <= 0) {
                    node.remove(this);
                    return 0;
                }
            }
        }
    }

    /**
     * Exclusive latch must be held, which is still held when method returns.
     */
    void signalOne() {
        Node head = mHead;
        if (head != null) {
            if ((mHead = head.mNext) == null) {
                mTail = null;
            }
            head.signal();
        }
    }

    /**
     * Exclusive latch must be held, which is still held when method returns.
     */
    void signalAll() {
        Node head = mHead;
        if (head != null) {
            while (true) {
                Node next = head.mNext;
                head.signal();
                if (next == null) {
                    mHead = null;
                    mTail = null;
                    return;
                }
                head = next;
            }
        }
    }

    /**
     * Signals all shared waiters in the queue until an exclusive waiter is
     * reached. Exclusive latch must be held, which is still held when method
     * returns.
     */
    void signalShared() {
        Node head = mHead;
        if (head instanceof Shared) {
            while (true) {
                Node next = head.mNext;
                head.signal();
                if (next == null) {
                    mHead = null;
                    mTail = null;
                    return;
                }
                if (!(next instanceof Shared)) {
                    mHead = next;
                    return;
                }
                head = next;
            }
        }
    }

    /**
     * Signals all shared waiters in the queue until an exclusive waiter is
     * reached. If only an exclusive waiter is seen, it is signalled instead.
     * Exclusive latch must be held, which is still held when method returns.
     */
    void signalSharedOrOneExclusive() {
        Node head = mHead;
        if (head != null) {
            while (true) {
                Node next = head.mNext;
                head.signal();
                if (next == null) {
                    mHead = null;
                    mTail = null;
                    return;
                }
                if (!(next instanceof Shared)) {
                    mHead = next;
                    return;
                }
                head = next;
            }
        }
    }

    static class Node {
        Thread mWaiter;
        Node mPrev;
        Node mNext;

        /**
         * @return -1 if interrupted, 0 if not signalled, 1 if signalled
         */
        final int state() {
            Thread thread = mWaiter;
            if (thread.isInterrupted()) {
                thread.interrupted();
                // Favor signal over interrupt. Caller removes if interrupted.
                return mNext == this ? 1 : -1;
            } else {
                return mNext == this ? 1 : 0;
            }
        }

        final void signal() {
            mNext = this;
            mPrev = null;
            LockSupport.unpark(mWaiter);
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

    static class Shared extends Node {
    }
}
