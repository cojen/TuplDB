/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.io.InterruptedIOException;
import java.io.IOException;

/**
 * Tracks a list of pages which were allocated, allowing them to be iterated
 * over in the original order.
 *
 * @author Brian S O'Neill
 */
final class PageAllocator {
    private final PageDb mPageDb;
    private final Latch mLatch;
    private final WaitQueue mWaitQueue;

    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mFlushNext;
    private int mFlushDirtyState;
    private int mAltFlushingCount;
    private IOException mAltFlushFailure;

    PageAllocator(PageDb source) {
        mPageDb = source;
        mLatch = new Latch();
        mWaitQueue = new WaitQueue();
    }

    /**
     * @param forNode node which needs a new page; must be latched
     */
    long allocPage(Node forNode) throws IOException {
        // When allocations are in order, the list maintains the order.
        dirty(forNode);
        return mPageDb.allocPage();
    }

    /**
     * Move or add node to the end of the dirty list.
     */
    void dirty(Node node) {
        final Latch latch = mLatch;
        latch.acquireExclusive();
        try {
            /*
            // If a flush is in progress, throttle any threads which are
            // dirtying more nodes, by forcing them to do some of the work.
            int dirtyState = mFlushDirtyState;
            if (dirtyState != 0) {
                try {
                    flushNextDirtyNode(dirtyState, false);
                } catch (IOException e) {
                    // Should not be thrown when flusher argument is false.
                    throw Utils.rethrow(e);
                }
                latch.acquireExclusive();
            }
            */

            final Node next = node.mNextDirty;
            final Node prev = node.mPrevDirty;
            if (next != null) {
                if ((next.mPrevDirty = prev) == null) {
                    mFirstDirty = next;
                } else {
                    prev.mNextDirty = next;
                }
                node.mNextDirty = null;
                (node.mPrevDirty = mLastDirty).mNextDirty = node;
            } else if (prev == null) {
                Node last = mLastDirty;
                if (last == node) {
                    return;
                }
                if (last == null) {
                    mFirstDirty = node;
                } else {
                    node.mPrevDirty = last;
                    last.mNextDirty = node;
                }
            }
            mLastDirty = node;
            // See flushNextDirtyNode for explanation for node latch requirement.
            if (mFlushNext == node) {
                mFlushNext = next;
            }
        } finally {
            latch.releaseExclusive();
        }
    }

    void recyclePage(long id) throws IOException {
        mPageDb.recyclePage(id);
    }

    /**
     * Flush all nodes matching the given state. Only one flush at a time is
     * allowed.
     */
    void flushDirtyNodes(int dirtyState) throws IOException {
        final Latch latch = mLatch;
        latch.acquireExclusive();
        mFlushNext = mFirstDirty;
        mFlushDirtyState = dirtyState;
        latch.releaseExclusive();

        do {
            latch.acquireExclusive();
        } while (flushNextDirtyNode(dirtyState, true));

        latch.acquireExclusive();
        try {
            // Wait for concurrent writes to complete.
            while (mAltFlushingCount != 0) {
                if (mWaitQueue.await(latch, new WaitQueue.Node(), -1, 0) < 0) {
                    throw new InterruptedIOException();
                }
            }
            IOException ex = mAltFlushFailure;
            if (ex != null) {
                mAltFlushFailure = null;
                throw ex;
            }
        } finally {
            latch.releaseExclusive();
        }
    }

    /**
     * Iterate to the next dirty node, remove it from the dirty list, and then
     * flush it. Caller must have acquired latch, which is released by this
     * method.
     *
     * @param dirtyState iteration and node must match this state
     * @param flusher pass true if called by main flushing thread; IOException
     * is not thrown from this method when false
     * @return false if iteration is complete
     */
    private boolean flushNextDirtyNode(int dirtyState, boolean flusher) throws IOException {
        Node node;
        final Latch latch;
        try {
            node = mFlushNext;
            if (node == null) {
                mFlushDirtyState = 0;
                return false;
            }
            if (!flusher) {
                if (!node.tryAcquireExclusive()) {
                    // Deadlock avoidance.
                    return true;
                }
                mAltFlushingCount++;
            }
            mFlushNext = node.mNextDirty;
        } finally {
            (latch = mLatch).releaseExclusive();
        }

        while (true) {
            if (flusher) {
                node.acquireExclusive();
            }
            if (node.mCachedState == dirtyState) {
                break;
            }
            node.releaseExclusive();

            latch.acquireExclusive();
            try {
                node = mFlushNext;
                if (node == null) {
                    mFlushDirtyState = 0;
                    if (!flusher && --mAltFlushingCount == 0) {
                        mWaitQueue.signal();
                    }
                    return false;
                }
                if (!flusher && !node.tryAcquireExclusive()) {
                    if (--mAltFlushingCount == 0) {
                        mWaitQueue.signal();
                    }
                    return true;
                }
                mFlushNext = node.mNextDirty;
            } finally {
                latch.releaseExclusive();
            }
        }

        // Remove from list. Because allocPage requires nodes to be latched,
        // there's no need to update mFlushNext. The removed node will never be
        // the same as mFlushNext.
        latch.acquireExclusive();
        try {
            Node next = node.mNextDirty;
            Node prev = node.mPrevDirty;
            if (next != null) {
                next.mPrevDirty = prev;
                node.mNextDirty = null;
            } else if (mLastDirty == node) {
                mLastDirty = prev;
            }
            if (prev != null) {
                prev.mNextDirty = next;
                node.mPrevDirty = null;
            } else if (mFirstDirty == node) {
                mFirstDirty = next;
            }
        } finally {
            latch.releaseExclusive();
        }

        IOException ex = null;
        try {
            node.downgrade();
            try {
                node.write(mPageDb);
                // Clean state must be set after write completes. Although latch
                // has been downgraded to shared, modifying the state is safe
                // because no other thread could have changed it. This is because
                // the exclusive latch was acquired first. Releasing the shared
                // latch performs a volatile write, and so the state change gets
                // propagated correctly.
                node.mCachedState = Node.CACHED_CLEAN;
            } finally {
                node.releaseShared();
            }
        } catch (IOException e) {
            if (flusher) {
                throw e;
            }
            ex = e;
        }

        if (!flusher) {
            latch.acquireExclusive();
            try {
                if (ex != null && mAltFlushFailure != null) {
                    mAltFlushFailure = ex;
                }
                if (--mAltFlushingCount == 0) {
                    mWaitQueue.signal();
                }
            } finally {
                latch.releaseExclusive();
            }
        }

        return true;
    }

    /**
     * Remove all nodes from dirty list, as part of close sequence.
     */
    void clearDirtyNodes() {
        mLatch.acquireExclusive();
        try {
            Node node = mFirstDirty;
            mFlushNext = null;
            mFirstDirty = null;
            mLastDirty = null;
            while (node != null) {
                Node next = node.mNextDirty;
                node.mPrevDirty = null;
                node.mNextDirty = null;
                node = next;
            }
        } finally {
            mLatch.releaseExclusive();
        }
    }
}
