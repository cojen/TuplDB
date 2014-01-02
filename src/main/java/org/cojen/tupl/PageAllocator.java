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

    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mFlushNext;

    PageAllocator(PageDb source) {
        mPageDb = source;
        mLatch = new Latch();
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
     * Flush all nodes matching the given state. Only one flush at a time is allowed.
     */
    void flushDirtyNodes(final int dirtyState) throws IOException {
        final Latch latch = mLatch;

        latch.acquireExclusive();
        mFlushNext = mFirstDirty;
        latch.releaseExclusive();

        while (true) {
            Node node;
            while (true) {
                latch.acquireExclusive();
                try {
                    node = mFlushNext;
                    if (node == null) {
                        return;
                    }
                    mFlushNext = node.mNextDirty;
                } finally {
                    latch.releaseExclusive();
                }

                node.acquireExclusive();
                if (node.mCachedState == dirtyState) {
                    break;
                }
                node.releaseExclusive();
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

            node.downgrade();
            try {
                node.write(mPageDb);
                // Clean state must be set after write completes. Although latch has been
                // downgraded to shared, modifying the state is safe because no other thread
                // could have changed it. This is because the exclusive latch was acquired
                // first.  Releasing the shared latch performs a volatile write, and so the
                // state change gets propagated correctly.
                node.mCachedState = Node.CACHED_CLEAN;
            } finally {
                node.releaseShared();
            }
        }
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
