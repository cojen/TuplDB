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
 * List of dirty nodes.
 *
 * @author Brian S O'Neill
 */
final class NodeDirtyList extends Latch {
    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mFlushNext;

    NodeDirtyList() {
    }

    /**
     * Move or add node to the end of the dirty list.
     */
    void add(Node node) {
        acquireExclusive();
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
            releaseExclusive();
        }
    }

    /**
     * Remove the old node from the dirty list and swap in the new node. The cached state of
     * the nodes is not altered.
     */
    void swapIfDirty(Node oldNode, Node newNode) {
        acquireExclusive();
        try {
            Node next = oldNode.mNextDirty;
            if (next != null) {
                newNode.mNextDirty = next;
                next.mPrevDirty = newNode;
                oldNode.mNextDirty = null;
            }
            Node prev = oldNode.mPrevDirty;
            if (prev != null) {
                newNode.mPrevDirty = prev;
                prev.mNextDirty = newNode;
                oldNode.mPrevDirty = null;
            }
            if (oldNode == mFirstDirty) {
                mFirstDirty = newNode;
            }
            if (oldNode == mLastDirty) {
                mLastDirty = newNode;
            }
            if (oldNode == mFlushNext) {
                mFlushNext = newNode;
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Flush all nodes matching the given state. Only one flush at a time is allowed.
     */
    void flush(final PageDb pageDb, final int dirtyState) throws IOException {
        acquireExclusive();
        mFlushNext = mFirstDirty;
        releaseExclusive();

        while (true) {
            Node node;
            while (true) {
                acquireExclusive();
                try {
                    node = mFlushNext;
                    if (node == null) {
                        return;
                    }
                    mFlushNext = node.mNextDirty;
                } finally {
                    releaseExclusive();
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
            acquireExclusive();
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
                releaseExclusive();
            }

            node.downgrade();
            try {
                node.write(pageDb);
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
     * Remove and delete nodes from dirty list, as part of close sequence.
     */
    void delete() {
        acquireExclusive();
        try {
            Node node = mFirstDirty;
            mFlushNext = null;
            mFirstDirty = null;
            mLastDirty = null;
            while (node != null) {
                node.delete();
                Node next = node.mNextDirty;
                node.mPrevDirty = null;
                node.mNextDirty = null;
                node = next;
            }
        } finally {
            releaseExclusive();
        }
    }
}
