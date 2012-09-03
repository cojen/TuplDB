/*
 *  Copyright 2012 Brian S O'Neill
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
    private final PageDb mSource;

    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mIterateNext;

    PageAllocator(PageDb source) {
        mSource = source;
    }

    /**
     * @param forNode node which needs a new page; must be latched
     */
    long allocPage(Node forNode) throws IOException {
        // When allocations are in order, the list maintains the order.
        dirty(forNode);
        return mSource.allocPage();
    }

    /**
     * Move or add node to the end of the dirty list.
     */
    synchronized void dirty(Node node) {
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
        // See removeNextDirtyNode for explanation for node latch requirement.
        if (mIterateNext == node) {
            mIterateNext = next;
        }
    }

    void recyclePage(long id) throws IOException {
        // TODO: Implement proper page recycling.
        mSource.deletePage(id);
    }

    /**
     * Begin iterating over the dirty node list. Only one iteration at a time
     * is allowed.
     */
    synchronized void beginDirtyIteration() {
        mIterateNext = mFirstDirty;
    }

    /**
     * Iterate to the next dirty node, and remove it from the dirty
     * list. Returned Nodes are latched exclusively; caller is responsible for
     * unlatching.
     *
     * @param dirtyState node must match this state
     */
    Node removeNextDirtyNode(int dirtyState) {
        Node node;
        while (true) {
            synchronized (this) {
                node = mIterateNext;
                if (node == null) {
                    return null;
                }
                mIterateNext = node.mNextDirty;
            }

            node.acquireExclusive();
            if (node.mCachedState == dirtyState) {
                break;
            }
            node.releaseExclusive();
        }

        // Remove from list. Because allocPage requires nodes to be latched,
        // there's no need to update mIterateNext. The removed node will never
        // be the same as mIterateNext.
        synchronized (this) {
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
        }

        return node;
    }

    /**
     * Remove all nodes from dirty list, as part of close sequence.
     */
    synchronized void clearDirtyNodes() {
        Node node = mFirstDirty;
        mIterateNext = null;
        mFirstDirty = null;
        mLastDirty = null;
        while (node != null) {
            Node next = node.mNextDirty;
            node.mPrevDirty = null;
            node.mNextDirty = null;
            node = next;
        }
    }
}
