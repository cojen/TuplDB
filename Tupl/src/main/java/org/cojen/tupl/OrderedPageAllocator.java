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
 * Ordered collection of available pages which also tracks which nodes have
 * been dirtied. One instance is expected to be used by a Database.
 *
 * @author Brian S O'Neill
 */
final class OrderedPageAllocator {
    private static final int UNKNOWN = 0, EMPTY = 1, READY = 2;

    private static final int MIN_FILL = 10;

    private final PageStore mSource;
    private final Index mLocalPages;
    private final Cursor mAllocCursor;
    private volatile int mReadyState;

    // Linked list of dirty nodes.
    private Node mFirstDirty;
    private Node mLastDirty;

    // Iterator over dirty nodes.
    private Node mIterateNext;

    /**
     * @param localPages index for storing readily available pages
     */
    OrderedPageAllocator(PageStore source, Index localPages) {
        mSource = source;
        mLocalPages = localPages;
        mAllocCursor = localPages.newCursor(Transaction.BOGUS);
        mAllocCursor.autoload(false);
    }

    void fill() throws IOException {
        PageStore source = mSource;
        if (source.allocPageCount() < MIN_FILL) {
            // The mere act of filling creates dirty pages, when then forces
            // the next checkpoint to perform work. This causes a cycle of
            // filling and checkpointing even when no user writes are being
            // performed. This breaks the cycle.
            return;
        }

        byte[] key = new byte[8];

        Cursor fillCursor = mLocalPages.newCursor(Transaction.BOGUS);
        try {
            fillCursor.autoload(false);
            long lastId = 0;
            long id;
            while ((id = source.tryAllocPage()) != 0) {
                DataUtils.writeLong(key, 0, id);
                if (id >= lastId) {
                    // PageStore implementation is expected to provide partially
                    // ordered ids, so use the faster find variant.
                    fillCursor.findNearby(key);
                } else {
                    // Id is lower than last, so another group was definitely
                    // read in. Don't expect it to be found nearby.
                    fillCursor.find(key);
                }
                fillCursor.store(Utils.EMPTY_BYTES);
                makeReady();
                lastId = id;
            }
        } finally {
            fillCursor.reset();
        }
    }

    /**
     * @param forNode node which needs a new page; must be latched
     */
    long allocPage(final Tree forIndex, final Node forNode) throws IOException {
        allocLocal: {
            synchronized (this) {
                // Move or add node to the end of the dirty list. Allocations
                // are in order, and the list maintains the order.
                move: {
                    final Node next = forNode.mNextDirty;
                    final Node prev = forNode.mPrevDirty;
                    if (next != null) {
                        if ((next.mPrevDirty = prev) == null) {
                            mFirstDirty = next;
                        } else {
                            prev.mNextDirty = next;
                        }
                        forNode.mNextDirty = null;
                        (forNode.mPrevDirty = mLastDirty).mNextDirty = forNode;
                    } else if (prev == null) {
                        Node last = mLastDirty;
                        if (last == forNode) {
                            break move;
                        }
                        if (last == null) {
                            mFirstDirty = forNode;
                        } else {
                            forNode.mPrevDirty = last;
                            last.mNextDirty = forNode;
                        }
                    }
                    mLastDirty = forNode;
                    // See removeNextDirtyNode for explanation for node latch requirement.
                    if (mIterateNext == forNode) {
                        mIterateNext = next;
                    }
                }
            }

            if (isInternal(forIndex) || mReadyState == EMPTY) {
                // Avoid cyclic dependency when allocating pages for internal
                // trees. Latch deadlock is highly likely, especially for the
                // page index itself and the registry.
                break allocLocal;
            }

            Cursor cursor = mAllocCursor;
            synchronized (cursor) {
                int state = mReadyState;
                if (state == EMPTY) {
                    break allocLocal;
                }

                byte[] key;
                makeReady: {
                    if (state == READY) {
                        cursor.next();
                        if ((key = cursor.key()) != null) {
                            break makeReady;
                        }
                    }
                    cursor.first();
                    if ((key = cursor.key()) == null) {
                        mReadyState = EMPTY;
                        break allocLocal;
                    }
                    mReadyState = READY;
                }

                long id = DataUtils.readLong(key, 0);

                // Delete entry while still synchronized -- cursor is not
                // thread-safe. Besides, one section of the index will be
                // heavily updated so this avoids latch contention.
                cursor.store(null);

                return id;
            }
        }

        return mSource.allocPage();
    }

    void deletePage(long id) throws IOException {
        // Page cannot be re-used until after checkpoint.
        mSource.deletePage(id);
    }

    void recyclePage(long id) throws IOException {
        byte[] key = new byte[8];
        DataUtils.writeLong(key, 0, id);
        mLocalPages.store(Transaction.BOGUS, key, Utils.EMPTY_BYTES);
        makeReady();
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

    private void makeReady() {
        if (mReadyState == EMPTY) synchronized (mAllocCursor) {
            if (mReadyState == EMPTY) {
                // Let the allocPage method determine for sure if ready.
                mReadyState = UNKNOWN;
            }
        }
    }

    private static boolean isInternal(Tree tree) {
        return tree != null && Tree.isInternal(tree.mId);
    }
}
