/*
 *  Copyright 2014 Brian S O'Neill
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

import static org.cojen.tupl.Node.*;

/**
 * List of Nodes, ordered from least to most recently used.
 *
 * @author Brian S O'Neill
 */
final class NodeUsageList extends Latch {
    // Allocate an unevictable node.
    static final int MODE_UNEVICTABLE = 1;

    // Don't evict a node when trying to allocate another.
    static final int MODE_NO_EVICT = 2;

    private final transient Database mDb;
    private int mMaxSize;
    private int mSize;
    private Node mMostRecentlyUsed;
    private Node mLeastRecentlyUsed;

    NodeUsageList(Database db, int maxSize) {
        mDb = db;
        acquireExclusive();
        mMaxSize = maxSize;
        releaseExclusive();
    }

    /**
     * Initialize and preallocate a minimum amount of nodes.
     */
    void initialize(int min) throws DatabaseException, OutOfMemoryError {
        // Least recently used node must always point to a valid, more recently used node.
        min = Math.max(min, 2);

        while (--min >= 0) {
            acquireExclusive();
            if (mSize >= mMaxSize) {
                releaseExclusive();
                break;
            }
            doAllocLatchedNode(0).releaseExclusive();
        }
    }

    int size() {
        acquireShared();
        int size = mSize;
        releaseShared();
        return size;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an id
     * of zero and a clean state.
     *
     * @param trial pass 1 for less aggressive recycle attempt
     * @param mode MODE_UNEVICTABLE | MODE_NO_EVICT
     * @return null if no nodes can be recycled or created
     */
    Node tryAllocLatchedNode(int trial, int mode) throws IOException {
        acquireExclusive();
        alloc: {
            int max = mMaxSize;

            if (max == 0) {
                break alloc;
            }

            if (mSize < max &&
                (trial > 1
                 || mLeastRecentlyUsed == null || mLeastRecentlyUsed.mMoreUsed == null))
            {
                return doAllocLatchedNode(mode);
            }

            if ((mode & MODE_UNEVICTABLE) != 0
                && mLeastRecentlyUsed.mMoreUsed == mMostRecentlyUsed)
            {
                // Cannot allow list to shrink to less than two elements.
                break alloc;
            }

            do {
                Node node = mLeastRecentlyUsed;
                (mLeastRecentlyUsed = node.mMoreUsed).mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;

                if (!node.tryAcquireExclusive()) {
                    continue;
                }

                if (trial == 1) {
                    if (node.mCachedState != CACHED_CLEAN) {
                        if (mSize < mMaxSize) {
                            // Grow the cache instead of evicting.
                            node.releaseExclusive();
                            return doAllocLatchedNode(mode);
                        } else if ((mode & MODE_NO_EVICT) != 0) {
                            node.releaseExclusive();
                            break alloc;
                        }
                    }

                    // For first attempt, release the latch early to prevent blocking other
                    // allocations while node is evicted. Subsequent attempts retain the latch,
                    // preventing potential allocation starvation.

                    releaseExclusive();

                    if ((node = Node.evict(node, mDb)) != null) {
                        if ((mode & MODE_UNEVICTABLE) != 0) {
                            node.mUsageList.makeUnevictable(node);
                        }
                        // Return with node latch still held.
                        return node;
                    }

                    acquireExclusive();

                    if (mMaxSize == 0) {
                        break alloc;
                    }
                } else if ((mode & MODE_NO_EVICT) != 0) {
                    if (node.mCachedState != CACHED_CLEAN) {
                        // MODE_NO_EVICT is only used by non-durable database. It ensures that
                        // all clean nodes are least recently used, so no need to keep looking.
                        node.releaseExclusive();
                        break alloc;
                    }
                } else {
                    try {
                        if ((node = Node.evict(node, mDb)) != null) {
                            if ((mode & MODE_UNEVICTABLE) != 0) {
                                node.mUsageList.doMakeUnevictable(node);
                            }
                            releaseExclusive();
                            // Return with node latch still held.
                            return node;
                        }
                    } catch (Throwable e) {
                        releaseExclusive();
                        throw e;
                    }
                }
            } while (--max > 0);
        }

        releaseExclusive();

        return null;
    }

    /**
     * Caller must acquire latch, which is released by this method.
     *
     * @param mode MODE_UNEVICTABLE
     */
    private Node doAllocLatchedNode(int mode) throws DatabaseException {
        try {
            mDb.checkClosed();
            Node node = new Node(this, mDb.mPageSize);
            node.acquireExclusive();
            mSize++;
            if ((mode & MODE_UNEVICTABLE) == 0) {
                if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    mMostRecentlyUsed.mMoreUsed = node;
                }
                mMostRecentlyUsed = node;
            }
            // Return with node latch still held.
            return node;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Indicate that a non-root node is most recently used. Root node is not managed in usage
     * list and cannot be evicted. Caller must hold any latch on node. Latch is never released
     * by this method, even if an exception is thrown.
     */
    void used(final Node node) {
        // Because this method can be a bottleneck, don't wait for exclusive latch. If node is
        // popular, it will get more chances to be identified as most recently used. This
        // strategy works well enough because cache eviction is always a best-guess approach.
        if (tryAcquireExclusive()) {
            Node moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                Node lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            releaseExclusive();
        }
    }

    /**
     * Indicate that node is least recently used, allowing it to be recycled immediately
     * without evicting another node. Node must be unlatched at this point, to prevent it from
     * being immediately promoted to most recently used by tryAllocLatchedNode.
     */
    void unused(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize == 0) {
                // Closed.
                return;
            }
            Node lessUsed = node.mLessUsed;
            if (lessUsed == null) {
                // Node might already be least...
                if (node.mMoreUsed != null) {
                    // ...confirmed.
                    return;
                }
                // ...Node isn't in the usage list at all.
            } else {
                Node moreUsed = node.mMoreUsed;
                if ((lessUsed.mMoreUsed = moreUsed) == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
            }
            (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
            mLeastRecentlyUsed = node;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable, starting off as the
     * most recently used.
     */
    void makeEvictable(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize == 0) {
                // Closed.
                return;
            }
            if (node.mMoreUsed != null || node.mLessUsed != null) {
                throw new IllegalStateException();
            }
            (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
            mMostRecentlyUsed = node;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable, as the least recently
     * used.
     */
    void makeEvictableNow(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize == 0) {
                // Closed.
                return;
            }
            if (node.mMoreUsed != null || node.mLessUsed != null) {
                throw new IllegalStateException();
            }
            (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
            mLeastRecentlyUsed = node;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as evictable to be unevictable.
     */
    void makeUnevictable(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize == 0) {
                // Closed.
                return;
            }
            doMakeUnevictable(node);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold latch.
     */
    private void doMakeUnevictable(final Node node) {
        final Node lessUsed = node.mLessUsed;
        final Node moreUsed = node.mMoreUsed;
        if (lessUsed == null) {
            (mLeastRecentlyUsed = moreUsed).mLessUsed = null;
        } else if (moreUsed == null) {
            (mMostRecentlyUsed = lessUsed).mMoreUsed = null;
        } else {
            lessUsed.mMoreUsed = moreUsed;
            moreUsed.mLessUsed = lessUsed;
        }
        node.mMoreUsed = null;
        node.mLessUsed = null;
    }

    void close() {
        acquireExclusive();
        try {
            // Prevent new allocations.
            mMaxSize = 0;

            Node node = mLeastRecentlyUsed;
            mLeastRecentlyUsed = null;
            mMostRecentlyUsed = null;

            while (node != null) {
                Node next = node.mMoreUsed;
                node.mLessUsed = null;
                node.mMoreUsed = null;

                // Make node appear to be evicted.
                node.mId = 0;

                node = next;
            }
        } finally {
            releaseExclusive();
        }
    }
}
