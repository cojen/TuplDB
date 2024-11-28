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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.util.Clutch;
import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.core.Node.*;
import static org.cojen.tupl.core.DirectPageOps.*;

/**
 * Nodes are organized into groups, to reduce contention when updating data structures which
 * track them. Essentially, this is a striping/partitioning/sharding strategy.
 *
 * <p>NodeGroup maintains a Node usage list, which orders Nodes from least to most recently
 * used. Least recently used Nodes are more likely to be selected for eviction. The group also
 * maintains a list of dirty nodes, which must be written when evicted, or as part of a
 * checkpoint.
 *
 * <p>Nodes are guarded by latches, and they use the Clutch class to more efficiently handle
 * high contention. The NodeGroup extends Clutch.Pack, which serves as the contention tracking
 * structure shared by all Nodes in the group. Using a Pack for each Node would consume much
 * more memory, and the Pack is designed to be shared in this fashion.
 *
 * <p>As a convenience, the Cluck.Pack class extends from Latch, which is used here to guard
 * access to the Node usage list. A Latch could be allocated as a final field, but this trick
 * eliminates a pointer hop.
 *
 * @author Brian S O'Neill
 */
final class NodeGroup extends Clutch.Pack implements Checkpointer.DirtySet {
    // Allocate an unevictable node.
    static final int MODE_UNEVICTABLE = 1;

    // Don't evict a node when trying to allocate another.
    static final int MODE_NO_EVICT = 2;

    // Amount of contended clutches that this Pack can support. Not sure what the best amount
    // is, but this seems to be more than enough. When running on a machine with more CPU
    // cores, more NodeGroups are created. In addition, the Pack itself will allocate more
    // counter slots for more cores.
    private static final int PACK_SLOTS = 64;

    final LocalDatabase mDatabase;
    private final int mPageSize;
    private final long mUsedRate;

    // The usage list fields are guarded by the latch inherited from Clutch.Pack.
    private int mMaxSize;
    private int mSize;
    private Node mMostRecentlyUsed;
    private Node mLeastRecentlyUsed;

    // Linked list of dirty nodes, guarded by synchronization.
    private Node mFirstDirty;
    private Node mLastDirty;
    private long mDirtyCount;
    // Iterator over dirty nodes.
    private Node mFlushNext;

    private final Latch mSparePageLatch;
    private long mSparePageAddr;

    /**
     * @param usedRate must be power of 2 minus 1, and it determines the likelihood that
     * calling the used method actually moves the node in the usage list. The higher the used
     * rate value, the less likely that calling the used method does anything. The used rate
     * value should be proportional to the total cache size. For larger caches, exact MRU
     * ordering is less critical, and the cost of updating the ordering is also higher. Hence,
     * a larger used rate value is recommended. Passing a value of -1 effectively disables node
     * movement (probability is extremely low).
     */
    NodeGroup(LocalDatabase db, long usedRate, int maxSize) {
        super(PACK_SLOTS);
        if (maxSize <= 0) {
            throw new IllegalArgumentException();
        }
        mDatabase = db;
        mPageSize = db.pageSize();
        mUsedRate = usedRate;

        acquireExclusive();
        mMaxSize = maxSize;
        releaseExclusive();

        mSparePageLatch = new Latch();
        mSparePageLatch.acquireExclusive();
        try {
            // If directPageSize is negative, then aligned allocation is requested.
            mSparePageAddr = DirectPageOps.p_callocPage(db.mPageDb.directPageSize());
        } finally {
            mSparePageLatch.releaseExclusive();
        }
    }

    int pageSize() {
        return mPageSize;
    }

    /**
     * Initialize and preallocate a minimum amount of nodes.
     *
     * @param arena optional
     */
    void initialize(Object arena, int min) throws DatabaseException, OutOfMemoryError {
        while (--min >= 0) {
            acquireExclusive();
            if (mSize >= mMaxSize) {
                releaseExclusive();
                break;
            }
            doAllocLatchedNode(arena, 0).releaseExclusive();
        }
    }

    int nodeCount() {
        acquireShared();
        int size = mSize;
        releaseShared();
        return size;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     *
     * @param trial pass 1 for less aggressive recycle attempt
     * @param mode MODE_UNEVICTABLE | MODE_NO_EVICT
     * @return null if no nodes can be recycled or created
     */
    Node tryAllocLatchedNode(int trial, int mode) throws IOException {
        acquireExclusive();

        int limit = mSize;
        do {
            Node node = mLeastRecentlyUsed;
            Node moreUsed;
            if (node == null || (moreUsed = node.mMoreUsed) == null) {
                // Grow the cache if possible.
                if (mSize < mMaxSize) {
                    return doAllocLatchedNode(null, mode);
                } else if (node == null) {
                    break;
                }
            } else {
                // Move node to the most recently used position.
                moreUsed.mLessUsed = null;
                mLeastRecentlyUsed = moreUsed;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }

            if (!node.tryAcquireExclusive()) {
                continue;
            }

            if (trial == 1) {
                if (node.mCachedState != CACHED_CLEAN) {
                    if (mSize < mMaxSize) {
                        // Grow the cache instead of evicting.
                        node.releaseExclusive();
                        return doAllocLatchedNode(null, mode);
                    } else if ((mode & MODE_NO_EVICT) != 0) {
                        node.releaseExclusive();
                        break;
                    }
                }

                // For first attempt, release the latch early to prevent blocking other
                // allocations while node is evicted. Subsequent attempts retain the latch,
                // preventing potential allocation starvation.

                releaseExclusive();

                if (node.evict(mDatabase)) {
                    if ((mode & MODE_UNEVICTABLE) != 0) {
                        node.mGroup.makeUnevictable(node);
                    }
                    // Return with node latch still held.
                    return node;
                }

                acquireExclusive();
            } else {
                if ((mode & MODE_NO_EVICT) != 0 && node.mCachedState != CACHED_CLEAN) {
                    // MODE_NO_EVICT is only used by non-stored database. It ensures that
                    // all clean nodes are least recently used, so no need to keep looking.
                    node.releaseExclusive();
                    break;
                }
                try {
                    if (node.evict(mDatabase)) {
                        if ((mode & MODE_UNEVICTABLE) != 0) {
                            NodeGroup group = node.mGroup;
                            if (group == this) {
                                doMakeUnevictable(node);
                            } else {
                                releaseExclusive();
                                group.makeUnevictable(node);
                                // Return with node latch still held.
                                return node;
                            }
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
        } while (--limit > 0);

        releaseExclusive();

        return null;
    }

    /**
     * Caller must acquire latch, which is released by this method.
     *
     * @param arena optional
     * @param mode MODE_UNEVICTABLE
     */
    private Node doAllocLatchedNode(Object arena, int mode) throws DatabaseException {
        try {
            mDatabase.checkClosed();

            long pageAddr = mDatabase.mFullyMapped ? p_nonTreePage()
                : p_callocPage(arena, mDatabase.mPageDb.directPageSize());

            var node = new Node(this, pageAddr);
            node.acquireExclusive();
            mSize++;

            if ((mode & MODE_UNEVICTABLE) == 0) {
                Node most = mMostRecentlyUsed;
                node.mLessUsed = most;
                if (most == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    most.mMoreUsed = node;
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
    void used(final Node node, final ThreadLocalRandom rnd) {
        // Moving the node in the usage list is expensive for several reasons. First is the
        // rapid rate at which shared memory is written to. This creates memory access
        // contention between CPU cores. Second is the garbage collector. The G1 collector in
        // particular appears to be very sensitive to old generation objects being shuffled
        // around too much. Finally, latch acquisition itself can cause contention. If the node
        // is popular, it will get more chances to be identified as most recently used. This
        // strategy works well enough because cache eviction is always a best-guess approach.

        if ((rnd.nextLong() & mUsedRate) == 0 && tryAcquireExclusive()) {
            doUsed(node);
        }
    }

    private void doUsed(final Node node) {
        Node moreUsed = node.mMoreUsed;
        if (moreUsed != null) {
            Node lessUsed = node.mLessUsed;
            moreUsed.mLessUsed = lessUsed;
            if (lessUsed == null) {
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

    /**
     * Indicate that node is least recently used, allowing it to be recycled immediately
     * without evicting another node. Node must be latched by caller, which is always released
     * by this method.
     */
    void unused(final Node node) {
        // Node latch is held to ensure that it isn't used for new allocations too soon. In
        // particular, it might be used for an unevictable allocation. This method would end up
        // erroneously moving the node back into the usage list. 

        try {
            acquireExclusive();
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }

        try {
            Node lessUsed = node.mLessUsed;
            if (lessUsed != null) {
                Node moreUsed = node.mMoreUsed;
                lessUsed.mMoreUsed = moreUsed;
                if (moreUsed == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
                (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
                mLeastRecentlyUsed = node;
            } else if (mMaxSize != 0) {
                doMakeEvictableNow(node);
            }
        } finally {
            // The node latch must be released before releasing the usage list latch, to
            // prevent the node from being immediately promoted to the most recently used by
            // tryAllocLatchedNode. The caller would acquire the usage list latch, fail to
            // acquire the node latch, and then the node gets falsely promoted.
            node.releaseExclusive();
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
            // Only insert if not closed and if not already in the list. The node latch doesn't
            // need to be held, and so a concurrent call to the unused method might insert the
            // node sooner.
            if (mMaxSize != 0 && node.mMoreUsed == null) {
                Node most = mMostRecentlyUsed;
                if (node != most) {
                    node.mLessUsed = most;
                    if (most == null) {
                        mLeastRecentlyUsed = node;
                    } else {
                        most.mMoreUsed = node;
                    }
                    mMostRecentlyUsed = node;
                }
            }
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
            // See comment in the makeEvictable method.
            if (mMaxSize != 0 && node.mLessUsed == null) {
                doMakeEvictableNow(node);
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold latch, have checked that this list isn't closed, and have checked that
     * node.mLessUsed is null.
     */
    private void doMakeEvictableNow(final Node node) {
        Node least = mLeastRecentlyUsed;
        if (node != least) {
            node.mMoreUsed = least;
            if (least == null) {
                mMostRecentlyUsed = node;
            } else {
                least.mLessUsed = node;
            }
            mLeastRecentlyUsed = node;
        }
    }

    /**
     * Allow a Node which was allocated as evictable to be unevictable.
     */
    void makeUnevictable(final Node node) {
        acquireExclusive();
        try {
            if (mMaxSize != 0) {
                doMakeUnevictable(node);
            }
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

        if (lessUsed != null) {
            node.mLessUsed = null;
            if (moreUsed != null) {
                node.mMoreUsed = null;
                lessUsed.mMoreUsed = moreUsed;
                moreUsed.mLessUsed = lessUsed;
            } else if (node == mMostRecentlyUsed) {
                mMostRecentlyUsed = lessUsed;
                lessUsed.mMoreUsed = null;
            }
        } else if (node == mLeastRecentlyUsed) {
            mLeastRecentlyUsed = moreUsed;
            if (moreUsed != null) {
                node.mMoreUsed = null;
                moreUsed.mLessUsed = null;
            } else {
                mMostRecentlyUsed = null;
            }
        }
    }

    /**
     * Move or add node to the end of the dirty list.
     *
     * @param node latched exclusively
     * @param cachedState node cached state to set; CACHED_DIRTY_0 or CACHED_DIRTY_1
     */
    synchronized void addDirty(Node node, byte cachedState) {
        node.mCachedState = cachedState;

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
            mDirtyCount++;
            if (last == null) {
                mFirstDirty = node;
            } else {
                node.mPrevDirty = last;
                last.mNextDirty = node;
            }
        }

        mLastDirty = node;

        if (mFlushNext == node) {
            // Ensure that flush continues scanning over dirty nodes with the old state.
            mFlushNext = next;
        }
    }

    /**
     * Remove the old node from the dirty list and swap in the new node. The cached state of
     * the nodes is not altered.
     */
    synchronized void swapIfDirty(Node oldNode, Node newNode) {
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
    }

    /**
     * Flush all nodes matching the given state. Only one flush at a time is allowed.
     *
     * @param dirtyState the old dirty state to match on; CACHED_DIRTY_0 or CACHED_DIRTY_1
     */
    @Override
    public void flushDirty(final int dirtyState) throws IOException {
        final PageDb pageDb = mDatabase.mPageDb;

        synchronized (this) {
            mFlushNext = mFirstDirty;
        }

        while (true) {
            Node node;
            int state;

            synchronized (this) {
                node = mFlushNext;
                if (node == null) {
                    return;
                }

                state = node.mCachedState;

                if (state == (dirtyState ^ 1)) {
                    // Now seeing nodes with new dirty state, so all done flushing.
                    mFlushNext = null;
                    return;
                }

                mFlushNext = node.mNextDirty;

                // Remove from list. Node can be clean or dirty at this point. If clean, then
                // node was written out without having been removed from the dirty list. Now's
                // a good time to fix the list.
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

                mDirtyCount--;
            }

            if (state == Node.CACHED_CLEAN) {
                // Don't write clean nodes. There's no need to latch and double-check the node
                // state, since the next valid state can only be the new dirty state.
                continue;
            }

            node.acquireExclusive();
            state = node.mCachedState;
            if (state != dirtyState) {
                // Node state is now clean or the new dirty state, so don't write it.
                node.releaseExclusive();
                continue;
            }

            node.downgrade();
            try {
                node.write(pageDb);
                // Clean state must be set after write completes. Although the latch has been
                // downgraded to shared, modifying the state is safe because no other thread
                // could have changed it. This is because the exclusive latch was acquired
                // first. Releasing the shared latch performs a volatile assignment, and so the
                // state change gets propagated correctly. This holds true even when using the
                // Clutch instead of a plain Latch. Exclusive acquisition always disables
                // contended mode, and it cannot flip back until after the downgraded latch has
                // been fully released.
                node.mCachedState = Node.CACHED_CLEAN;
            } catch (Throwable e) {
                // Add it back to the list for flushing again later.
                addDirty(node, (byte) state);
                throw e;
            } finally {
                node.releaseShared();
            }
        }
    }

    synchronized long dirtyCount() {
        return mDirtyCount;
    }

    long acquireSparePageAddr() {
        mSparePageLatch.acquireExclusive();
        return mSparePageAddr;
    }

    void releaseSparePageAddr(long pageAddr) {
        mSparePageAddr = pageAddr;
        mSparePageLatch.releaseExclusive();
    }

    /**
     * Must be called when object is no longer referenced. All nodes tracked by this group are
     * removed and deleted.
     */
    void delete() {
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

                // Free memory and make node appear to be evicted.
                node.delete(mDatabase);

                node = next;
            }
        } finally {
            releaseExclusive();
        }

        synchronized (this) {
            Node node = mFirstDirty;
            mFlushNext = null;
            mFirstDirty = null;
            mLastDirty = null;
            while (node != null) {
                node.delete(mDatabase);
                Node next = node.mNextDirty;
                node.mPrevDirty = null;
                node.mNextDirty = null;
                node = next;
            }
        }

        mSparePageLatch.acquireExclusive();
        try {
            DirectPageOps.p_delete(mSparePageAddr);
            mSparePageAddr = DirectPageOps.p_null();
        } finally {
            mSparePageLatch.releaseExclusive();
        }
    }
}
