/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.util.Arrays;
import java.util.BitSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.PageManager.ALLOC_NORMAL;
import static org.cojen.tupl.PageManager.ALLOC_RESERVE;
import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.scramble;

/**
 * Used by PageManager to implement free lists.
 *
 * @author Brian S O'Neill
 */
final class PageQueue implements IntegerRef {
    /*

    Header structure is encoded as follows, in 44 bytes:

    +---------------------------------------------------+
    | long: remove page count                           |
    | long: remove node count                           |
    | long: remove head id                              |
    | int:  remove head offset                          |
    | long: remove head first page id (seed for deltas) |
    | long: append head/tail id                         |
    +---------------------------------------------------+

    The page queue nodes are encoded as follows:

    +---------------------------------------+
    | long: next node id                    |
    | long: first page id (seed for deltas) |
    +---------------------------------------+
    | remaining page ids (delta encoded)    |
    -                                       -
    |                                       |
    +---------------------------------------+

    The two longs in the queue node header are big endian encoded. The first
    byte in the node is almost always 0, aiding in quick detection of
    corruption if a page queue node is referenced by another data structure.

    */

    // Indexes of header entries.
    private static final int I_REMOVE_PAGE_COUNT         = 0;
    private static final int I_REMOVE_NODE_COUNT         = I_REMOVE_PAGE_COUNT + 8;
    private static final int I_REMOVE_HEAD_ID            = I_REMOVE_NODE_COUNT + 8;
    private static final int I_REMOVE_HEAD_OFFSET        = I_REMOVE_HEAD_ID + 8;
    private static final int I_REMOVE_HEAD_FIRST_PAGE_ID = I_REMOVE_HEAD_OFFSET + 4;
    private static final int I_APPEND_HEAD_ID            = I_REMOVE_HEAD_FIRST_PAGE_ID + 8;
            static final int HEADER_SIZE                 = I_APPEND_HEAD_ID + 8;

    // Indexes of node entries.
    private static final int I_NEXT_NODE_ID  = 0;
    private static final int I_FIRST_PAGE_ID = I_NEXT_NODE_ID + 8;
    private static final int I_NODE_START    = I_FIRST_PAGE_ID + 8;

    private final PageManager mManager;
    private final int mAllocMode;
    private final boolean mAggressive;

    // These fields are guarded by remove lock provided by caller.
    private long mRemovePageCount;
    private long mRemoveNodeCount;
    private final /*P*/ byte[] mRemoveHead;
    private long mRemoveHeadId;
    private int mRemoveHeadOffset;
    private long mRemoveHeadFirstPageId;
    private long mRemoveStoppedId;
    private long mRemovedNodeCounter; // non-persistent count of nodes removed
    private long mReserveReclaimUpperBound; // used by reclaim method

    // Barrier between the remove and append lists. Remove stops when it
    // encounters the append head. Modification is permitted with the append
    // lock held, but no lock is required to access this id. Hence, volatile.
    private volatile long mAppendHeadId;

    // These fields are guarded by mAppendLock.
    private final ReentrantLock mAppendLock;
    private final IdHeap mAppendHeap;
    private final /*P*/ byte[] mAppendTail;
    private volatile long mAppendTailId;
    private long mAppendPageCount;
    private long mAppendNodeCount;
    private boolean mDrainInProgress;

    /**
     * Returns true if a valid PageQueue instance is encoded in the header.
     */
    static boolean exists(/*P*/ byte[] header, int offset) {
        return p_longGetLE(header, offset + I_REMOVE_HEAD_ID) != 0;
    }

    /**
     * @param manager used for allocating and deleting pages for the queue itself
     */
    static PageQueue newRegularFreeList(PageManager manager) {
        return new PageQueue(manager, ALLOC_NORMAL, false, null);
    }

    /**
     * @param manager used for allocating and deleting pages for the queue itself
     */
    static PageQueue newRecycleFreeList(PageManager manager) {
        return new PageQueue(manager, ALLOC_NORMAL, true, null);
    }

    /**
     * @param manager used for allocating and deleting pages for the queue itself
     * @param allocMode ALLOC_NORMAL or ALLOC_RESERVE
     * @param aggressive pass true if most appended pages are safe to remove
     */
    private PageQueue(PageManager manager, int allocMode, boolean aggressive,
                      ReentrantLock appendLock)
    {
        mManager = manager;
        mAllocMode = allocMode;
        mAggressive = aggressive;
        PageArray array = manager.pageArray();

        mRemoveHead = p_calloc(array.pageSize());

        if (appendLock == null) {
            // This lock must be reentrant. The appendPage method can call into
            // drainAppendHeap, which calls allocPage, which can re-acquire the append
            // lock. The commit method acquires the remove lock too, and then it calls
            // drainAppendHeap, acquiring the locks again. Note that locks are unfair. Pages
            // are allocated and deleted by tree operations, which unfairly acquire
            // latches. Waiting for a fair lock afterwards leads to priority inversion.
            mAppendLock = new ReentrantLock(false);
        } else {
            mAppendLock = appendLock;
        }

        mAppendHeap = new IdHeap(array.pageSize() - I_NODE_START);
        mAppendTail = p_calloc(array.pageSize());
    }

    /**
     * Must be called when object is no longer referenced.
     */
    void delete() {
        p_delete(mRemoveHead);
        p_delete(mAppendTail);
    }

    /**
     * @throws IllegalStateException if this is not a regular free list
     */
    PageQueue newReserveFreeList() {
        if (mAggressive) {
            throw new IllegalStateException();
        }
        // Needs to share the same lock as the regular free list to avoid deadlocks. The
        // reserve list might append to the regular list when deleting nodes, and the regular
        // list might append to the reserve list when doing the same thing. Neither will ever
        // call into the recycle list, since free list nodes cannot safely be recycled.
        // Allocate as non-aggressive, preventing page manager from raiding pages that were
        // deleted instead of recycled. Full reclamation is possible only after a checkpoint.
        return new PageQueue(mManager, ALLOC_RESERVE, false, mAppendLock);
    }

    /**
     * Initialize a fresh (non-restored) queue.
     */
    void init(long headNodeId) {
        mAppendLock.lock();
        try {
            mRemoveStoppedId = mAppendHeadId = mAppendTailId = headNodeId;
        } finally {
            mAppendLock.unlock();
        }
    }

    /**
     * Initialize a restored queue. Caller must hold append and remove locks.
     */
    void init(/*P*/ byte[] header, int offset) throws IOException {
        mRemovePageCount = p_longGetLE(header, offset + I_REMOVE_PAGE_COUNT);
        mRemoveNodeCount = p_longGetLE(header, offset + I_REMOVE_NODE_COUNT);

        mRemoveHeadId = p_longGetLE(header, offset + I_REMOVE_HEAD_ID);
        mRemoveHeadOffset = p_intGetLE(header, offset + I_REMOVE_HEAD_OFFSET);
        mRemoveHeadFirstPageId = p_longGetLE(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID);

        mAppendHeadId = mAppendTailId = p_longGetLE(header, offset + I_APPEND_HEAD_ID);

        if (mRemoveHeadId == 0) {
            mRemoveStoppedId = mAppendHeadId;
        } else {
            mManager.pageArray().readPage(mRemoveHeadId, mRemoveHead);
            if (mRemoveHeadFirstPageId == 0) {
                mRemoveHeadFirstPageId = p_longGetBE(mRemoveHead, I_FIRST_PAGE_ID);
            }
        }
    }

    /**
     * Delete all available pages, effectively deleting this PageQueue. Only works for page
     * manager reserve list, and only after a checkpoint.
     *
     * @param upperBound inclusive; pages greater than the upper bound are discarded
     */
    void reclaim(Lock removeLock, long upperBound) throws IOException {
        if (mAllocMode != ALLOC_RESERVE) {
            throw new IllegalStateException();
        }

        removeLock.lock();
        mReserveReclaimUpperBound = upperBound;

        while (true) {
            long pageId = tryRemove(removeLock);
            if (pageId == 0) {
                removeLock.unlock();
                break;
            }
            if (pageId <= upperBound) {
                mManager.deletePage(pageId);
            }
            removeLock.lock();
        }

        long pageId = mRemoveStoppedId;
        if (pageId != 0 && pageId <= upperBound) {
            mManager.deletePage(pageId);
        }
    }

    // Caller must hold remove lock.
    long getRemoveScanTarget() {
        return mRemovedNodeCounter + mRemoveNodeCount;
    }

    // Caller must hold remove lock.
    boolean isRemoveScanComplete(long target) {
        // Subtract for modulo comparison (not that it's really necessary).
        return (mRemovedNodeCounter - target) >= 0;
    }

    /**
     * Remove a page to satisfy an allocation request. Caller must acquire
     * remove lock, which might be released by this method.
     *
     * @param lock lock to be released by this method, unless return value is 0
     * @return 0 if queue is empty or if remaining pages are off limits
     */
    long tryRemove(Lock lock) throws IOException {
        if (mRemoveHeadId == 0) {
            if (!mAggressive || mRemoveStoppedId == mAppendTailId) {
                return 0;
            }
            // Can continue removing aggressively now that a new append tail exists.
            loadRemoveNode(mRemoveStoppedId);
            mRemoveStoppedId = 0;
        }

        long pageId;
        long oldHeadId;

        try {
            pageId = mRemoveHeadFirstPageId;

            if (mAllocMode != ALLOC_RESERVE && mManager.isPageOutOfBounds(pageId)) {
                throw new CorruptDatabaseException
                    ("Invalid page id in free list: " + pageId + "; list node: " + mRemoveHeadId);
            }

            mRemovePageCount--;

            final /*P*/ byte[] head = mRemoveHead;
            if (mRemoveHeadOffset < p_length(head)) {
                // Pass this as an IntegerRef to mRemoveHeadOffset.
                long delta = p_ulongGetVar(head, this);
                if (delta > 0) {
                    mRemoveHeadFirstPageId = pageId + delta;
                    return pageId;
                }
                // Zero delta is a terminator.
            }

            oldHeadId = mRemoveHeadId;

            if (mAllocMode == ALLOC_RESERVE && oldHeadId > mReserveReclaimUpperBound) {
                // Don't add to free list if not in the reclamation range.
                oldHeadId = 0;
            }

            // Move to the next node in the list.
            long nextId = p_longGetBE(head, I_NEXT_NODE_ID);

            if (nextId == (mAggressive ? mAppendTailId : mAppendHeadId)) {
                // Cannot remove from the append list. Those pages are off limits.
                mRemoveHeadId = 0;
                mRemoveHeadOffset = 0;
                mRemoveHeadFirstPageId = 0;
                mRemoveStoppedId = nextId;
            } else {
                loadRemoveNode(nextId);
            }

            mRemoveNodeCount--;
            mRemovedNodeCounter++;
        } finally {
            lock.unlock();
        }

        // Delete old head outside lock section to prevent deadlock with
        // commit. Lock acquisition order wouldn't match. Note that node is
        // deleted instead of used for next allocation. This ensures that no
        // important data is overwritten until after commit.
        if (oldHeadId != 0) {
            mManager.deletePage(oldHeadId);
        }

        return pageId;
    }

    // Caller must hold remove lock.
    private void loadRemoveNode(long id) throws IOException {
        if (mAllocMode != ALLOC_RESERVE && mManager.isPageOutOfBounds(id)) {
            throw new CorruptDatabaseException("Invalid node id in free list: " + id);
        }
        /*P*/ byte[] head = mRemoveHead;
        mManager.pageArray().readPage(id, head);
        mRemoveHeadId = id;
        mRemoveHeadOffset = I_NODE_START;
        mRemoveHeadFirstPageId = p_longGetBE(head, I_FIRST_PAGE_ID);
    }

    /**
     * Append a page which has been deleted.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    void append(long id) throws IOException {
        if (id <= 1) {
            throw new IllegalArgumentException("Page id: " + id);
        }

        final IdHeap appendHeap = mAppendHeap;

        mAppendLock.lock();
        try {
            appendHeap.add(id);
            mAppendPageCount++;
            if (!mDrainInProgress && appendHeap.shouldDrain()) {
                drainAppendHeap(appendHeap);
            }
            // If a drain is in progress, then append is called by allocPage
            // which is called by drainAppendHeap itself. The IdHeap has
            // padding for one more id, which gets drained when control returns
            // to the caller.
        } finally {
            mAppendLock.unlock();
        }
    }

    /**
     * Removes a page which was recently appended. To avoid deadlock, don't invoke with remove
     * lock held.
     *
     * @return 0 if none available
     */
    long tryUnappend() {
        mAppendLock.lock();
        try {
            final IdHeap appendHeap = mAppendHeap;
            if (mDrainInProgress && appendHeap.size() <= 1) {
                // Must leave at least one page, because drain will remove it.
                return 0;
            }
            long id = appendHeap.tryRemove();
            if (id != 0) {
                mAppendPageCount--;
            }
            return id;
        } finally {
            mAppendLock.unlock();
        }
    }

    // Caller must hold mAppendLock.
    private void drainAppendHeap(IdHeap appendHeap) throws IOException {
        if (mDrainInProgress) {
            throw new AssertionError();
        }

        mDrainInProgress = true;
        try {
            long newTailId = mManager.allocPage(mAllocMode);
            long firstPageId = appendHeap.remove();

            /*P*/ byte[] tailBuf = mAppendTail;
            p_longPutBE(tailBuf, I_NEXT_NODE_ID, newTailId);
            p_longPutBE(tailBuf, I_FIRST_PAGE_ID, firstPageId);

            int end = appendHeap.drain(firstPageId,
                                       tailBuf,
                                       I_NODE_START,
                                       p_length(tailBuf) - I_NODE_START);

            // Clean out any cruft from previous usage and ensure delta terminator.
            p_clear(tailBuf, end, p_length(tailBuf));

            mManager.pageArray().writePage(mAppendTailId, tailBuf);

            mAppendNodeCount++;
            mAppendTailId = newTailId;
        } finally {
            mDrainInProgress = false;
        }
    }

    Lock appendLock() {
        return mAppendLock;
    }

    /**
     * Caller must hold append and remove locks.
     */
    void preCommit() throws IOException {
        final IdHeap appendHeap = mAppendHeap;
        while (appendHeap.size() > 0) {
            // A new mAppendTailId is assigned as a side-effect.
            drainAppendHeap(appendHeap);
        }
    }

    /**
     * Caller must hold append and remove locks and have called preCommit.
     */
    void commitStart(/*P*/ byte[] header, int offset) {
        p_longPutLE(header, offset + I_REMOVE_PAGE_COUNT, mRemovePageCount + mAppendPageCount);
        p_longPutLE(header, offset + I_REMOVE_NODE_COUNT, mRemoveNodeCount + mAppendNodeCount);

        if (mRemoveHeadId == 0 && mAppendPageCount > 0) {
            long headId = mAppendHeadId;
            if (headId != mRemoveStoppedId) {
                // Aggressive recycling has consumed some or all of the append list.
                if (mRemoveStoppedId == mAppendTailId) {
                    // Entire append list has been consumed. With no remove list encoded, the
                    // init method will restore the proper stop position at the append tail.
                    headId = 0;
                } else {
                    // Append list has been partially consumed. The stop position will be
                    // detected when the append tail is seen.
                    headId = mRemoveStoppedId;
                }
            }

            p_longPutLE(header, offset + I_REMOVE_HEAD_ID, headId);
            p_intPutLE (header, offset + I_REMOVE_HEAD_OFFSET, I_NODE_START);
            // First page is defined in node itself, and init method reads it.
            p_longPutLE(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID, 0);
        } else {
            p_longPutLE(header, offset + I_REMOVE_HEAD_ID, mRemoveHeadId);
            p_intPutLE (header, offset + I_REMOVE_HEAD_OFFSET, mRemoveHeadOffset);
            p_longPutLE(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID, mRemoveHeadFirstPageId);
        }

        // Post-commit, all appended pages are eligible to be removed.
        p_longPutLE(header, offset + I_APPEND_HEAD_ID, mAppendTailId);

        // Increase counts now, but not all pages are not available until after
        // commitEnd is called.
        mRemovePageCount += mAppendPageCount;
        mRemoveNodeCount += mAppendNodeCount;

        mAppendPageCount = 0;
        mAppendNodeCount = 0;
    }

    /**
     * Caller must hold remove lock.
     *
     * @param header header with contents filled in by commitStart
     */
    void commitEnd(/*P*/ byte[] header, int offset) throws IOException {
        long newAppendHeadId = p_longGetLE(header, offset + I_APPEND_HEAD_ID);

        if (mRemoveHeadId == 0
            && mRemoveStoppedId != newAppendHeadId && mRemoveStoppedId != mAppendTailId)
        {
            // Allow removing of previously appended pages.
            loadRemoveNode(mRemoveStoppedId);
            mRemoveStoppedId = 0;
        }

        mAppendHeadId = newAppendHeadId;
    }

    /**
     * Caller must hold append and remove locks.
     */
    void addTo(PageDb.Stats stats) {
        stats.freePages +=
            mRemovePageCount + mAppendPageCount +
            mRemoveNodeCount + mAppendNodeCount;
    }

    /**
     * Scans all pages in the queue and checks if it matches the given range, assuming no
     * duplicates exist.
     */
    boolean verifyPageRange(long startId, long endId) throws IOException {
        // Be extra paranoid and use a hash for duplicate detection.
        long expectedHash = 0;
        for (long i=startId; i<endId; i++) {
            // Pages will not be observed in order, but addition is commutative. Note that xor
            // is not used here. If a page is triple counted, xor will not detect this.
            expectedHash += scramble(i);
        }

        long hash = 0;
        long count = 0;

        long nodeId = mRemoveHeadId;

        if (nodeId != 0) {
            /*P*/ byte[] node = p_clone(mRemoveHead);
            try {
                long pageId = mRemoveHeadFirstPageId;
                IntegerRef.Value nodeOffsetRef = new IntegerRef.Value();
                nodeOffsetRef.value = mRemoveHeadOffset;

                while (true) {
                    if (pageId < startId || pageId >= endId) {
                        // Out of bounds.
                        return false;
                    }

                    hash += scramble(pageId);
                    count++;

                    if (nodeOffsetRef.value < p_length(node)) {
                        long delta = p_ulongGetVar(node, nodeOffsetRef);
                        if (delta > 0) {
                            pageId += delta;
                            continue;
                        }
                    }

                    if (nodeId >= startId && nodeId < endId) {
                        // Count in-range queue nodes too.
                        hash += scramble(nodeId);
                        count++;
                    }

                    // Move to the next queue node.

                    nodeId = p_longGetBE(node, I_NEXT_NODE_ID);
                    if (nodeId == mAppendTailId) {
                        break;
                    }

                    mManager.pageArray().readPage(nodeId, node);
                    pageId = p_longGetBE(node, I_FIRST_PAGE_ID);
                    nodeOffsetRef.value = I_NODE_START;
                }
            } finally {
                p_delete(node);
            }
        }

        return hash == expectedHash && count == (endId - startId);
    }

    /**
     * Clears bits representing all removable pages in the queue. Caller must
     * hold remove lock.
     */
    int traceRemovablePages(BitSet pages) throws IOException {
        int count = 0;

        // Even though not removable, also clear append head. Otherwise, it
        // gives the impression that one page is missing, even after startup.
        long nodeId = mAppendHeadId;
        if (nodeId < mManager.pageArray().getPageCount()) {
            count++;
            clearPageBit(pages, nodeId);
        }

        nodeId = mRemoveHeadId;

        if (nodeId == 0) {
            return count;
        }

        /*P*/ byte[] node = p_clone(mRemoveHead);
        try {
            long pageId = mRemoveHeadFirstPageId;
            IntegerRef.Value nodeOffsetRef = new IntegerRef.Value();
            nodeOffsetRef.value = mRemoveHeadOffset;

            while (true) {
                /*
                  if (mManager.isPageOutOfBounds(pageId)) {
                  throw new CorruptDatabaseException("Invalid page id in free list: " + pageId);
                  }
                */

                count++;
                clearPageBit(pages, pageId);

                if (nodeOffsetRef.value < p_length(node)) {
                    long delta = p_ulongGetVar(node, nodeOffsetRef);
                    if (delta > 0) {
                        pageId += delta;
                        continue;
                    }
                }

                // Indicate free list node itself as free and move to the next node
                // in the free list.

                count++;
                clearPageBit(pages, nodeId);

                nodeId = p_longGetBE(node, I_NEXT_NODE_ID);
                if (nodeId == mAppendHeadId || nodeId == mAppendTailId) {
                    break;
                }

                mManager.pageArray().readPage(nodeId, node);
                pageId = p_longGetBE(node, I_FIRST_PAGE_ID);
                nodeOffsetRef.value = I_NODE_START;
            }
        } finally {
            p_delete(node);
        }

        return count;
    }

    private static void clearPageBit(BitSet pages, long pageId) throws CorruptDatabaseException {
        int index = (int) pageId;
        if (pages.get(index)) {
            pages.clear(index);
        } else if (index < pages.size()) {
            throw new CorruptDatabaseException("Doubly freed page: " + pageId);
        }
    }

    // Required by IntegerRef.
    @Override
    public int get() {
        return mRemoveHeadOffset;
    }

    // Required by IntegerRef.
    @Override
    public void set(int offset) {
        mRemoveHeadOffset = offset;
    }
}
