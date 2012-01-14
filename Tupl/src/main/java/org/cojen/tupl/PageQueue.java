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

import java.io.IOException;

import java.util.Arrays;
import java.util.BitSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    // These fields are guarded by remove lock provided by caller.
    private long mRemovePageCount;
    private long mRemoveNodeCount;
    private final byte[] mRemoveHead;
    private long mRemoveHeadId;
    private int mRemoveHeadOffset;
    private long mRemoveHeadFirstPageId;
    private long mRemoveStoppedId;

    // Barrier between the remove and append lists. Remove stops when it
    // encounters the append head. Modification is permitted with the append
    // lock held, but no lock is required to access this id. Hence, volatile.
    private volatile long mAppendHeadId;

    // These fields are guarded by mAppendLock.
    private final ReentrantLock mAppendLock;
    private final IdHeap mAppendHeap;
    private final byte[] mAppendTail;
    private long mAppendTailId;
    private long mAppendPageCount;
    private long mAppendNodeCount;
    private boolean mDrainInProgress;

    /**
     * @param manager used for allocating and deleting pages for the queue itself
     */
    PageQueue(PageManager manager) {
        mManager = manager;
        PageArray array = manager.pageArray();

        mRemoveHead = new byte[array.pageSize()];

        // This lock must be reentrant. The appendPage method can call into
        // drainAppendHeap, which calls allocPage, which can re-acquire the
        // append lock. The commit method acquires the remove lock too, and
        // then it calls drainAppendHeap, acquiring the locks again. Note
        // that locks are unfair. Pages are allocated and deleted by tree
        // operations, which unfairly acquire latches. Waiting for a fair lock
        // afterwards leads to priority inversion.
        mAppendLock = new ReentrantLock(false);

        mAppendHeap = new IdHeap(array.pageSize() - I_NODE_START);
        mAppendTail = new byte[array.pageSize()];
    }

    /**
     * Initialize a fresh (non-restored) queue.
     */
    void init() throws IOException {
        mAppendLock.lock();
        try {
            mRemoveStoppedId = mAppendHeadId = mAppendTailId = mManager.allocPage();
        } finally {
            mAppendLock.unlock();
        }
    }

    /**
     * Initialize a restored queue. Caller must hold append and remove locks.
     */
    void init(byte[] header, int offset) throws IOException {
        mRemovePageCount = DataIO.readLong(header, offset + I_REMOVE_PAGE_COUNT);
        mRemoveNodeCount = DataIO.readLong(header, offset + I_REMOVE_NODE_COUNT);

        mRemoveHeadId = DataIO.readLong(header, offset + I_REMOVE_HEAD_ID);
        mRemoveHeadOffset = DataIO.readInt(header, offset + I_REMOVE_HEAD_OFFSET);
        mRemoveHeadFirstPageId = DataIO.readLong(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID);

        mAppendHeadId = mAppendTailId = DataIO.readLong(header, offset + I_APPEND_HEAD_ID);

        if (mRemoveHeadId == 0) {
            mRemoveStoppedId = mAppendHeadId;
        } else {
            mManager.pageArray().readPage(mRemoveHeadId, mRemoveHead);
            if (mRemoveHeadFirstPageId == 0) {
                mRemoveHeadFirstPageId = DataIO.readLong(mRemoveHead, I_FIRST_PAGE_ID);
            }
        }
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
            return 0;
        }

        long pageId;
        long oldHeadId;

        try {
            pageId = mRemoveHeadFirstPageId;

            if (mManager.isPageOutOfBounds(pageId)) {
                throw new CorruptPageStoreException("Invalid page id in free list: " + pageId);
            }

            mRemovePageCount--;

            final byte[] head = mRemoveHead;
            if (mRemoveHeadOffset < head.length) {
                // Pass this as an IntegerRef to mRemoveHeadOffset.
                long delta = DataIO.readUnsignedVarLong(head, this);
                if (delta > 0) {
                    mRemoveHeadFirstPageId = pageId + delta;
                    return pageId;
                }
                // Zero delta is a terminator.
            }

            oldHeadId = mRemoveHeadId;

            // Move to the next node in the list.
            long nextId = DataIO.readLong(head, I_NEXT_NODE_ID);

            if (nextId == mAppendHeadId) {
                // Cannot remove from the append list. Those pages are off limits.
                mRemoveHeadId = 0;
                mRemoveHeadOffset = 0;
                mRemoveHeadFirstPageId = 0;
                mRemoveStoppedId = nextId;
            } else {
                if (mManager.isPageOutOfBounds(nextId)) {
                    throw new CorruptPageStoreException("Invalid node id in free list: " + nextId);
                }
                mManager.pageArray().readPage(nextId, head);
                mRemoveHeadId = nextId;
                mRemoveHeadOffset = I_NODE_START;
                mRemoveHeadFirstPageId = DataIO.readLong(head, I_FIRST_PAGE_ID);
            }

            mRemoveNodeCount--;
        } finally {
            lock.unlock();
        }

        // Delete old head outside lock section to prevent deadlock with
        // commit. Lock acquisition order wouldn't match. Note that node is
        // deleted instead of used for next allocation. This ensures that no
        // important data is overwritten until after commit.
        mManager.deletePage(oldHeadId);

        return pageId;
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
     * Removes a page which was recently appended.
     *
     * @return 0 if none available
     */
    long tryUnappend() {
        mAppendLock.lock();
        try {
            return mAppendHeap.tryRemove();
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
            long newTailId = mManager.allocPage();
            long firstPageId = appendHeap.remove();

            byte[] tailBuf = mAppendTail;
            DataIO.writeLong(tailBuf, I_NEXT_NODE_ID, newTailId);
            DataIO.writeLong(tailBuf, I_FIRST_PAGE_ID, firstPageId);

            int end = appendHeap.drain(firstPageId,
                                       tailBuf,
                                       I_NODE_START,
                                       tailBuf.length - I_NODE_START);

            // Clean out any cruft from previous usage and ensure delta terminator.
            Arrays.fill(tailBuf, end, tailBuf.length, (byte) 0);

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
    void commitStart(byte[] header, int offset) throws IOException {
        final IdHeap appendHeap = mAppendHeap;

        while (appendHeap.size() > 0) {
            // A new mAppendTailId is assigned as a side-effect.
            drainAppendHeap(appendHeap);
        }

        DataIO.writeLong(header, offset+I_REMOVE_PAGE_COUNT, mRemovePageCount + mAppendPageCount);
        DataIO.writeLong(header, offset+I_REMOVE_NODE_COUNT, mRemoveNodeCount + mAppendNodeCount);

        if (mRemoveHeadId == 0 && mAppendPageCount > 0) {
            DataIO.writeLong(header, offset + I_REMOVE_HEAD_ID, mAppendHeadId);
            DataIO.writeInt (header, offset + I_REMOVE_HEAD_OFFSET, I_NODE_START);
            // First page is defined in node itself, and init method reads it.
            DataIO.writeLong(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID, 0);
        } else {
            DataIO.writeLong(header, offset + I_REMOVE_HEAD_ID, mRemoveHeadId);
            DataIO.writeInt (header, offset + I_REMOVE_HEAD_OFFSET, mRemoveHeadOffset);
            DataIO.writeLong(header, offset + I_REMOVE_HEAD_FIRST_PAGE_ID, mRemoveHeadFirstPageId);
        }

        // Post-commit, all appended pages are eligible to be removed.
        DataIO.writeLong(header, offset + I_APPEND_HEAD_ID, mAppendTailId);

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
    void commitEnd(byte[] header, int offset) throws IOException {
        long newAppendHeadId = DataIO.readLong(header, offset + I_APPEND_HEAD_ID);

        if (mRemoveHeadId == 0 && mRemoveStoppedId != newAppendHeadId) {
            // Allow removing of previously appended pages.
            mManager.pageArray().readPage(mRemoveStoppedId, mRemoveHead);
            mRemoveHeadId = mRemoveStoppedId;
            mRemoveHeadOffset = I_NODE_START;
            mRemoveHeadFirstPageId = DataIO.readLong(mRemoveHead, I_FIRST_PAGE_ID);
            mRemoveStoppedId = 0;
        }

        mAppendHeadId = newAppendHeadId;
    }

    /**
     * Caller must hold remove lock.
     */
    void addTo(PageStore.Stats stats) {
        stats.freePages += mRemovePageCount + mRemoveNodeCount;
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

        class NodeOffsetRef implements IntegerRef {
            int offset;
            public int get() {
                return offset;
            }
            public void set(int v) {
                offset = v;
            }
        }

        NodeOffsetRef nodeOffsetRef = new NodeOffsetRef();

        byte[] node = mRemoveHead.clone();
        long pageId = mRemoveHeadFirstPageId;
        nodeOffsetRef.offset = mRemoveHeadOffset;

        while (true) {
            /*
            if (isPageOutOfBounds(pageId)) {
                throw new CorruptPageStoreException("Invalid page id in free list: " + pageId);
            }
            */

            count++;
            clearPageBit(pages, pageId);

            if (nodeOffsetRef.offset < node.length) {
                long delta = DataIO.readUnsignedVarLong(node, nodeOffsetRef);
                if (delta > 0) {
                    pageId += delta;
                    continue;
                }
            }

            // Indicate free list node itself as free and move to the next node
            // in the free list.

            count++;
            clearPageBit(pages, nodeId);

            nodeId = DataIO.readLong(node, I_NEXT_NODE_ID);
            if (nodeId == mAppendHeadId) {
                break;
            }

            mManager.pageArray().readPage(nodeId, node);
            pageId = DataIO.readLong(node, I_FIRST_PAGE_ID);
            nodeOffsetRef.offset = I_NODE_START;
        }

        return count;
    }

    private static void clearPageBit(BitSet pages, long pageId) throws CorruptPageStoreException {
        int index = (int) pageId;
        if (pages.get(index)) {
            pages.clear(index);
        } else if (index < pages.size()) {
            throw new CorruptPageStoreException("Doubly freed page: " + pageId);
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
