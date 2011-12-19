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
 * Manages free and deleted pages.
 *
 * @author Brian S O'Neill
 */
final class PageManager {
    /*

    Header structure is encoded as follows, in 64 bytes:

    +-----------------------------------------------------------+
    | int:  reserved; must be zero                              |
    | long: total page count                                    |
    | long: free page count                                     |
    | long: free list page count                                |
    | long: free list head id                                   |
    | int:  free List head offset                               |
    | long: free list head first free page id (seed for deltas) |
    | long: last free list node id                              |
    | long: deleted list tail id                                |
    +-----------------------------------------------------------+

    The free list stores available page ids, in a queue. Deleted page ids are
    appended to the tail, and allocated pages are dequeued from the head.
    Deleted pages cannot be used for allocations until after a commit.

    +--------------------------------------------+
    | long: next free list node id               |
    | long: first free page id (seed for deltas) |
    +--------------------------------------------+
    | remaining page ids (delta encoded)         |
    -                                            -
    |                                            |
    +--------------------------------------------+

    */

    // Indexes of entries in header.
    private static final int I_TOTAL_PAGE_COUNT             = 4;
    private static final int I_FREE_PAGE_COUNT              = I_TOTAL_PAGE_COUNT + 8;
    private static final int I_FREE_LIST_PAGE_COUNT         = I_FREE_PAGE_COUNT + 8;
    private static final int I_FREE_LIST_HEAD_ID            = I_FREE_LIST_PAGE_COUNT + 8;
    private static final int I_FREE_LIST_HEAD_OFFSET        = I_FREE_LIST_HEAD_ID + 8;
    private static final int I_FREE_LIST_HEAD_FIRST_PAGE_ID = I_FREE_LIST_HEAD_OFFSET + 4;
    private static final int I_LAST_FREE_LIST_ID            = I_FREE_LIST_HEAD_FIRST_PAGE_ID + 8;
    private static final int I_DELETED_LIST_TAIL_ID         = I_LAST_FREE_LIST_ID + 8;
    private static final int HEADER_SIZE                    = I_DELETED_LIST_TAIL_ID + 8;

    // Indexes of entries in free list node.
    private static final int I_NEXT_FREE_LIST_ID  = 0;
    private static final int I_FIRST_FREE_PAGE_ID = I_NEXT_FREE_LIST_ID + 8;
    private static final int I_FREE_NODE_START    = I_FIRST_FREE_PAGE_ID + 8;

    private final PageArray mPageArray;

    // These fields are guarded by mAllocLock.
    private final ReentrantLock mAllocLock;
    private long mTotalPageCount;
    private long mFreePageCount;
    private long mFreeListPageCount;
    private final byte[] mFreeListHead;
    private long mFreeListHeadId;
    private final IntegerRef mFreeListHeadOffsetRef;
    int mFreeListHeadOffset;
    private long mFreeListHeadFirstPageId;
    private long mLastFreeListNodeId;
    private long mStoppedFreeListNodeId;

    // These fields are guarded by mDeleteLock.
    private final ReentrantLock mDeleteLock;
    private final IdHeap mDeletedIds;
    private final byte[] mDeletedListTail;
    private long mDeletedListTailId;
    private long mDeletedPageCount;
    private long mDeletedListPageCount;
    private boolean mDrainInProgress;

    /**
     * Create a new PageManager.
     */
    PageManager(PageArray array) throws IOException {
        this(false, array, null, 0);
    }

    /**
     * Create a restored PageManager.
     *
     * @param header source for reading allocator root structure
     * @param offset offset into header
     */
    PageManager(PageArray array, byte[] header, int offset) throws IOException {
        this(true, array, header, offset);
    }

    private PageManager(boolean restored, PageArray array, byte[] header, int offset)
        throws IOException
    {
        if (array == null) {
            throw new IllegalArgumentException("PageArray is null");
        }

        byte[] freeListHead = new byte[array.pageSize()];

        long totalPageCount;
        long freePageCount;
        long freeListPageCount;
        long freeListHeadId;
        int  freeListHeadOffset;
        long freeListHeadFirstPageId;
        long lastFreeListNodeId;
        long deletedListTailId;

        if (!restored) {
            // Pages 0 and 1 are reserved.
            totalPageCount = 2;
            freePageCount = 0;
            freeListPageCount = 0;
            freeListHeadId = 0;
            freeListHeadOffset = 0;
            freeListHeadFirstPageId = 0;
            lastFreeListNodeId = 0;
            deletedListTailId = 0;
        } else {
            totalPageCount = DataIO.readLong(header, offset + I_TOTAL_PAGE_COUNT);
            deletedListTailId = DataIO.readLong(header, offset + I_DELETED_LIST_TAIL_ID);

            long actualPageCount = array.getPageCount();
            if (actualPageCount > totalPageCount) {
                if (!array.isReadOnly()) {
                    // Truncate extra uncommitted pages.
                    //System.out.println("Page count is too large: "
                    //                   + actualPageCount + " > " + totalPageCount);
                    array.setPageCount(totalPageCount, false);
                }
            } else if (actualPageCount < totalPageCount) {
                // Mismatch might be caused by reserved deleted list tail.
                if ((deletedListTailId + 1) != totalPageCount ||
                    (totalPageCount - actualPageCount) != 1)
                {
                    throw new CorruptPageStoreException
                        ("Page count is too small: " + actualPageCount + " < " + totalPageCount);
                }
            }

            freePageCount = DataIO.readLong(header, offset + I_FREE_PAGE_COUNT);
            freeListPageCount = DataIO.readLong(header, offset + I_FREE_LIST_PAGE_COUNT);

            freeListHeadId = DataIO.readLong(header, offset + I_FREE_LIST_HEAD_ID);
            if (freeListHeadId == 0) {
                freeListHeadOffset = 0;
                freeListHeadFirstPageId = 0;
            } else {
                freeListHeadOffset = DataIO.readInt(header, offset + I_FREE_LIST_HEAD_OFFSET);
                freeListHeadFirstPageId =
                    DataIO.readLong(header, offset + I_FREE_LIST_HEAD_FIRST_PAGE_ID);
                array.readPage(freeListHeadId, freeListHead);
            }

            lastFreeListNodeId = DataIO.readLong(header, offset + I_LAST_FREE_LIST_ID);
        }

        mPageArray = array;

        // These locks must be reentrant. The deletePage method can call into
        // drainDeletedIds, which calls allocPage, which can re-acquire the
        // delete lock. The commit method acquires both locks, and then it
        // calls drainDeletedIds, acquiring the locks again. Note that locks
        // are unfair. Pages are allocated and deleted by tree operations,
        // which unfairly acquire latches. Waiting for a fair lock afterwards
        // leads to priority inversion.
        mAllocLock = new ReentrantLock(false);
        mDeleteLock = new ReentrantLock(false);

        fullLock();
        try {
            mTotalPageCount = totalPageCount;
            mFreePageCount = freePageCount;
            mFreeListPageCount = freeListPageCount;
            mFreeListHead = freeListHead;
            mFreeListHeadId = freeListHeadId;
            mFreeListHeadOffsetRef = new IntegerRef() {
                public int get() {
                    return mFreeListHeadOffset;
                }
                public void set(int offset) {
                    mFreeListHeadOffset = offset;
                }
            };
            mFreeListHeadOffset = freeListHeadOffset;
            mFreeListHeadFirstPageId = freeListHeadFirstPageId;
            mLastFreeListNodeId = lastFreeListNodeId;

            mDeletedIds = new IdHeap(array.pageSize() - I_FREE_NODE_START);
            mDeletedListTail = new byte[array.pageSize()];

            if (deletedListTailId == 0) {
                deletedListTailId = createPage(false);
            }

            mDeletedListTailId = deletedListTailId;
        } finally {
            fullUnlock();
        }
    }

    public int headerSize() {
        return HEADER_SIZE;
    }

    public PageArray pageArray() {
        return mPageArray;
    }

    /**
     * Allocates a page from the free list or by growing the underlying page
     * array. No page allocations are permanent until after commit is called.
     *
     * @param grow hint to ensure space is allocated for larger count
     * @return non-zero page id
     */
    public long allocPage(boolean grow) throws IOException {
        long pageId;
        long oldFreeListHeadId;

        mAllocLock.lock();
        try {
            if (mFreeListHeadId == 0) {
                return createPage(grow);
            }

            pageId = mFreeListHeadFirstPageId;

            if (isPageOutOfBounds(pageId)) {
                throw new CorruptPageStoreException("Invalid page id in free list: " + pageId);
            }

            mFreePageCount--;

            final byte[] freeListHead = mFreeListHead;
            if (mFreeListHeadOffset < freeListHead.length) {
                long delta = DataIO.readUnsignedVarLong(freeListHead, mFreeListHeadOffsetRef);
                if (delta > 0) {
                    mFreeListHeadFirstPageId = pageId + delta;
                    return pageId;
                }
                // Zero delta is a terminator.
            }

            oldFreeListHeadId = mFreeListHeadId;

            if (oldFreeListHeadId == mLastFreeListNodeId) {
                // Remaining free pages are off limits until after commit.
                mStoppedFreeListNodeId = oldFreeListHeadId;
                mFreeListHeadId = 0;
                mFreeListHeadOffset = 0;
                mFreeListHeadFirstPageId = 0;
            } else {
                // Move to the next node in the free list.
                long nextId = DataIO.readLong(freeListHead, I_NEXT_FREE_LIST_ID);
                if (isPageOutOfBounds(nextId)) {
                    throw new CorruptPageStoreException("Invalid node id in free list: " + nextId);
                }
                mPageArray.readPage(nextId, freeListHead);
                mFreeListHeadId = nextId;
                mFreeListHeadOffset = I_FREE_NODE_START;
                mFreeListHeadFirstPageId = DataIO.readLong(freeListHead, I_FIRST_FREE_PAGE_ID);
            }

            mFreeListPageCount--;
        } finally {
            mAllocLock.unlock();
        }

        // Delete old free list node outside allocation lock to prevent
        // deadlock with commit. Lock acquisition order wouldn't match. Note
        // that node is deleted instead of used for next allocation. This
        // ensures that no important data is overwritten until after commit.
        deletePage(oldFreeListHeadId);

        return pageId;
    }

    /**
     * Deletes a page to be reused after commit is called. No page deletions
     * are permanent until after commit is called.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void deletePage(long id) throws IOException {
        if (id <= 1) {
            throw new IllegalArgumentException("Page id: " + id);
        }

        final IdHeap deletedIds = mDeletedIds;

        mDeleteLock.lock();
        try {
            deletedIds.add(id);
            mDeletedPageCount++;
            if (!mDrainInProgress && deletedIds.shouldDrain()) {
                drainDeletedIds(deletedIds);
            }
            // If a drain is in progress, then deletePage is called by
            // allocPage which is called by drainDeletedIds itself. The IdHeap
            // has padding for one more id, which gets drained when control
            // returns to the caller.
        } finally {
            mDeleteLock.unlock();
        }
    }

    /**
     * Preallocates pages for use later. Preallocation is not permanent until
     * after commit is called.
     */
    public void preallocate(long pageCount) throws IOException {
        if (pageCount <= 0) {
            return;
        }

        mAllocLock.lock();
        try {
            pageCount -= (mFreePageCount + mFreeListPageCount);
        } finally {
            mAllocLock.unlock();
        }

        while (--pageCount >= 0) {
            long pageId;
            mAllocLock.lock();
            try {
                pageId = createPage(pageCount == 0);
            } finally {
                mAllocLock.unlock();
            }
            deletePage(pageId);
        }
    }

    /**
     * Makes changes permanent for page allocations and deletions. The
     * underlying page array is not flushed, however.
     *
     * @param header destination for writing allocator root structure
     * @param offset offset into header
     * @param ready if provided, invoked during the commit operation
     */
    public void commit(byte[] header, int offset, CommitReady ready) throws IOException {
        // Values to commit.
        final long totalPageCount;
        final long freePageCount;
        final long freeListPageCount;
        final long freeListHeadId;
        final int  freeListHeadOffset;
        final long freeListHeadFirstPageId;
        final long lastFreeListNodeId;
        final long deletedListTailId;

        final long deletedPageCount;
        final long deletedListPageCount;

        final IdHeap deletedIds = mDeletedIds;

        fullLock();
        try {
            if (deletedIds.size() == 0) {
                // No deleted pages since last commit.
                lastFreeListNodeId = mLastFreeListNodeId;
                deletedListTailId = mDeletedListTailId;
            } else {
                lastFreeListNodeId = mDeletedListTailId;
                do {
                    // A new mDeletedListTailId is assigned as a side-effect.
                    drainDeletedIds(deletedIds);
                } while (deletedIds.size() > 0);
                deletedListTailId = mDeletedListTailId;
            }

            freeListHeadId = mFreeListHeadId;
            freeListHeadOffset = mFreeListHeadOffset;
            freeListHeadFirstPageId = mFreeListHeadFirstPageId;

            deletedPageCount = mDeletedPageCount;
            deletedListPageCount = mDeletedListPageCount;

            totalPageCount = mTotalPageCount;
            freePageCount = mFreePageCount + deletedPageCount;
            freeListPageCount = mFreeListPageCount + deletedListPageCount;

            if (totalPageCount <= 1) {
                throw new CorruptPageStoreException
                    ("Illegal total page count: " + totalPageCount);
            }

            if (freePageCount < 0) {
                throw new CorruptPageStoreException
                    ("Illegal free list page count: " + totalPageCount);
            }

            if (freeListPageCount < 0) {
                throw new CorruptPageStoreException
                    ("Illegal free list page count: " + freeListPageCount);
            }

            /* FIXME
            if (freeListPageCount == 0) {
                // Not strictly required to be zero, but they should be.
                if (freeListHeadOffset != 0) {
                    throw new CorruptPageStoreException
                        ("Free list page offset should be zero: " + freeListHeadOffset);
                }
                if (freeListHeadFirstPageId != 0) {
                    throw new CorruptPageStoreException
                        ("Free list first page id should be zero: " + freeListHeadFirstPageId);
                }
            } else {
                if (isPageOutOfBounds(freeListHeadId)) {
                    throw new CorruptPageStoreException
                        ("Illegal free list page id: " + freeListHeadId);
                }
                if (freeListHeadOffset < I_FREE_NODE_START) {
                    throw new CorruptPageStoreException
                        ("Illegal free list page offset: " + freeListHeadOffset);
                }
                if (isPageOutOfBounds(freeListHeadFirstPageId)) {
                    throw new CorruptPageStoreException
                        ("Illegal free list first page id: " + freeListHeadFirstPageId);
                }
            }
            */

            mDeletedPageCount = 0;
            mDeletedListPageCount = 0;
        } finally {
            fullUnlock();
        }
        
        DataIO.writeInt (header, offset, 0); // reserved
        DataIO.writeLong(header, offset + I_TOTAL_PAGE_COUNT, totalPageCount);
        DataIO.writeLong(header, offset + I_FREE_PAGE_COUNT, freePageCount);
        DataIO.writeLong(header, offset + I_FREE_LIST_PAGE_COUNT, freeListPageCount);
        DataIO.writeLong(header, offset + I_FREE_LIST_HEAD_ID, freeListHeadId);
        DataIO.writeInt (header, offset + I_FREE_LIST_HEAD_OFFSET, freeListHeadOffset);
        DataIO.writeLong(header, offset + I_FREE_LIST_HEAD_FIRST_PAGE_ID, freeListHeadFirstPageId);
        DataIO.writeLong(header, offset + I_LAST_FREE_LIST_ID, lastFreeListNodeId);
        DataIO.writeLong(header, offset + I_DELETED_LIST_TAIL_ID, deletedListTailId);

        Object state = ready == null ? null : ready.started();

        fullLock();
        try {
            if (ready != null) {
                ready.finished(state, offset + HEADER_SIZE);
            }

            if (mFreeListHeadId == 0 && mLastFreeListNodeId != 0) {
                // Allow allocations of previously deleted pages.
                byte[] freeListHead = mFreeListHead;
                if (mStoppedFreeListNodeId != mLastFreeListNodeId) {
                    mPageArray.readPage(mLastFreeListNodeId, freeListHead);
                }
                long nextId = DataIO.readLong(freeListHead, I_NEXT_FREE_LIST_ID);
                if (isPageOutOfBounds(nextId)) {
                    throw new CorruptPageStoreException("Invalid node id in free list: " + nextId);
                }
                mPageArray.readPage(nextId, freeListHead);
                mFreeListHeadId = nextId;
                mFreeListHeadOffset = I_FREE_NODE_START;
                mFreeListHeadFirstPageId = DataIO.readLong(freeListHead, I_FIRST_FREE_PAGE_ID);
                mStoppedFreeListNodeId = 0;
            }

            mFreePageCount += deletedPageCount;
            mFreeListPageCount += deletedListPageCount;

            mLastFreeListNodeId = lastFreeListNodeId;
        } finally {
            fullUnlock();
        }
    }

    private void fullLock() {
        // Avoid deadlock by acquiring delete lock before allocation lock. This
        // matches the lock order used by the deletePage method, which might
        // call allocPage, which in turn acquires allocation lock.
        mDeleteLock.lock();
        mAllocLock.lock();
    }

    private void fullUnlock() {
        mAllocLock.unlock();
        mDeleteLock.unlock();
    }

    // Caller must hold delete lock.
    private void drainDeletedIds(IdHeap deletedIds) throws IOException {
        if (mDrainInProgress) {
            throw new AssertionError();
        }

        mDrainInProgress = true;
        try {
            long newTailId = allocPage(false);
            long firstPageId = deletedIds.remove();

            byte[] tailBuf = mDeletedListTail;
            DataIO.writeLong(tailBuf, I_NEXT_FREE_LIST_ID, newTailId);
            DataIO.writeLong(tailBuf, I_FIRST_FREE_PAGE_ID, firstPageId);

            int end = deletedIds.drain(firstPageId,
                                       tailBuf,
                                       I_FREE_NODE_START,
                                       tailBuf.length - I_FREE_NODE_START);

            // Clean out any cruft from previous usage and ensure delta terminator.
            Arrays.fill(tailBuf, end, tailBuf.length, (byte) 0);

            mPageArray.writePage(mDeletedListTailId, tailBuf);

            mDeletedListPageCount++;
            mDeletedListTailId = newTailId;
        } finally {
            mDrainInProgress = false;
        }
    }

    void addTo(PageStore.Stats stats) {
        mAllocLock.lock();
        try {
            stats.totalPages += mTotalPageCount;
            stats.freePages += mFreePageCount + mFreeListPageCount;
        } finally {
            mAllocLock.unlock();
        }
    }

    /**
     * Sets a bit for each page.
     */
    void markAllPages(BitSet pages, int scalar, int offset) throws IOException {
        mAllocLock.lock();
        try {
            int limit = (Integer.MAX_VALUE - offset) / scalar;
            int total = (int) Math.min(mTotalPageCount, limit);
            for (int i=0; i<total; i++) {
                pages.set(i * scalar + offset);
            }
        } finally {
            mAllocLock.unlock();
        }
    }

    /**
     * Clears bits representing pages which are in the free list. Pages in the
     * deleted list are not traced.
     */
    int traceFreePages(BitSet pages,
                       int scalar, int offset,
                       int fnScalar, int fnOffset)
        throws IOException
    {
        int count = 0;

        /* FIXME
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

        mAllocLock.lock();
        try {
            long nodeId = mFreeListHeadId;
            if (nodeId == 0) {
                return count;
            }

            byte[] node = mFreeListHead.clone();
            long pageId = mFreeListHeadFirstPageId;
            nodeOffsetRef.offset = mFreeListHeadOffset;

            while (true) {
                if (isPageOutOfBounds(pageId)) {
                    throw new CorruptPageStoreException
                        ("Invalid page id in free list: " + (pageId * scalar + offset));
                }

                count++;
                clearPageBit(pages, pageId, scalar, offset);

                if (nodeOffsetRef.offset < node.length) {
                    long delta = DataIO.readUnsignedVarLong(node, nodeOffsetRef);
                    if (delta > 0) {
                        pageId += delta;
                        continue;
                    }
                }

                // Indicate free list node itself as free and move to the next
                // node in the free list.

                count++;
                clearPageBit(pages, nodeId, fnScalar, fnOffset);
                nodeId = DataIO.readLong(node, I_NEXT_FREE_LIST_ID);
                if (nodeId == 0) {
                    break;
                }
                nodeOffsetRef.offset = DataIO.readInt(node, I_NEXT_FREE_OFFSET);
                pageId = DataIO.readLong(node, I_NEXT_FREE_FIRST_PAGE_ID);
                mPageArray.readPage(nodeId, node);
            }
        } finally {
            mAllocLock.unlock();
        }
        */

        return count;
    }

    private static void clearPageBit(BitSet pages, long pageId, int scalar, int offset)
        throws CorruptPageStoreException
    {
        pageId = pageId * scalar + offset;
        int index = (int) pageId;
        if (pages.get(index)) {
            pages.clear(index);
        } else {
            if (index < pages.size()) {
                throw new CorruptPageStoreException("Doubly freed page: " + pageId);
            }
        }
    }

    /**
     * Expand the underlying page store to create a new page. Method is
     * invoked with allocation lock held.
     *
     * @param grow hint to ensure space is allocated for larger count
     */
    private long createPage(boolean grow) throws IOException {
        long id = mTotalPageCount++;
        mPageArray.setPageCount(id + 1, grow);
        return id;
    }

    /**
     * Method is invoked with allocation lock held.
     */
    private boolean isPageOutOfBounds(long id) {
        return id <= 1 || id >= mTotalPageCount;
    }

    static interface CommitReady {
        /**
         * Called when commit is underway. Implementation is expected to flush
         * all unwritten pages. No locks are held during this time, and so
         * pages can continue to be allocated and deleted. These allocations
         * and deletions will not take effect until the next commit.
         *
         * @return optional state to pass into finished method
         */
        public Object started() throws IOException;

        /**
         * Called after header content has been prepared. Implementation is
         * expected to only write the header. This method is invoked with locks
         * held, preventing page allocation and deletion. Although current
         * thread owns these locks, implementation of this method must not
         * allocate or delete pages.
         *
         * @param state state returned by started method
         * @param offset new header offset
         */
        public void finished(Object state, int offset) throws IOException;
    }
}
