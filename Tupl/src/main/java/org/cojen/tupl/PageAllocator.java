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

import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides allocation management features for a page array.
 *
 * @author Brian S O'Neill
 */
class PageAllocator {
    /*

    Root structure is encoded as follows, in 48 bytes:

    +------------------------------------------------------------+
    | int:  reserved; must be zero                               |
    | long: total page count                                     |
    | long: free page count                                      |
    | long: free list page count                                 |
    | long: first free list node id                              |
    | int:  first free node offset                               |
    | long: first free node first free page id (seed for deltas) |
    +------------------------------------------------------------+

    The total page count is encoded in the header, which is used to truncate
    the file if uncommitted pages were allocated at the end.

    The free list stores available page ids. Deleted page ids are appended to a
    new free list, in queue order. At commit, the new free list is stitched to
    the existing free list in stack order. This design allows the free list to
    be updated without ever updating existing free list nodes.

    +-----------------------------------------------------------+
    | long: next free list node id                              |
    | int:  next free node offset                               |
    | long: next free node first free page id (seed for deltas) |
    +-----------------------------------------------------------+
    | remaining free page ids (delta encoded)                   |
    -                                                           -
    |                                                           |
    +-----------------------------------------------------------+

    */

    // Indexes of entries in header.
    private static final int I_TOTAL_PAGE_COUNT = 4;
    private static final int I_FREE_PAGE_COUNT = I_TOTAL_PAGE_COUNT + 8;
    private static final int I_FREE_LIST_PAGE_COUNT = I_FREE_PAGE_COUNT + 8;
    private static final int I_FIRST_FREE_LIST_ID = I_FREE_LIST_PAGE_COUNT + 8;
    private static final int I_FIRST_FREE_OFFSET = I_FIRST_FREE_LIST_ID + 8;
    private static final int I_FIRST_FREE_FIRST_PAGE_ID = I_FIRST_FREE_OFFSET + 4;
    private static final int HEADER_SIZE = I_FIRST_FREE_FIRST_PAGE_ID + 8;

    // Indexes of entries in free list node.
    private static final int I_NEXT_FREE_LIST_ID = 0;
    private static final int I_NEXT_FREE_OFFSET = I_NEXT_FREE_LIST_ID + 8;
    private static final int I_NEXT_FREE_FIRST_PAGE_ID = I_NEXT_FREE_OFFSET + 4;
    private static final int I_FREE_NODE_START = I_NEXT_FREE_FIRST_PAGE_ID + 8;

    private final PageArray mPageArray;

    // These fields are guarded by mAllocLock.
    private final ReentrantLock mAllocLock;
    private final byte[] mFreeListHead;
    private long mFreeListHeadId;
    private final IntegerRef mFreeListHeadOffsetRef;
    int mFreeListHeadOffset;
    private long mFreeListHeadFirstPageId;
    private long mFreeListPageCount;
    private long mFreePageCount;
    private long mTotalPageCount;

    // These fields are guarded by mDeleteLock.
    private final ReentrantLock mDeleteLock;
    private final IdHeap mDeletedIds;
    private final byte[] mDeletedListTail;
    private long mDeletedListTailId;
    private long mDeletedListHeadId;
    private long mDeletedListHeadFirstPageId;
    private long mDeletedListPageCount;
    private long mDeletedPageCount;

    /**
     * Create a PageAllocator for a new array.
     *
     * @param firstPageIndex first page index that can be allocated; must be at least 1
     */
    public PageAllocator(PageArray array, long firstPageIndex) throws IOException {
        this(array, firstPageIndex, null, 0);
    }

    /**
     * Create a PageAllocator for an existing array.
     *
     * @param header source for reading allocator root structure
     * @param offset offset into header
     */
    public PageAllocator(PageArray array, byte[] header, int offset) throws IOException {
        // Access header length only for null check.
        this(array, header.length * 0, header, offset);
    }

    private PageAllocator(PageArray array, long firstPageIndex, byte[] header, int offset)
        throws IOException
    {
        if (array == null) {
            throw new IllegalArgumentException("PageArray is null");
        }

        byte[] freeListHead = new byte[array.pageSize()];

        long totalPageCount;
        long freePageCount;
        long freeListPageCount;
        long firstFreeListId;
        int firstFreeOffset;
        long firstFreeFirstPageId;

        if (header == null) {
            if (firstPageIndex < 1) {
                throw new IllegalArgumentException("First page index: " + firstPageIndex);
            }

            totalPageCount = firstPageIndex;
            freePageCount = 0;
            freeListPageCount = 0;

            firstFreeListId = 0;
            firstFreeOffset = 0;
            firstFreeFirstPageId = 0;
        } else {
            totalPageCount = DataIO.readLong(header, offset + I_TOTAL_PAGE_COUNT);
            freePageCount = DataIO.readLong(header, offset + I_FREE_PAGE_COUNT);
            freeListPageCount = DataIO.readLong(header, offset + I_FREE_LIST_PAGE_COUNT);

            long actualPageCount = array.getPageCount();
            if (actualPageCount > totalPageCount) {
                if (!array.isReadOnly()) {
                    // Truncate extra uncommitted pages.
                    //System.out.println("Page count is too large: "
                    //                   + actualPageCount + " > " + totalPageCount);
                    array.setPageCount(totalPageCount, false);
                }
            } else if (actualPageCount < totalPageCount) {
                throw new CorruptPageStoreException
                    ("Page count is too small: " + actualPageCount + " < " + totalPageCount);
            }

            firstFreeListId = DataIO.readLong(header, offset + I_FIRST_FREE_LIST_ID);
            if (firstFreeListId == 0) {
                firstFreeOffset = 0;
                firstFreeFirstPageId = 0;
            } else {
                firstFreeOffset = DataIO.readInt(header, offset + I_FIRST_FREE_OFFSET);
                firstFreeFirstPageId =
                    DataIO.readLong(header, offset + I_FIRST_FREE_FIRST_PAGE_ID);
                array.readPage(firstFreeListId, freeListHead);
            }
        }

        mPageArray = array;

        // These locks must be reentrant. The deletePage method can call into
        // drainDeletedIds, which calls allocPage, which can re-acquire delete
        // lock. The commit method acquires both locks, and then it calls
        // drainDeletedIds, acquiring the locks again.
        mAllocLock = new ReentrantLock(true);
        mDeleteLock = new ReentrantLock(true);

        mAllocLock.lock();
        try {
            mFreeListHead = freeListHead;
            mFreeListHeadId = firstFreeListId;
            mFreeListHeadOffsetRef = new IntegerRef() {
                public int get() {
                    return mFreeListHeadOffset;
                }
                public void set(int offset) {
                    mFreeListHeadOffset = offset;
                }
            };
            mFreeListHeadOffset = firstFreeOffset;
            mFreeListHeadFirstPageId = firstFreeFirstPageId;
            mFreeListPageCount = freeListPageCount;
            mFreePageCount = freePageCount;
            mTotalPageCount = totalPageCount;
        } finally {
            mAllocLock.unlock();
        }

        mDeletedIds = new IdHeap(array.pageSize() - I_FREE_NODE_START);
        mDeletedListTail = new byte[array.pageSize()];
    }

    public PageArray pageArray() {
        return mPageArray;
    }

    /**
     * Allocates a page from the free list or by growing the underlying page
     * array. No page allocations are permanent until after commit is called.
     */
    public long allocPage() throws IOException {
        return allocPage(false);
    }

    /**
     * Deletes a page to be reused after commit is called. No page deletions
     * are permanent until after commit is called.
     */
    public void deletePage(long id) throws IOException {
        if (id < 1) {
            throw new IllegalArgumentException("Page id: " + id);
        }

        final IdHeap deletedIds = mDeletedIds;

        mDeleteLock.lock();
        try {
            deletedIds.add(id);
            mDeletedPageCount++;
            if (deletedIds.shouldDrain()) {
                drainDeletedIds(deletedIds);
            }
        } finally {
            mDeleteLock.unlock();
        }
    }

    /**
     * Makes changes permanent for page allocations and deletions. The
     * underlying page array is not flushed, however.
     *
     * @param header destination for writing allocator root structure
     * @param offset offset into header
     * @param ready invoked during the commit operation
     */
    public void commit(byte[] header, int offset, CommitReady ready) throws IOException {
        final long freePageCount;
        final long freeListPageCount;
        final long freeListHeadId;
        final int freeListHeadOffset;
        final long freeListHeadFirstPageId;
        final long totalPageCount;

        final IdHeap deletedIds = mDeletedIds;

        // Lock everything to prevent any changes during commit. Avoid deadlock
        // by acquiring delete lock before allocation lock. This matches the
        // lock order used by the deletePage method, which might call
        // allocPage, which in turn acquires allocation lock.

        mDeleteLock.lock();

        // Attempt unfair lock acquisition to avoid waiting.
        if (!mAllocLock.tryLock()) {
            mAllocLock.lock();
        }

        try {
            // Link deleted ids to free list.
            while (deletedIds.size() > 0) {
                drainDeletedIds(deletedIds);
            }
            if (mDeletedListTailId != 0) {
                byte[] tailBuf = mDeletedListTail;
                DataIO.writeLong(tailBuf, I_NEXT_FREE_LIST_ID, mFreeListHeadId);
                DataIO.writeInt(tailBuf, I_NEXT_FREE_OFFSET, mFreeListHeadOffset);
                DataIO.writeLong(tailBuf, I_NEXT_FREE_FIRST_PAGE_ID, mFreeListHeadFirstPageId);
                mPageArray.writePage(mDeletedListTailId, tailBuf);
            }

            freeListPageCount = mFreeListPageCount + mDeletedListPageCount;
            freePageCount = mFreePageCount + mDeletedPageCount;
            totalPageCount = mTotalPageCount;

            if (mDeletedListHeadId == 0) {
                freeListHeadId = mFreeListHeadId;
                freeListHeadOffset = mFreeListHeadOffset;
                freeListHeadFirstPageId = mFreeListHeadFirstPageId;
            } else {
                freeListHeadId = mDeletedListHeadId;
                freeListHeadOffset = I_FREE_NODE_START;
                freeListHeadFirstPageId = mDeletedListHeadFirstPageId;

                if (freeListHeadId == mDeletedListTailId) {
                    // Still have a copy of deleted list head in memory.
                    System.arraycopy(mDeletedListTail, 0,
                                     mFreeListHead, 0, mFreeListHead.length);
                } else {
                    // Need to go back and read deleted list head.
                    mPageArray.readPage(freeListHeadId, mFreeListHead);
                }
            }
        } catch (IOException e) {
            mAllocLock.unlock();
            mDeleteLock.unlock();
            throw e;
        }

        mDeletedListTailId = 0;
        mDeletedListHeadId = 0;
        mDeletedListHeadFirstPageId = 0;
        mDeletedListPageCount = 0;
        mDeletedPageCount = 0;

        // After releasing this lock, pages can be deleted again, but no pages
        // can be allocated. As a result, once the deleted list fills up, it
        // cannot drain without blocking on the allocation lock.
        mDeleteLock.unlock();

        try {
            if (totalPageCount < 1) {
                throw new CorruptPageStoreException("Illegal total page count: " + totalPageCount);
            }

            if (freeListPageCount < 0) {
                throw new CorruptPageStoreException
                    ("Illegal free list page count: " + freeListPageCount);
            }

            if (freeListPageCount > (totalPageCount - 1)) {
                throw new CorruptPageStoreException
                    ("Free list page count too large: " + freeListPageCount + " > " +
                     (totalPageCount - 1));
            }

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
                if (!isValidPageId(freeListHeadId, totalPageCount)) {
                    throw new CorruptPageStoreException
                        ("Illegal free list page id: " + freeListHeadId);
                }
                if (freeListHeadOffset < I_FREE_NODE_START) {
                    throw new CorruptPageStoreException
                        ("Illegal free list page offset: " + freeListHeadOffset);
                }
                if (!isValidPageId(freeListHeadFirstPageId, totalPageCount)) {
                    throw new CorruptPageStoreException
                        ("Illegal free list first page id: " + freeListHeadFirstPageId);
                }
            }

            DataIO.writeInt(header, offset, 0); // reserved
            DataIO.writeLong(header, offset + I_TOTAL_PAGE_COUNT, totalPageCount);
            DataIO.writeLong(header, offset + I_FREE_PAGE_COUNT, freePageCount);
            DataIO.writeLong(header, offset + I_FREE_LIST_PAGE_COUNT, freeListPageCount);
            DataIO.writeLong(header, offset + I_FIRST_FREE_LIST_ID, freeListHeadId);
            DataIO.writeInt(header, offset + I_FIRST_FREE_OFFSET, freeListHeadOffset);
            DataIO.writeLong(header, offset + I_FIRST_FREE_FIRST_PAGE_ID, freeListHeadFirstPageId);

            if (ready != null) {
                ready.ready(HEADER_SIZE);
            }

            mFreeListHeadId = freeListHeadId;
            mFreeListHeadOffset = freeListHeadOffset;
            mFreeListHeadFirstPageId = freeListHeadFirstPageId;
            mFreeListPageCount = freeListPageCount;
            mFreePageCount = freePageCount;
        } finally {
            mAllocLock.unlock();
        }
    }

    private long allocPage(boolean forDeletedList) throws IOException {
        long pageId;
        long oldFreeListHeadId;
        boolean accessedFile;

        final byte[] freeListHead = mFreeListHead;

        mAllocLock.lock();
        try {
            if (mFreeListHeadId == 0) {
                pageId = mTotalPageCount++;

                // Force file to grow, distributing the cost of allocation.
                mPageArray.setPageCount(pageId + 1, true);

                return pageId;
            }

            pageId = mFreeListHeadFirstPageId;

            if (!isValidPageId(pageId, mTotalPageCount)) {
                throw new CorruptPageStoreException("Invalid page id in free list: " + pageId);
            }

            mFreePageCount--;

            if (mFreeListHeadOffset < mPageArray.pageSize()) {
                long delta = DataIO.readUnsignedVarLong(freeListHead, mFreeListHeadOffsetRef);
                if (delta > 0) {
                    mFreeListHeadFirstPageId = pageId + delta;
                    return pageId;
                }
            }

            // Move to the next node in the free list.

            oldFreeListHeadId = mFreeListHeadId;
            long nextId = DataIO.readLong(freeListHead, I_NEXT_FREE_LIST_ID);

            if (nextId == 0) {
                mFreeListHeadId = 0;
                mFreeListHeadOffset = 0;
                mFreeListHeadFirstPageId = 0;
            } else {
                int nextOffset = DataIO.readInt(freeListHead, I_NEXT_FREE_OFFSET);
                long nextFirstPageId = DataIO.readLong(freeListHead, I_NEXT_FREE_FIRST_PAGE_ID);

                mPageArray.readPage(nextId, freeListHead);

                mFreeListHeadId = nextId;
                mFreeListHeadOffset = nextOffset;
                mFreeListHeadFirstPageId = nextFirstPageId;
            }

            mFreeListPageCount--;
        } finally {
            mAllocLock.unlock();
        }

        // Delete old free list node outside allocation lock to prevent
        // deadlock with commit. Lock acquisition order wouldn't match. Note
        // that node is deleted instead of used for next allocation. This
        // ensures that no important data is overwritten until after commit.
        {
            final IdHeap deletedIds = mDeletedIds;

            mDeleteLock.lock();
            try {
                deletedIds.add(oldFreeListHeadId);
                mDeletedPageCount++;
                if (!forDeletedList && deletedIds.shouldDrain()) {
                    drainDeletedIds(deletedIds);
                }
                // If forDeletedList was true, then allocPage is called by
                // drainDeletedIds itself. The IdHeap has padding for one more
                // id, which gets drained when control returns to the caller.
            } finally {
                mDeleteLock.unlock();
            }
        }

        return pageId;
    }

    // Caller must hold delete lock.
    private void drainDeletedIds(IdHeap deletedIds) throws IOException {
        long newTailId = allocPage(true);

        byte[] tailBuf = mDeletedListTail;
        long firstPageId = deletedIds.peek();

        if (mDeletedListTailId == 0) {
            mDeletedListHeadId = newTailId;
            mDeletedListHeadFirstPageId = firstPageId;
        } else {
            DataIO.writeLong(tailBuf, I_NEXT_FREE_LIST_ID, newTailId);
            DataIO.writeInt(tailBuf, I_NEXT_FREE_OFFSET, I_FREE_NODE_START);
            DataIO.writeLong(tailBuf, I_NEXT_FREE_FIRST_PAGE_ID, firstPageId);
            mPageArray.writePage(mDeletedListTailId, tailBuf);
        }

        mDeletedListPageCount++;

        int pageSize = mPageArray.pageSize();
        int end = deletedIds.drain(deletedIds.remove(),
                                   tailBuf,
                                   I_FREE_NODE_START,
                                   pageSize - I_FREE_NODE_START);

        // Clean out any cruft from previous usage.
        Arrays.fill(tailBuf, end, pageSize, (byte) 0);

        mDeletedListTailId = newTailId;
    }

    void addTo(PageStore.Stats stats) {
        mAllocLock.lock();
        try {
            stats.totalPages += mTotalPageCount;
            stats.freePages += mFreePageCount;
        } finally {
            mAllocLock.unlock();
        }
    }

    /**
     * Clears bits representing pages which are in the free list, and set pages
     * which are in use.
     */
    void traceFreePages(BitSet pages, int scalar, int offset) throws IOException {
        {
            int total;
            mAllocLock.lock();
            try {
                total = (int) mTotalPageCount;
            } finally {
                mAllocLock.unlock();
            }
            for (int i=0; i<total; i++) {
                pages.set(i * scalar + offset);
            }
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

        mAllocLock.lock();
        try {
            long nodeId = mFreeListHeadId;
            if (nodeId == 0) {
                return;
            }

            byte[] node = mFreeListHead.clone();
            long pageId = mFreeListHeadFirstPageId;
            nodeOffsetRef.offset = mFreeListHeadOffset;

            while (true) {
                if (!isValidPageId(pageId, mTotalPageCount)) {
                    throw new CorruptPageStoreException
                        ("Invalid page id in free list: " + (pageId * scalar + offset));
                }

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

                clearPageBit(pages, nodeId, scalar, offset);
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

    private static boolean isValidPageId(long pageId, long totalPageCount) {
        return 1 <= pageId && pageId < totalPageCount;
    }

    public static interface CommitReady {
        /**
         * Called when commit has written data into the header. Until this
         * method returns, new page allocations are blocked. Pages can still be
         * deleted, but they might block too if an allocation is required to
         * drain deleted pages.
         *
         * @param amount written into allocator header
         */
        public void ready(int headerSize) throws IOException;
    }
}
