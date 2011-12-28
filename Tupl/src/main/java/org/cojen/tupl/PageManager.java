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

    Header structure is encoded as follows, in 52 bytes:

    +--------------------------------------------+
    | long: total page count                     |
    | PageQueue: regular free list (44 bytes)    |
    +--------------------------------------------+

    */

    // Indexes of entries in header.
    private static final int I_TOTAL_PAGE_COUNT = 0;
    private static final int I_REGULAR_QUEUE    = I_TOTAL_PAGE_COUNT + 8;
    private static final int HEADER_SIZE        = I_REGULAR_QUEUE + PageQueue.HEADER_SIZE;

    private final PageArray mPageArray;

    // One remove lock for all queues.
    private final ReentrantLock mRemoveLock;
    private long mTotalPageCount;

    private final PageQueue mRegularFreeList;

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

        mPageArray = array;

        // Lock must be reentrant and unfair. See notes in PageQueue.
        mRemoveLock = new ReentrantLock(false);
        mRegularFreeList = new PageQueue(this);

        if (!restored) {
            // Pages 0 and 1 are reserved.
            mTotalPageCount = 2;
            mRegularFreeList.init();
        } else {
            mTotalPageCount = DataIO.readLong(header, offset + I_TOTAL_PAGE_COUNT);

            long actualPageCount = array.getPageCount();
            if (actualPageCount > mTotalPageCount) {
                if (!array.isReadOnly()) {
                    // Truncate extra uncommitted pages.
                    System.out.println("Page count is too large: "
                                       + actualPageCount + " > " + mTotalPageCount);
                    array.setPageCount(mTotalPageCount, false);
                }
            } else if (actualPageCount < mTotalPageCount) {
                System.out.println("Page count is too small: " + actualPageCount + " < " +
                                   mTotalPageCount);
                /* FIXME: can be caused by pre-allocated append tail node
                throw new CorruptPageStoreException
                    ("Page count is too small: " + actualPageCount + " < " + mTotalPageCount);
                */
            }

            fullLock();
            try {
                mRegularFreeList.init(header, offset + I_REGULAR_QUEUE);
            } finally {
                fullUnlock();
            }
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
        final Lock lock = mRemoveLock;
        lock.lock();
        long pageId = mRegularFreeList.remove(lock);
        if (pageId != 0) {
            return pageId;
        }
        try {
            return createPage(grow);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deletes a page to be reused after commit is called. No page deletions
     * are permanent until after commit is called.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void deletePage(long id) throws IOException {
        mRegularFreeList.append(id);
    }

    /**
     * Recycles a page for immediate re-use.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void recyclePage(long id) throws IOException {
        // FIXME: use recycle free list
        deletePage(id);
    }

    /**
     * Preallocates pages for immediate use.
     */
    public void preallocate(long pageCount) throws IOException {
        if (pageCount <= 0) {
            return;
        }

        PageStore.Stats stats = new PageStore.Stats();
        addTo(stats);
        pageCount -= stats.freePages;

        while (--pageCount >= 0) {
            long pageId;
            mRemoveLock.lock();
            try {
                pageId = createPage(pageCount == 0);
            } finally {
                mRemoveLock.unlock();
            }
            recyclePage(pageId);
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
        fullLock();
        try {
            mRegularFreeList.commitStart(header, offset + I_REGULAR_QUEUE);
            // Write total after committing free list, to account for
            // additional pages it needed to allocate for draining nodes.
            DataIO.writeLong(header, offset + I_TOTAL_PAGE_COUNT, mTotalPageCount);
        } finally {
            fullUnlock();
        }

        Object state = ready == null ? null : ready.started();

        // FIXME: Is full lock required? Are two callbacks required? Is
        // commitStart and commitEnd sufficient?
        fullLock();
        try {
            if (ready != null) {
                ready.finished(state, offset + HEADER_SIZE);
            }

            // FIXME: Only remove lock is required.
            mRegularFreeList.commitEnd(header, offset + I_REGULAR_QUEUE);
        } finally {
            fullUnlock();
        }
    }

    private void fullLock() {
        // Avoid deadlock by acquiring append locks before remove lock. This
        // matches the lock order used by the deletePage method, which might
        // call allocPage, which in turn acquires remove lock.
        mRegularFreeList.appendLock().lock();
        mRemoveLock.lock();
    }

    private void fullUnlock() {
        mRemoveLock.unlock();
        mRegularFreeList.appendLock().unlock();
    }

    void addTo(PageStore.Stats stats) {
        mRemoveLock.lock();
        try {
            stats.totalPages += mTotalPageCount;
            mRegularFreeList.addTo(stats);
        } finally {
            mRemoveLock.unlock();
        }
    }

    /**
     * Sets a bit for each page except the first two.
     */
    void markAllPages(BitSet pages) throws IOException {
        mRemoveLock.lock();
        try {
            int total = (int) mTotalPageCount;
            for (int i=2; i<total; i++) {
                pages.set(i);
            }
        } finally {
            mRemoveLock.unlock();
        }
    }

    /**
     * Clears bits representing pages which are allocatable.
     */
    int traceFreePages(BitSet pages) throws IOException {
        int count = mRegularFreeList.traceRemovablePages(pages);
        return count;
    }

    /**
     * Expand the underlying page store to create a new page. Method is
     * invoked with remove lock held.
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
    boolean isPageOutOfBounds(long id) {
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
