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

import java.util.BitSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.cojen.tupl.io.PageArray;

/**
 * Manages free and deleted pages.
 *
 * @author Brian S O'Neill
 */
final class PageManager {
    /*

    Header structure is encoded as follows, in 140 bytes:

    +--------------------------------------------+
    | long: total page count                     |
    | PageQueue: regular free list (44 bytes)    |
    | PageQueue: recycle free list (44 bytes)    |
    | PageQueue: reserve list (44 bytes)         |
    +--------------------------------------------+

    */

    // Indexes of entries in header.
    private static final int I_TOTAL_PAGE_COUNT  = 0;
    private static final int I_REGULAR_QUEUE     = I_TOTAL_PAGE_COUNT + 8;
    private static final int I_RECYCLE_QUEUE     = I_REGULAR_QUEUE + PageQueue.HEADER_SIZE;
    private static final int I_RESERVE_QUEUE     = I_RECYCLE_QUEUE + PageQueue.HEADER_SIZE;

    private final PageArray mPageArray;

    // One remove lock for all queues.
    private final ReentrantLock mRemoveLock;
    private long mTotalPageCount;

    private final PageQueue mRegularFreeList;
    private final PageQueue mRecycleFreeList;

    private final ReentrantReadWriteLock mCompactionLock;
    private volatile boolean mCompacting;
    private long mCompactionTargetPageCount;
    private long mReserveReclaimUpperBound;
    private PageQueue mReserveList;

    static final int
        ALLOC_TRY_RESERVE = -1, // Create pages: no.  Compaction zone: yes
        ALLOC_NORMAL = 0,       // Create pages: yes. Compaction zone: no
        ALLOC_RESERVE = 1;      // Create pages: yes. Compaction zone: yes

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
        mRegularFreeList = PageQueue.newRegularFreeList(this);
        mRecycleFreeList = PageQueue.newRecycleFreeList(this);

        mCompactionLock = new ReentrantReadWriteLock(false);

        if (!restored) {
            // Pages 0 and 1 are reserved.
            mTotalPageCount = 2;
            mRegularFreeList.init(createPage());
            mRecycleFreeList.init(createPage());
        } else {
            mTotalPageCount = readTotalPageCount(header, offset + I_TOTAL_PAGE_COUNT);

            long actualPageCount = array.getPageCount();
            if (actualPageCount > mTotalPageCount) {
                if (!array.isReadOnly()) {
                    // Truncate extra uncommitted pages.
                    /*
                    System.out.println("Page count is too large: "
                                       + actualPageCount + " > " + mTotalPageCount);
                    */
                    array.setPageCount(mTotalPageCount);
                }
            } else if (actualPageCount < mTotalPageCount) {
                /* TODO: can be caused by pre-allocated append tail node
                System.out.println("Page count is too small: " + actualPageCount + " < " +
                                   mTotalPageCount);
                throw new CorruptPageStoreException
                    ("Page count is too small: " + actualPageCount + " < " + mTotalPageCount);
                */
            }

            PageQueue reserve;
            fullLock();
            try {
                mRegularFreeList.init(header, offset + I_REGULAR_QUEUE);
                mRecycleFreeList.init(header, offset + I_RECYCLE_QUEUE);

                if (PageQueue.exists(header, offset + I_RESERVE_QUEUE)) {
                    reserve = mRegularFreeList.newReserveFreeList();
                    reserve.init(header, offset + I_RESERVE_QUEUE);
                } else {
                    reserve = null;
                }
            } finally {
                fullUnlock();
            }

            if (reserve != null) {
                // Reclaim reserved pages from an aborted compaction. Pages are immediately
                // usable, because a commit had completed.
                reserve.reclaim(mRemoveLock, mTotalPageCount - 1, true);
            }
        }
    }

    static long readTotalPageCount(byte[] header, int offset) {
        return Utils.decodeLongLE(header, offset + I_TOTAL_PAGE_COUNT);
    }

    public PageArray pageArray() {
        return mPageArray;
    }

    /**
     * Allocates a page from the free list or by growing the underlying page
     * array. No page allocations are permanent until after commit is called.
     *
     * @return non-zero page id
     */
    public long allocPage() throws IOException {
        return allocPage(ALLOC_NORMAL);
    }

    /**
     * @param mode ALLOC_TRY_RESERVE, ALLOC_NORMAL, ALLOC_RESERVE
     * @return 0 if mode is ALLOC_TRY_RESERVE and free lists are empty
     */
    long allocPage(int mode) throws IOException {
        while (true) {
            long pageId;
            alloc: {
                // Remove recently recycled pages first, reducing page writes caused by queue
                // drains. For allocations by the reserve lists (during compaction), pages can
                // come from the reserve lists themselves. This favors using pages in the
                // compaction zone, since the reserve lists will be discarded anyhow.

                pageId = mRecycleFreeList.tryUnappend();
                if (pageId != 0) {
                    break alloc;
                }
                final Lock lock = mRemoveLock;
                lock.lock();
                pageId = mRecycleFreeList.tryRemove(lock);
                if (pageId != 0) {
                    break alloc;
                }
                pageId = mRegularFreeList.tryRemove(lock);
                if (pageId != 0) {
                    break alloc;
                }
                try {
                    return mode >= ALLOC_NORMAL ? createPage() : 0;
                } finally {
                    lock.unlock();
                }
            }

            if (mode == ALLOC_NORMAL && mCompacting) {
                final Lock compactionLock = mCompactionLock.readLock();
                compactionLock.lock();
                try {
                    if (mCompacting && pageId >= mCompactionTargetPageCount) {
                        // Page is in the compaction zone, so allocate another.
                        mReserveList.append(pageId);
                        continue;
                    }
                } finally {
                    compactionLock.unlock();
                }
            }

            return pageId;
        }
    }

    /**
     * Deletes a page to be reused after commit is called. No page deletions
     * are permanent until after commit is called.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void deletePage(long id) throws IOException {
        deletePage(id, false);
    }

    /**
     * Recycles a page for immediate re-use.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void recyclePage(long id) throws IOException {
        deletePage(id, true);
    }

    void deletePage(long id, boolean recycle) throws IOException {
        if (mCompacting) {
            final Lock compactionLock = mCompactionLock.readLock();
            compactionLock.lock();
            try {
                if (mCompacting && id >= mCompactionTargetPageCount) {
                    // Page is in the compaction zone.
                    mReserveList.append(id);
                    return;
                }
            } finally {
                compactionLock.unlock();
            }
        }

        if (recycle) {
            mRecycleFreeList.append(id);
        } else {
            mRegularFreeList.append(id);
        }
    }

    /**
     * Allocates pages for immediate use.
     *
     * @return actual amount allocated
     */
    public long allocatePages(long pageCount) throws IOException {
        if (pageCount <= 0) {
            return 0;
        }

        PageDb.Stats stats = new PageDb.Stats();
        addTo(stats);
        pageCount -= stats.freePages;

        if (pageCount <= 0) {
            return 0;
        }

        for (int i=0; i<pageCount; i++) {
            long pageId;
            mRemoveLock.lock();
            try {
                pageId = createPage();
            } finally {
                mRemoveLock.unlock();
            }
            recyclePage(pageId);
        }

        return pageCount;
    }

    /**
     * @return false if target cannot be met
     */
    public boolean compactionStart(long targetPageCount) throws IOException {
        final Lock lock = mCompactionLock.writeLock();
        lock.lock();
        try {
            if (mCompacting) {
                throw new IllegalStateException("Compaction in progress");
            }
            if (mReserveList != null) {
                throw new IllegalStateException();
            }

            if (targetPageCount < 2) {
                return false;
            }

            mRemoveLock.lock();
            try {
                if (targetPageCount >= mTotalPageCount) {
                    return false;
                }
            } finally {
                mRemoveLock.unlock();
            }

            long initPageId = allocPage(ALLOC_TRY_RESERVE);
            if (initPageId == 0) {
                return false;
            }

            try {
                // Allocate as agressive, allowing reclamation access to all pages.
                mReserveList = mRegularFreeList.newReserveFreeList();
                mReserveList.init(initPageId);
            } catch (Throwable e) {
                mReserveList = null;
                try {
                    recyclePage(initPageId);
                } catch (Throwable e2) {
                    // Ignore.
                }
                throw Utils.rethrow(e);
            }

            mCompactionTargetPageCount = targetPageCount;
            mCompacting = true;
        } finally {
            lock.unlock();
        }

        return mCompacting;
    }

    /**
     * @return false if aborted
     */
    public boolean compactionScanFreeList() throws IOException {
        return compactionScanFreeList(mRecycleFreeList)
            && compactionScanFreeList(mRegularFreeList);
    }

    private boolean compactionScanFreeList(PageQueue list) throws IOException {
        long target = list.getRemoveScanTarget();

        while (mCompacting) {
            mRemoveLock.lock();
            long pageId;
            if (list.isRemoveScanComplete(target) || (pageId = list.tryRemove(mRemoveLock)) == 0) {
                mRemoveLock.unlock();
                return mCompacting;
            }
            if (pageId >= mCompactionTargetPageCount) {
                mReserveList.append(pageId);
            } else {
                mRecycleFreeList.append(pageId);
            }
        }

        return false;
    }

    /**
     * @return false if verification failed
     */
    public boolean compactionVerify() throws IOException {
        if (!mCompacting) {
            // Prevent caller from doing any more work.
            return true;
        }
        long total;
        mRemoveLock.lock();
        total = mTotalPageCount;
        mRemoveLock.unlock();
        return mReserveList.verifyPageRange(mCompactionTargetPageCount, total);
    }

    /**
     * @return false if aborted
     */
    public boolean compactionEnd() throws IOException {
        // Default will reclaim everything.
        long upperBound = Long.MAX_VALUE;
        boolean result = false;

        if (mCompacting) {
            if (!compactionVerify()) {
                mCompacting = false;
            } else {
                // FIXME: deadlock possible
                fullLock();
                if (mCompacting && mTotalPageCount > mCompactionTargetPageCount) {
                    // When lock is released, compaction is commit-ready. All pages in the
                    // compaction zone are accounted for, but the reserve list's queue nodes
                    // might be in the valid zone. Most pages will simply be discarded.
                    mTotalPageCount = mCompactionTargetPageCount;
                    upperBound = mTotalPageCount - 1;
                    result = true;
                }
                mCompacting = false;
                fullUnlock();
            }
        }

        if (mReserveList != null) {
            mReserveList.reclaim(mRemoveLock, upperBound, false);
            mReserveList = null;
        }

        return result;
    }

    /**
     * Called after compaction, to actually shrink the file.
     */
    public void truncatePages() throws IOException {
        mRemoveLock.lock();
        try {
            if (mTotalPageCount < mPageArray.getPageCount()) {
                mPageArray.setPageCount(mTotalPageCount);
            }
        } finally {
            mRemoveLock.unlock();
        }
    }

    /**
     * Initiates the process for making page allocations and deletions permanent.
     *
     * @param header destination for writing page manager structures
     * @param offset offset into header
     */
    public void commitStart(byte[] header, int offset) throws IOException {
        fullLock();
        try {
            // Pre-commit all first, draining the append heaps.
            mRegularFreeList.preCommit();
            mRecycleFreeList.preCommit();
            if (mReserveList != null) {
                mReserveList.preCommit();
            }

            // Total page count is written after append heaps have been
            // drained, because additional pages might have been allocated.
            Utils.encodeLongLE(header, offset + I_TOTAL_PAGE_COUNT, mTotalPageCount);

            mRegularFreeList.commitStart(header, offset + I_REGULAR_QUEUE);
            mRecycleFreeList.commitStart(header, offset + I_RECYCLE_QUEUE);
            if (mReserveList != null) {
                mReserveList.commitStart(header, offset + I_RESERVE_QUEUE);
            }
        } finally {
            fullUnlock();
        }
    }

    /**
     * Finishes the commit process. Must pass in header and offset used by commitStart.
     *
     * @param header contains page manager structures
     * @param offset offset into header
     */
    public void commitEnd(byte[] header, int offset) throws IOException {
        mRemoveLock.lock();
        try {
            mRegularFreeList.commitEnd(header, offset + I_REGULAR_QUEUE);
            mRecycleFreeList.commitEnd(header, offset + I_RECYCLE_QUEUE);
            if (mReserveList != null) {
                mReserveList.commitEnd(header, offset + I_RESERVE_QUEUE);
            }
        } finally {
            mRemoveLock.unlock();
        }
    }

    private void fullLock() {
        // Avoid deadlock by acquiring append locks before remove lock. This matches the lock
        // order used by the deletePage method, which might call allocPage, which in turn
        // acquires remove lock. Compaction lock is acquired before append lock because
        // allocPage may call append with compaction lock held.
        mCompactionLock.writeLock().lock();
        mRegularFreeList.appendLock().lock();
        mRecycleFreeList.appendLock().lock();
        if (mReserveList != null) {
            mReserveList.appendLock().lock();
        }
        mRemoveLock.lock();
    }

    private void fullUnlock() {
        mRemoveLock.unlock();
        if (mReserveList != null) {
            mReserveList.appendLock().unlock();
        }
        mRecycleFreeList.appendLock().unlock();
        mRegularFreeList.appendLock().unlock();
        mCompactionLock.writeLock().unlock();
    }

    void addTo(PageDb.Stats stats) {
        fullLock();
        try {
            stats.totalPages += mTotalPageCount;
            mRegularFreeList.addTo(stats);
            mRecycleFreeList.addTo(stats);
            if (mReserveList != null) {
                mReserveList.addTo(stats);
            }
        } finally {
            fullUnlock();
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
        count += mRecycleFreeList.traceRemovablePages(pages);
        return count;
    }

    /**
     * Expand the underlying page store to create a new page. Method is
     * invoked with remove lock held.
     */
    private long createPage() throws IOException {
        // Compaction cannot succeed if store is growing. Check first before performing
        // volatile write.
        if (mCompacting) {
            mCompacting = false;
        }
        return mTotalPageCount++;
    }

    /**
     * Method must be invoked with remove lock held.
     */
    boolean isPageOutOfBounds(long id) {
        return id <= 1 || id >= mTotalPageCount;
    }

    /**
     * Method must be invoked with remove lock held.
     */
    void reserveReclaimUpperBound(long upperBound) {
        mReserveReclaimUpperBound = upperBound;
    }

    /**
     * Method must be invoked with remove lock held.
     */
    long reserveReclaimUpperBound() {
        return mReserveReclaimUpperBound;
    }
}
