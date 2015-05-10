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

    private volatile boolean mCompacting;
    private long mCompactionTargetPageCount = Long.MAX_VALUE;
    private PageQueue mReserveList;

    static final int
        ALLOC_TRY_RESERVE = -1, // Create pages: no.  Compaction zone: yes
        ALLOC_NORMAL = 0,       // Create pages: yes. Compaction zone: no
        ALLOC_RESERVE = 1;      // Create pages: yes. Compaction zone: yes

    /**
     * Create a new PageManager.
     */
    PageManager(PageArray array) throws IOException {
        this(false, array, PageOps.p_null(), 0);
    }

    /**
     * Create a restored PageManager.
     *
     * @param header source for reading allocator root structure
     * @param offset offset into header
     */
    PageManager(PageArray array, /*P*/ byte[] header, int offset) throws IOException {
        this(true, array, header, offset);
    }

    private PageManager(boolean restored, PageArray array, /*P*/ byte[] header, int offset)
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

        try {
            if (!restored) {
                // Pages 0 and 1 are reserved.
                mTotalPageCount = 4;
                mRegularFreeList.init(2);
                mRecycleFreeList.init(3);
            } else {
                mTotalPageCount = readTotalPageCount(header, offset + I_TOTAL_PAGE_COUNT);

                long actualPageCount = array.getPageCount();
                if (actualPageCount > mTotalPageCount) {
                    if (!array.isReadOnly()) {
                        // Truncate extra uncommitted pages.
                        array.setPageCount(mTotalPageCount);
                    }
                } else if (actualPageCount < mTotalPageCount) {
                    // Not harmful -- can be caused by pre-allocated append tail node.
                }

                PageQueue reserve;
                fullLock();
                try {
                    mRegularFreeList.init(header, offset + I_REGULAR_QUEUE);
                    mRecycleFreeList.init(header, offset + I_RECYCLE_QUEUE);

                    if (PageQueue.exists(header, offset + I_RESERVE_QUEUE)) {
                        reserve = mRegularFreeList.newReserveFreeList();
                        try {
                            reserve.init(header, offset + I_RESERVE_QUEUE);
                        } catch (Throwable e) {
                            reserve.delete();
                            throw e;
                        }
                    } else {
                        reserve = null;
                    }
                } finally {
                    fullUnlock();
                }

                if (reserve != null) {
                    try {
                        // Reclaim reserved pages from an aborted compaction.
                        reserve.reclaim(mRemoveLock, mTotalPageCount - 1);
                    } finally {
                        reserve.delete();
                    }
                }
            }
        } catch (Throwable e) {
            delete();
            throw e;
        }
    }

    /**
     * Must be called when object is no longer referenced.
     */
    void delete() {
        if (mRegularFreeList != null) {
            mRegularFreeList.delete();
        }
        if (mRecycleFreeList != null) {
            mRecycleFreeList.delete();
        }
        PageQueue reserve = mReserveList;
        if (reserve != null) {
            reserve.delete();
            mReserveList = null;
        }
    }

    static long readTotalPageCount(/*P*/ byte[] header, int offset) {
        return PageOps.p_longGetLE(header, offset + I_TOTAL_PAGE_COUNT);
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
                    // Lock has been released as a side-effect.
                    break alloc;
                }
                pageId = mRegularFreeList.tryRemove(lock);
                if (pageId != 0) {
                    // Lock has been released as a side-effect.
                    break alloc;
                }

                if (mode >= ALLOC_NORMAL) {
                    // Expand the file or abort compaction.

                    PageQueue reserve = mReserveList;
                    if (reserve != null) {
                        if (mCompacting) {
                            // Only do a volatile write the first time.
                            mCompacting = false;
                        }
                        // Attempt to raid the reserves.
                        pageId = reserve.tryRemove(lock);
                        if (pageId != 0) {
                            // Lock has been released as a side-effect.
                            return pageId;
                        }
                    }

                    pageId = mTotalPageCount++;
                }

                lock.unlock();
                return pageId;
            }

            if (mode == ALLOC_NORMAL && pageId >= mCompactionTargetPageCount && mCompacting) {
                // Page is in the compaction zone, so allocate another.
                mReserveList.append(pageId);
                continue;
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
        if (id >= mCompactionTargetPageCount && mCompacting) {
            // Page is in the compaction zone.
            mReserveList.append(id);
        } else {
            mRegularFreeList.append(id);
        }
    }

    /**
     * Recycles a page for immediate re-use.
     *
     * @throws IllegalArgumentException if id is less than or equal to one
     */
    public void recyclePage(long id) throws IOException {
        if (id >= mCompactionTargetPageCount && mCompacting) {
            // Page is in the compaction zone.
            mReserveList.append(id);
        } else {
            mRecycleFreeList.append(id);
        }
    }

    public void allocAndRecyclePage() throws IOException {
        long pageId;
        mRemoveLock.lock();
        try {
            pageId = mTotalPageCount++;
        } finally {
            mRemoveLock.unlock();
        }
        recyclePage(pageId);
    }

    /**
     * Caller must hold exclusive commit lock.
     *
     * @return false if target cannot be met
     */
    public boolean compactionStart(long targetPageCount) throws IOException {
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
            if (targetPageCount >= mTotalPageCount
                && targetPageCount >= mPageArray.getPageCount())
            {
                return false;
            }
        } finally {
            mRemoveLock.unlock();
        }

        long initPageId = allocPage(ALLOC_TRY_RESERVE);
        if (initPageId == 0) {
            return false;
        }

        PageQueue reserve;
        try {
            reserve = mRegularFreeList.newReserveFreeList();
            reserve.init(initPageId);
        } catch (Throwable e) {
            try {
                recyclePage(initPageId);
            } catch (Throwable e2) {
                // Ignore.
            }
            throw e;
        }

        mRemoveLock.lock();
        if (mReserveList != null) {
            mReserveList.delete();
        }
        mReserveList = reserve;
        mCompactionTargetPageCount = targetPageCount;
        // Volatile write performed after all the states it depends on are set. Because remove
        // lock is held, only append operations depend on this ordering.
        mCompacting = true;
        mRemoveLock.unlock();

        return true;
    }

    /**
     * @return false if aborted
     */
    public boolean compactionScanFreeList(ReentrantReadWriteLock commitLock) throws IOException {
        return compactionScanFreeList(commitLock, mRecycleFreeList)
            && compactionScanFreeList(commitLock, mRegularFreeList);
    }

    private boolean compactionScanFreeList(ReentrantReadWriteLock commitLock, PageQueue list)
        throws IOException
    {
        Lock sharedCommitLock = commitLock.readLock();

        long target;
        mRemoveLock.lock();
        target = list.getRemoveScanTarget();
        mRemoveLock.unlock();

        sharedCommitLock.lock();
        try {
            while (mCompacting) {
                mRemoveLock.lock();
                long pageId;
                if (list.isRemoveScanComplete(target)
                    || (pageId = list.tryRemove(mRemoveLock)) == 0)
                {
                    mRemoveLock.unlock();
                    return mCompacting;
                }
                if (pageId >= mCompactionTargetPageCount) {
                    mReserveList.append(pageId);
                } else {
                    mRecycleFreeList.append(pageId);
                }
                if (commitLock.hasQueuedThreads()) {
                    sharedCommitLock.unlock();
                    sharedCommitLock.lock();
                }
            }
        } finally {
            sharedCommitLock.unlock();
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
    public boolean compactionEnd(ReentrantReadWriteLock commitLock) throws IOException {
        // Default will reclaim everything.
        long upperBound = Long.MAX_VALUE;

        boolean ready = compactionVerify();

        // Need exclusive commit lock to prevent delete and recycle from attempting to operate
        // against a null reserve list. A race condition exists otherwise. Acquire full lock
        // too out of paranoia.
        commitLock.writeLock().lock();
        fullLock();

        if (ready && (ready = mCompacting && (mTotalPageCount > mCompactionTargetPageCount
                                              || mPageArray.getPageCount() > mTotalPageCount)))
        {
            // When locks are released, compaction is commit-ready. All pages in the compaction
            // zone are accounted for, but the reserve list's queue nodes might be in the valid
            // zone. Most pages will simply be discarded.
            mTotalPageCount = mCompactionTargetPageCount;
            upperBound = mTotalPageCount - 1;
        }

        mCompacting = false;
        mCompactionTargetPageCount = Long.MAX_VALUE;

        // Capture reserve list with full lock held to prevent allocPage from stealing from the
        // reserves. They're now off limits.
        PageQueue reserve = mReserveList;
        mReserveList = null;

        fullUnlock();
        commitLock.writeLock().unlock();

        if (reserve != null) {
            try {
                // Need to unlock first because fullUnlock didn't see it.
                reserve.appendLock().unlock();

                reserve.reclaim(mRemoveLock, upperBound);
            } finally {
                reserve.delete();
            }
        }

        return ready;
    }

    /**
     * Called after compaction, to actually shrink the file.
     */
    public boolean truncatePages() throws IOException {
        mRemoveLock.lock();
        try {
            if (mTotalPageCount < mPageArray.getPageCount()) {
                try {
                    mPageArray.setPageCount(mTotalPageCount);
                    return true;
                } catch (IllegalStateException e) {
                    // Snapshot in progress.
                    return false;
                }
            }
        } finally {
            mRemoveLock.unlock();
        }
        return false;
    }

    /**
     * Initiates the process for making page allocations and deletions permanent.
     *
     * @param header destination for writing page manager structures
     * @param offset offset into header
     */
    public void commitStart(/*P*/ byte[] header, int offset) throws IOException {
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
            PageOps.p_longPutLE(header, offset + I_TOTAL_PAGE_COUNT, mTotalPageCount);

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
    public void commitEnd(/*P*/ byte[] header, int offset) throws IOException {
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
        // acquires remove lock.
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
     * Method must be invoked with remove lock held.
     */
    boolean isPageOutOfBounds(long id) {
        return id <= 1 || id >= mTotalPageCount;
    }
}
