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

import java.util.BitSet;

import java.util.concurrent.locks.Lock;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PageAllocator extends PageQueue {
    /*

    Header structure is encoded as follows, in 48 bytes:

    +---------------------------+
    | queue header (40 bytes)   |
    | long: total page count    |
    +---------------------------+

    */

    private long mTotalPageCount;

    /**
     * Create a new PageAllocator.
     *
     * @param firstPageIndex first page index that can be allocated; must be at least 1
     */
    public PageAllocator(PageArray array, long firstPageIndex) throws IOException {
        super(array);
        if (firstPageIndex < 1) {
            throw new IllegalArgumentException("First page index: " + firstPageIndex);
        }
        mTotalPageCount = firstPageIndex;
    }

    /**
     * Create a restored PageAllocator.
     *
     * @param header source for reading allocator root structure
     * @param offset offset into header
     */
    public PageAllocator(PageArray array, byte[] header, int offset) throws IOException {
        super(array, header, offset);

        long totalPageCount = DataIO.readLong(header, offset + super.headerSize());

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

        mTotalPageCount = totalPageCount;
    }

    @Override
    public int headerSize() {
        return super.headerSize() + 8;
    }

    @Override
    public void commit(final byte[] header, int offset,
                       final CommitReady ready)
        throws IOException
    {
        super.commit(header, offset, new CommitReady() {
            public void ready(int newOffset) throws IOException {
                long totalPageCount = mTotalPageCount;

                if (totalPageCount < 1) {
                    throw new CorruptPageStoreException
                        ("Illegal total page count: " + totalPageCount);
                }

                DataIO.writeLong(header, newOffset, totalPageCount);
                ready.ready(newOffset + 8);
            }
        });
    }

    @Override
    void addTo(PageStore.Stats stats) {
        Lock lock = allocLock();
        lock.lock();
        try {
            stats.totalPages += mTotalPageCount;
            super.addTo(stats);
        } finally {
            lock.unlock();
        }
    }

    @Override
    long createPage(boolean grow) throws IOException {
        long id = mTotalPageCount++;
        pageArray().setPageCount(id + 1, grow);
        return id;
    }

    @Override
    boolean isPageOutOfBounds(long id) {
        return id < 1 || id >= mTotalPageCount;
    }

    @Override
    long allocQueuePage() throws IOException {
        return allocPage(false);
    }

    @Override
    void deleteQueuePage(long id) throws IOException {
        deletePage(id);
    }

    /**
     * Sets a bit for each page.
     */
    void markAllPages(BitSet pages, int scalar, int offset) throws IOException {
        Lock lock = allocLock();
        lock.lock();
        try {
            int limit = (Integer.MAX_VALUE - offset) / scalar;
            int total = (int) Math.min(mTotalPageCount, limit);
            for (int i=0; i<total; i++) {
                pages.set(i * scalar + offset);
            }
        } finally {
            lock.unlock();
        }
    }
}
