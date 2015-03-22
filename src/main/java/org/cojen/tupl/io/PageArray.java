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

package org.cojen.tupl.io;

import java.io.IOException;

/**
 * Defines a persistent, array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit index, starting at zero.
 *
 * @author Brian S O'Neill
 */
public abstract class PageArray implements CauseCloseable {
    final int mPageSize;

    protected PageArray(int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
        }
        mPageSize = pageSize;
    }

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public final int pageSize() {
        return mPageSize;
    }

    public abstract boolean isReadOnly();

    public abstract boolean isEmpty() throws IOException;

    /**
     * Returns the total count of pages in the array or Long.MAX_VALUE if not applicable.
     */
    public abstract long getPageCount() throws IOException;

    /**
     * Set the total count of pages, truncating or growing the array as necessary. Array
     * implementation might not support setting the page count, in which case this method does
     * nothing.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public abstract void setPageCount(long count) throws IOException;

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, /*P*/ byte[] buf) throws IOException {
        readPage(index, buf, 0, mPageSize);
    }

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public abstract void readPage(long index, /*P*/ byte[] buf, int offset, int length)
        throws IOException;

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if the index is
     * greater than or equal to the current page count. If array supports caching, page must be
     * immediately copied into it.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, /*P*/ byte[] buf) throws IOException {
        writePage(index, buf, 0);
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if the index is
     * greater than or equal to the current page count. If array supports caching, page must be
     * immediately copied into it.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public abstract void writePage(long index, /*P*/ byte[] buf, int offset) throws IOException;

    /**
     * If supported, copies a page into the cache, but does not write it. Cached copy can be
     * removed when read again or be evicted sooner. Default implementation does nothing.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     */
    public void cachePage(long index, /*P*/ byte[] buf) throws IOException {
        cachePage(index, buf, 0);
    }

    /**
     * If supported, copies a page into the cache, but does not write it. Cached copy can be
     * removed when read again or be evicted sooner. Default implementation does nothing.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     */
    public void cachePage(long index, /*P*/ byte[] buf, int offset) throws IOException {
    }

    /**
     * If supported, removes a page from the cache. Default implementation does nothing.
     *
     * @param index zero-based page index to write
     */
    public void uncachePage(long index) throws IOException {
    }

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public abstract void sync(boolean metadata) throws IOException;

    /**
     * Durably flushes the page at the given index, but implementation might flush more pages.
     * File metadata is not flushed.
     */
    public void syncPage(long index) throws IOException {
        sync(false);
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    /**
     * @param cause null if close is not caused by a failure
     */
    @Override
    public abstract void close(Throwable cause) throws IOException;
}
