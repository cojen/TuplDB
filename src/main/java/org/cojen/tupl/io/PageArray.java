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

    public final boolean isDirectIO() {
        return directPageSize() < 0;
    }

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public final int pageSize() {
        return mPageSize;
    }

    /**
     * Returns a positive page size if not using direct I/O, else negate to get the page size
     * to allocate for direct I/O.
     */
    public int directPageSize() {
        return pageSize();
    }

    public boolean isFullyMapped() {
        return false;
    }

    public abstract boolean isReadOnly();

    public abstract boolean isEmpty() throws IOException;

    /**
     * Returns the total count of pages in the array, or Long.MAX_VALUE if not applicable.
     */
    public abstract long pageCount() throws IOException;

    /**
     * Attempt to truncate the total count of pages. Array implementation might not support
     * truncating the page count, in which case this method does nothing.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public abstract void truncatePageCount(long count) throws IOException;

    /**
     * Attempt to expand the total count of pages. Array implementation might not support
     * expanding the page count, in which case this method does nothing.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public abstract void expandPageCount(long count) throws IOException;

    /**
     * Return maximum allowed page count, or -1 if not applicable.
     */
    public long pageCountLimit() throws IOException {
        return -1;
    }

    /**
     * @param index zero-based page index to read
     * @param dstAddr receives read data
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, long dstAddr) throws IOException {
        readPage(index, dstAddr, 0, mPageSize);
    }

    /**
     * @param index zero-based page index to read
     * @param dstAddr receives read data
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public abstract void readPage(long index, long dstAddr, int offset, int length)
        throws IOException;

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if the index is
     * greater than or equal to the current page count. If array supports caching, page must be
     * immediately copied into it.
     *
     * @param index zero-based page index to write
     * @param srcAddr data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, long srcAddr) throws IOException {
        writePage(index, srcAddr, 0);
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if the index is
     * greater than or equal to the current page count. If array supports caching, page must be
     * immediately copied into it.
     *
     * @param index zero-based page index to write
     * @param srcAddr data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public abstract void writePage(long index, long srcAddr, int offset) throws IOException;

    /**
     * Same as writePage, except that the given buffer might be altered and a replacement might
     * be returned. Caller must not alter the original buffer if a replacement was provided,
     * and the contents of the replacement are undefined.
     *
     * @param index zero-based page index to write
     * @param bufAddr data to write; implementation might alter the contents
     * @throws IndexOutOfBoundsException if index is negative
     * @return replacement buffer, or same instance if replacement was not performed
     */
    public long evictPage(long index, long bufAddr) throws IOException {
        writePage(index, bufAddr);
        return bufAddr;
    }

    // Only expected to be called when isFullyMapped.
    public long directPageAddress(long index) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Indicate that the contents of the given page will be modified. Permits the
     * implementation to make a copy of the existing page contents, if it supports
     * snapshotting.
     *
     * @return direct pointer to destination
     */
    // Only expected to be called when isFullyMapped.
    public long dirtyPage(long index) throws IOException {
        return directPageAddress(index);
    }

    /**
     * @return direct pointer to destination
     */
    // Only expected to be called when isFullyMapped.
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @return direct pointer to destination
     */
    // Only expected to be called when isFullyMapped.
    public long copyPageFromAddress(long srcAddr, long dstIndex) throws IOException {
        throw new UnsupportedOperationException();
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

    public abstract boolean isClosed();
}
