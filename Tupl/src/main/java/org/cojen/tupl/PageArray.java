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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import java.util.EnumSet;

/**
 * Defines a pesistent, array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit index, starting at zero.
 *
 * @author Brian S O'Neill
 */
class PageArray implements Closeable {
    private final int mPageSize;
    private final FileIO mFio;

    PageArray(int pageSize, File file, EnumSet<OpenOption> options) throws IOException {
        this(pageSize, JavaFileIO.open(file, options));
    }

    PageArray(int pageSize, FileIO fio) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
        }
        mPageSize = pageSize;
        mFio = fio;
    }

    public boolean isReadOnly() {
        return mFio.isReadOnly();
    }

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public int pageSize() {
        return mPageSize;
    }

    public boolean isEmpty() throws IOException {
        return mFio.length() == 0;
    }

    /**
     * Returns the total count of pages in the array.
     */
    public long getPageCount() throws IOException {
        // Always round page count down. A partial last page effectively doesn't exist.
        return mFio.length() / mPageSize;
    }

    /**
     * Set the total count of pages, truncating or growing the array as necessary.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public void setPageCount(long count) throws IOException {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }
        if (isReadOnly()) {
            return;
        }
        mFio.setLength(count * mPageSize);
    }

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, byte[] buf) throws IOException {
        readPage(index, buf, 0);
    }

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        int pageSize = mPageSize;
        mFio.read(index * pageSize, buf, offset, pageSize);
    }

    /**
     * @param index zero-based page index to read
     * @param start start of page to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length length to read
     * @return actual length read
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        int pageSize = mPageSize;
        int remaining = pageSize - start;
        length = remaining <= 0 ? 0 : Math.min(remaining, length);
        mFio.read(index * pageSize + start, buf, offset, length);
        return length;
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, byte[] buf) throws IOException {
        writePage(index, buf, 0);
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        int pageSize = mPageSize;
        mFio.write(index * pageSize, buf, offset, pageSize);
    }

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public void sync(boolean metadata) throws IOException {
        mFio.sync(metadata);
    }

    public void close() throws IOException {
        mFio.close();
    }
}
