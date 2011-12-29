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
import java.io.Flushable;
import java.io.IOException;

/**
 * Defines a pesistent, array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit index, starting at zero.
 *
 * @author Brian S O'Neill
 */
interface PageArray extends Closeable, Flushable {
    public boolean isReadOnly();

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public int pageSize();

    /**
     * Returns the total count of pages in the array.
     */
    public long getPageCount() throws IOException;

    /**
     * Set the total count of pages, truncating or growing the array as necessary.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public void setPageCount(long count) throws IOException;

    /**
     * Force all pages to be allocated. Page allocation is automatic, but it
     * might be delayed until a write is actually performed.
     */
    public void allocatePages() throws IOException;

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, byte[] buf) throws IOException;

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, byte[] buf, int offset) throws IOException;

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
        throws IOException;

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, byte[] buf) throws IOException;

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, byte[] buf, int offset) throws IOException;

    /**
     * Durably flushes all writes to the underlying device.
     */
    @Override
    public void flush() throws IOException;
}
