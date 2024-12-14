/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.Snapshot;

import org.cojen.tupl.io.PageArray;

/**
 * Allows the active snapshot to be accessed via a PageArray, which can then be used to open it
 * into a Database instance. This isn't very efficient, but it can be useful for extracting
 * information about the snapshot.
 *
 * @author Brian S O'Neill
 */
interface ReadableSnapshot extends Snapshot {
    int pageSize();

    long pageCount();

    void readPage(long index, long dstAddr, int offset, int length) throws IOException;

    /**
     * Returns a PageArray view which when closed doesn't close the underlying snapshot. Is
     * only valid before writing the snapshot begins, since it will delete copied pages as it
     * goes.
     */
    default PageArray asPageArray() {
        return new PageArray(pageSize()) {
            @Override
            public boolean isReadOnly() {
                return true;
            }

            @Override
            public boolean isEmpty() throws IOException {
                return false;
            }

            @Override
            public long pageCount() throws IOException {
                return ReadableSnapshot.this.pageCount();
            }

            @Override
            public void truncatePageCount(long count) throws IOException {
                // Ignore.
            }

            @Override
            public void expandPageCount(long count) throws IOException {
                // Ignore.
            }

            @Override
            public void readPage(long index, long dstAddr, int offset, int length)
                throws IOException
            {
                ReadableSnapshot.this.readPage(index, dstAddr, offset, length);
            }

            @Override
            public void writePage(long index, long srcAddr, int offset) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void sync(boolean metadata) throws IOException {
                // Ignore.
            }

            @Override
            public void close(Throwable cause) {
                // Ignore.
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        };
    }
}
