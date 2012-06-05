/*
 *  Copyright 2012 Brian S O'Neill
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

/**
 * PageDb implementation which doesn't actually work. Used for non-durable
 * databases.
 *
 * @author Brian S O'Neill
 */
class NonPageDb extends PageDb {
    private final int mPageSize;

    private long mAllocId;

    NonPageDb(int pageSize) {
        mPageSize = pageSize;
        // Next assigned id is 2, the first legal identifier.
        mAllocId = 1;
    }

    public int pageSize() {
        return mPageSize;
    }

    public Stats stats() {
        return new Stats();
    }

    public BitSet tracePages() throws IOException {
        return new BitSet();
    }

    public void readPage(long id, byte[] buf) throws IOException {
        fail(false);
    }

    public void readPage(long id, byte[] buf, int offset) throws IOException {
        fail(false);
    }

    public void readPartial(long id, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        fail(false);
    }

    public synchronized long allocPage() throws IOException {
        // For ordinary nodes, the same identifier can be vended out each
        // time. Fragmented values require unique identifiers.
        long id = mAllocId + 1;
        if (id == 0) {
            // Wrapped around. In practice, this will not happen in 100 years.
            throw new DatabaseException("All page identifiers exhausted");
        }
        mAllocId = id;
        return id;
    }

    public long tryAllocPage() throws IOException {
        return 0;
    }

    public long allocPageCount() {
        return 0;
    }

    public void writePage(long id, byte[] buf) throws IOException {
        fail(true);
    }

    public void writePage(long id, byte[] buf, int offset) throws IOException {
        fail(true);
    }

    public void deletePage(long id) throws IOException {
        // Do nothing.
    }

    public void allocatePages(long pageCount) throws IOException {
        // Do nothing.
    }

    public void commit(final CommitCallback callback) throws IOException {
        fail(false);
    }

    public void readExtraCommitData(byte[] extra) throws IOException {
        Arrays.fill(extra, (byte) 0);
    }

    public void close() throws IOException {
        // Do nothing.
    }

    private static void fail(boolean forWrite) throws DatabaseException {
        String message = "Non-durable database";
        if (forWrite) {
            message += " is full";
        }
        throw new DatabaseException(message);
    }
}
