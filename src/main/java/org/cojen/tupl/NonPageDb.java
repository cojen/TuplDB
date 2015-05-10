/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import static org.cojen.tupl.PageOps.*;

/**
 * PageDb implementation which doesn't actually work. Used for non-durable
 * databases.
 *
 * @author Brian S O'Neill
 */
final class NonPageDb extends PageDb {
    private final int mPageSize;
    private final PageCache mCache;

    private long mAllocId;

    /**
     * @param cache optional
     */
    NonPageDb(int pageSize, PageCache cache) {
        mPageSize = pageSize;
        mCache = cache;
        // Next assigned id is 2, the first legal identifier.
        mAllocId = 1;
    }

    @Override
    void delete() {
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    @Override
    public int allocMode() {
        return NodeUsageList.MODE_NO_EVICT;
    }

    @Override
    public Node allocLatchedNode(Database db, int mode) throws IOException {
        Node node = db.allocLatchedNode(Utils.randomSeed(), mode);
        long nodeId = node.mId;
        if (nodeId < 0) {
            // Recycle the id.
            nodeId = -nodeId;
        } else {
            nodeId = allocPage();
        }
        node.mId = nodeId;
        return node;
    }

    @Override
    public int pageSize() {
        return mPageSize;
    }

    @Override
    public long pageCount() {
        return 0;
    }

    @Override
    public Stats stats() {
        return new Stats();
    }

    @Override
    public BitSet tracePages() throws IOException {
        return new BitSet();
    }

    @Override
    public void readPage(long id, /*P*/ byte[] buf) throws IOException {
        readPage(id, buf, 0);
    }

    @Override
    public void readPage(long id, /*P*/ byte[] buf, int offset) throws IOException {
        PageCache cache = mCache;
        if (cache == null || !cache.remove(id, buf, offset, p_length(buf))) {
            fail(false);
        }
    }

    @Override
    public synchronized long allocPage() throws IOException {
        // Cached nodes and fragmented values always require unique identifiers.
        long id = mAllocId + 1;
        if (id > 0x0000_ffff_ffff_ffffL) {
            // Identifier is limited to 48-bit range.
            throw new DatabaseFullException();
        }
        mAllocId = id;
        return id;
    }

    @Override
    public void writePage(long id, /*P*/ byte[] buf) throws IOException {
        writePage(id, buf, 0);
    }

    @Override
    public void writePage(long id, /*P*/ byte[] buf, int offset) throws IOException {
        PageCache cache = mCache;
        if (cache == null || !cache.add(id, buf, offset, p_length(buf), false)) {
            fail(true);
        }
    }

    @Override
    public void cachePage(long id, /*P*/ byte[] buf) throws IOException {
        cachePage(id, buf, 0);
    }

    @Override
    public void cachePage(long id, /*P*/ byte[] buf, int offset) throws IOException {
        PageCache cache = mCache;
        if (cache != null && !cache.add(id, buf, offset, p_length(buf), false)) {
            fail(false);
        }
    }

    @Override
    public void uncachePage(long id) throws IOException {
        PageCache cache = mCache;
        if (cache != null) {
            cache.remove(id, p_null(), 0, 0);
        }
    }

    @Override
    public void deletePage(long id) throws IOException {
        uncachePage(id);
    }

    @Override
    public void recyclePage(long id) throws IOException {
        uncachePage(id);
    }

    @Override
    public long allocatePages(long pageCount) throws IOException {
        // Do nothing.
        return 0;
    }

    @Override
    public boolean compactionStart(long targetPageCount) throws IOException {
        return false;
    }

    @Override
    public boolean compactionScanFreeList() throws IOException {
        return false;
    }

    @Override
    public boolean compactionVerify() throws IOException {
        return false;
    }

    @Override
    public boolean compactionEnd() throws IOException {
        return false;
    }

    @Override
    public boolean truncatePages() throws IOException {
        return false;
    }

    @Override
    public int extraCommitDataOffset() {
        return 0;
    }

    @Override
    public void commit(boolean resume, /*P*/ byte[] header, CommitCallback callback)
        throws IOException
    {
        // This is more of an assertion failure.
        throw new DatabaseException("Cannot commit to a non-durable database");
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        Arrays.fill(extra, (byte) 0);
    }

    @Override
    public void close() {
        if (mCache != null) {
            mCache.close();
        }
    }

    @Override
    public void close(Throwable cause) {
        close();
    }

    private static void fail(boolean forWrite) throws DatabaseException {
        if (forWrite) {
            throw new DatabaseFullException();
        } else {
            // This is more of an assertion failure.
            throw new DatabaseException("Cannot read from a non-durable database");
        }
    }
}
