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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.Arrays;
import java.util.Random;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DatabaseFullException;

import org.cojen.tupl.ext.Crypto;

/**
 * PageDb implementation which doesn't actually store anything into a file or page array.
 *
 * @author Brian S O'Neill
 */
final class NonPageDb extends PageDb {
    private final int mPageSize;

    private final AtomicLong mAllocId;
    private final LongAdder mFreePageCount;

    private final long mDatabaseId;

    NonPageDb(int pageSize) {
        mPageSize = pageSize;

        // Next assigned id is 2, the first legal identifier.
        mAllocId = new AtomicLong(1);
        mFreePageCount = new LongAdder();

        mDatabaseId = generateDatabaseId(new Random());
    }

    @Override
    long databaseId() {
        return mDatabaseId;
    }

    @Override
    void pageCache(LocalDatabase db) {
    }

    @Override
    Crypto dataCrypto() {
        return null;
    }
 
    @Override
    Supplier<Checksum> checksumFactory() {
        return null;
    }

    @Override
    void delete() {
    }

    @Override
    boolean isCacheOnly() {
        return true;
    }

    @Override
    public int directPageSize() {
        return pageSize();
    }

    @Override
    public int allocMode() {
        return NodeGroup.MODE_NO_EVICT;
    }

    @Override
    public Node allocLatchedNode(LocalDatabase db, int mode) throws IOException {
        Node node = db.allocLatchedNode(mode);
        long nodeId = node.id();
        if (nodeId < 0) {
            // Recycle the id.
            nodeId = -nodeId;
            mFreePageCount.decrement();
        } else {
            nodeId = allocPage();
        }
        node.id(nodeId);
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
    public void pageLimit(long limit) {
        // Ignored.
    }

    @Override
    public long pageLimit() {
        // No explicit limit.
        return -1;
    }

    @Override
    public void pageLimitOverride(long limit) {
        // Ignored.
    }

    @Override
    public Stats stats() {
        var stats = new Stats();
        stats.freePages = Math.max(0, mFreePageCount.sum());
        stats.totalPages = Math.max(stats.freePages, mAllocId.get());
        return stats;
    }

    @Override
    public boolean requiresCommit() {
        return false;
    }

    @Override
    public void readPage(long id, long pageAddr) throws IOException {
        fail(false);
    }

    @Override
    public long allocPage() throws IOException {
        // Cached nodes and fragmented values always require unique identifiers.
        long id = mAllocId.incrementAndGet();
        if (id > 0x0000_ffff_ffff_ffffL) {
            // Identifier is limited to 48-bit range.
            mAllocId.decrementAndGet();
            throw new DatabaseFullException();
        }
        return id;
    }

    @Override
    public void writePage(long id, long pageAddr) throws IOException {
        fail(true);
    }

    @Override
    public long evictPage(long id, long pageAddr) throws IOException {
        writePage(id, pageAddr);
        return pageAddr;
    }

    @Override
    public void deletePage(long id, boolean force) throws IOException {
        mFreePageCount.increment();
    }

    @Override
    public void recyclePage(long id) throws IOException {
        deletePage(id, true);
    }

    @Override
    public long allocatePages(long pageCount) throws IOException {
        // Do nothing.
        return 0;
    }

    @Override
    public void scanFreeList(LongConsumer dst) throws IOException {
        // No stored pages to scan.
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
    public void compactionReclaim() throws IOException {
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
    public void commit(boolean resume, long headerAddr, CommitCallback callback)
        throws IOException
    {
        // This is more of an assertion failure.
        throw new DatabaseException("Cannot commit to a non-stored database");
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        Arrays.fill(extra, (byte) 0);
    }

    @Override
    public void close() {
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
            throw new DatabaseException("Cannot read from a non-stored database");
        }
    }
}
