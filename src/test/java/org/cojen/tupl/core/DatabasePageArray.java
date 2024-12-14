/*
 *  Copyright (C) 2019 Cojen.org
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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.io.IOException;

import java.util.function.BooleanSupplier;

import org.cojen.tupl.*;

import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.core.DirectMemory.ALL;

/**
 * A simple PageArray backed by a Database, for testing purposes.
 *
 * @author Brian S O'Neill
 */
class DatabasePageArray extends PageArray {
    private final Database mDb;
    private final Index mPages;

    private volatile BooleanSupplier mFailureChecker;

    DatabasePageArray(int pageSize, Database db) throws IOException {
        this(pageSize, db, db.openIndex("pages"));
    }

    DatabasePageArray(int pageSize, Database db, Index pages) {
        super(pageSize);
        mDb = db;
        mPages = pages;
    }

    /**
     * @param checker return true to fail the write
     */
    void enableWriteFailures(BooleanSupplier checker) {
        mFailureChecker = checker;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isEmpty() throws IOException {
        try (Cursor c = mPages.newCursor(Transaction.BOGUS)) {
            c.first();
            return c.key() == null;
        }
    }

    @Override
    public long pageCount() throws IOException {
        try (Cursor c = mPages.newCursor(Transaction.BOGUS)) {
            c.last();
            byte[] key = c.key();
            return key == null ? 0 : (indexFor(key) + 1);
        }
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        try (Cursor c = mPages.newCursor(Transaction.BOGUS)) {
            c.autoload(false);
            for (c.findGe(keyFor(count)); c.key() != null; c.next()) {
                c.store(null);
            }
        }
    }

    @Override
    public void expandPageCount(long count) throws IOException {
    }

    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        try (ValueAccessor accessor = mPages.newAccessor(Transaction.BOGUS, keyFor(index))) {
            accessor.valueRead(0, dst, offset, length);
        }
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        var buf = new byte[length];
        readPage(index, buf, 0, length);
        MemorySegment.copy(buf, 0, ALL, ValueLayout.JAVA_BYTE, dstAddr, length);
    }

    public void writePage(long index, byte[] buf, int offset) throws IOException {
        BooleanSupplier checker = mFailureChecker;
        if (checker != null && checker.getAsBoolean()) {
            throw new WriteFailureException("test failure");
        }

        try (ValueAccessor accessor = mPages.newAccessor(Transaction.BOGUS, keyFor(index))) {
            accessor.valueWrite(0, buf, offset, pageSize());
        }
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        var buf = new byte[pageSize()];
        MemorySegment.copy(ALL, ValueLayout.JAVA_BYTE, srcAddr + offset, buf, 0, buf.length);
        writePage(index, buf, 0);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        // Pages are written non-transactionally, and so a checkpoint must be performed.
        mDb.checkpoint();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mDb.close(cause);
    }

    @Override
    public boolean isClosed() {
        return mDb.isClosed();
    }

    private static byte[] keyFor(long index) {
        var key = new byte[6];
        Utils.encodeInt48BE(key, 0, index);
        return key;
    }

    private static long indexFor(byte[] key) {
        return Utils.decodeUnsignedInt48BE(key, 0);
    }
}
