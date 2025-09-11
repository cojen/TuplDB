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
import java.io.OutputStream;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.nio.channels.ClosedChannelException;

import java.util.function.Supplier;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.PageCompressor;

import org.cojen.tupl.util.LocalPool;

/**
 * PageArray implementation which compresses pages and stores them into another database.
 *
 * @author Brian S O'Neill
 */
final class CompressedPageArray extends PageArray implements Supplier<PageCompressor>, Compactable {
    private final LocalDatabase mDatabase;
    private final Index mPages;
    private final Supplier<? extends PageCompressor> mCompressorFactory;
    private final LocalPool<PageCompressor> mCompressors;

    /**
     * @param fullPageSize full size of pages when uncompressed
     */
    CompressedPageArray(int fullPageSize, LocalDatabase db, Index pages,
                        Supplier<? extends PageCompressor> factory)
    {
        super(fullPageSize);
        mDatabase = db;
        mPages = pages;
        mCompressorFactory = factory;
        mCompressors = new LocalPool<>(this);
    }

    // Required by Supplier.
    @Override
    public PageCompressor get() {
        if (isClosed()) {
            throw Utils.rethrow(new ClosedChannelException());
        }
        return mCompressorFactory.get();
    }

    public boolean isCacheOnly() {
        return mDatabase.isCacheOnly();
    }

    @Override
    public boolean isReadOnly() {
        return mDatabase.isReadOnly();
    }

    @Override
    public boolean isEmpty() throws IOException {
        Cursor c = mPages.newCursor(Transaction.BOGUS);
        c.first();
        boolean isEmpty = c.key() == null;
        c.reset();
        return isEmpty;
    }

    @Override
    public long pageCount() throws IOException {
        Cursor c = mPages.newCursor(Transaction.BOGUS);
        c.last();
        byte[] key = c.key();
        c.reset();
        return key == null ? 0 : (indexFor(key) + 1);
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        Cursor c = mPages.newCursor(null);
        try {
            c.autoload(false);
            for (c.findGe(keyFor(count)); c.key() != null; c.next()) {
                c.store(null);
            }
        } finally {
            c.reset();
        }
    }

    @Override
    public boolean compact(double target) throws IOException {
        return mDatabase.compactFile(null, target);
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        // Nothing to do.
    }

    @Override
    public void readPage(long index, long dstAddr) throws IOException {
        readPage(index, dstAddr, 0);
    }

    private void readPage(long index, long dstAddr, int offset) throws IOException {
        byte[] value = mPages.load(Transaction.BOGUS, keyFor(index));
        if (value == null) {
            MemorySegment.ofAddress(dstAddr + offset).reinterpret(pageSize()).fill((byte) 0);
        } else {
            var entry = mCompressors.access();
            try {
                entry.get().decompress(value, 0, value.length, dstAddr, offset, pageSize());
            } finally {
                entry.release();
            }
        }
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        int pageSize = pageSize();
        if (length == pageSize) {
            readPage(index, dstAddr, offset);
        } else {
            try (Arena a = Arena.ofConfined()) {
                MemorySegment page = a.allocate(pageSize);
                readPage(index, page.address(), 0);
                MemorySegment.copy(page, 0, DirectMemory.ALL, dstAddr + offset, length);
            }
        }
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        try (Cursor c = mPages.newAccessor(Transaction.BOGUS, keyFor(index))) {
            var entry = mCompressors.access();
            try {
                PageCompressor compressor = entry.get();
                int len = compressor.compress(srcAddr, offset, pageSize());
                c.valueWrite(0, compressor.compressedBytes(), 0, len);
                c.valueLength(len);
            } finally {
                entry.release();
            }
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        // No need to do anything. See syncPage.
    }

    @Override
    public void syncPage(long index) throws IOException {
        // This method is only called by StoredPageDb to finish a checkpoint, after writing
        // the new header page. Prior to writing the header page it calls the regular sync
        // method to ensure proper ordering of the writes. Because the underlying storage here
        // is another database instance, checkpointing achieves atomic durability, thus
        // ensuring that the new header isn't visible before everything else.
        mDatabase.checkpoint();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        try {
            mDatabase.close(cause);
        } finally {
            mCompressors.clear(PageCompressor::close);
        }
    }

    @Override
    public boolean isClosed() {
        return mDatabase.isClosed();
    }

    Snapshot beginSnapshot() throws IOException {
        Snapshot snap = mDatabase.beginSnapshot();

        /*
          Need to provide the correct redo position in the snapshot, but extracting this from
          the header page is tricky due to the layering. The information extracted from the
          header might reside in the cache only, and might not be in the underlying file
          yet. This race condition exists when a checkpoint is running concurrently.

          One strategy is for StoredPageDb.commitHeader to hold mHeaderLatch while syncPage is
          called, but this can stall the start of a snapshot for a long time waiting for the
          syncPage to finish. The syncPage method as implemented in this class performs a
          checkpoint of the underlying compressed database, which can take a long time.

          The solution is to extract the header information from the snapshot by opening the
          database from the snapshot itself. This is more complicated than the usual strategy,
          but it avoids race conditions and stalls.
         */

        long redoPos;
        {
            var snapArray = ((ReadableSnapshot) snap).asPageArray();

            var launcher = new Launcher();
            launcher.mBasicMode = true;
            launcher.dataPageArray(snapArray);
            launcher.minCacheSize(0);
            launcher.maxCacheSize(100L * snapArray.pageSize());
            launcher.readOnly(true);
            launcher.encrypt(mDatabase.dataCrypto());
            launcher.checksumPages(mDatabase.checksumFactory());

            try (var snapDb = launcher.open(false, null)) {
                snapArray = new CompressedPageArray
                    (pageSize(), snapDb, snapDb.registry(), mCompressorFactory);
                var snapPageDb = StoredPageDb.open(null, snapArray, null, null, false, 0);
                redoPos = snapPageDb.snapshotRedoPos();
            }
        }

        return new Snapshot() {
            @Override
            public long length() {
                return snap.length();
            }

            @Override
            public long position() {
                return redoPos;
            }

            @Override
            public boolean isCompressible() {
                return false;
            }

            @Override
            public void writeTo(OutputStream out) throws IOException {
                snap.writeTo(out);
            }

            @Override
            public void close() throws IOException {
                snap.close();
            }
        };
    }

    private static byte[] keyFor(long index) {
        byte[] key = new byte[6];
        Utils.encodeInt48BE(key, 0, index);
        return key;
    }

    private static long indexFor(byte[] key) {
        return Utils.decodeUnsignedInt48BE(key, 0);
    }
}
