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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.ChecksumException;

import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.util.LocalPool;

/**
 * A {@link PageArray} implementation which applies a 32-bit checksum to each page, stored in
 * the last 4 bytes of the page. If the source PageArray reports a page size of 4096 bytes, the
 * CheckedPageArray reports a page size of 4092 bytes.
 *
 * @author Brian S O'Neill
 */
abstract class ChecksumPageArray extends TransformedPageArray {
    private static final ValueLayout.OfInt INT_LE;

    static {
        INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    }

    static ChecksumPageArray open(PageArray source, Supplier<? extends Checksum> supplier) {
        return source.isDirectIO() ? new Direct(source, supplier) : new Standard(source, supplier);
    }

    final Supplier<? extends Checksum> mSupplier;

    ChecksumPageArray(PageArray source, Supplier<? extends Checksum> supplier) {
        super(source.pageSize() - 4, source); // need 4 bytes for the checksum
        mSupplier = supplier;
    }

    @Override
    public boolean isReadOnly() {
        return mSource.isReadOnly();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public long pageCount() throws IOException {
        return mSource.pageCount();
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        mSource.truncatePageCount(count);
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        mSource.expandPageCount(count);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        mSource.syncPage(index);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mSource.close(cause);
    }

    static void check(long index, int storedChecksum, Checksum checksum) throws ChecksumException { 
        // Note that checksum failures of header pages (0 and 1) are ignored. StoredPageDb
        // performs an independent check and selects the correct header.
        if (storedChecksum != (int) checksum.getValue() && index > 1) {
            throw new ChecksumException(index, storedChecksum, (int) checksum.getValue());
        }
    }

    private static class Standard extends ChecksumPageArray {
        private final LocalPool<BufRef> mBufRefPool;

        Standard(PageArray source, Supplier<? extends Checksum> supplier) {
            super(source, supplier);
            mBufRefPool = new LocalPool<>(this::allocateBufRef, -4);
        }

        private BufRef allocateBufRef() {
            return new BufRef(Arena.ofAuto().allocate(pageSize() + 4), mSupplier.get());
        }

        @Override
        public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
            int pageSize = pageSize();
            if (offset != 0 || length != pageSize()) {
                try (Arena a = Arena.ofConfined()) {
                    MemorySegment page = a.allocate(pageSize);
                    readPage(index, page.address());
                    MemorySegment.copy(page, 0, DirectMemory.ALL, dstAddr + offset, length);
                }
            } else {
                LocalPool.Entry<BufRef> e = mBufRefPool.access();
                try {
                    MemorySegment page = readPageAndChecksum(e.get(), index, length);
                    MemorySegment.copy(page, 0, DirectMemory.ALL, dstAddr + offset, length);
                } finally {
                    e.release();
                }
            }
        }

        /**
         * @param length must be equal to pageSize (which is source pageSize - 4)
         * @return MemorySegment to copy page from
         */
        private MemorySegment readPageAndChecksum(BufRef ref, long index, int length)
            throws IOException
        {
            MemorySegment ms = ref.mPagePlusCRC;
            mSource.readPage(index, ms.address());
            Checksum checksum = ref.mChecksum;
            checksum.reset();
            checksum.update(ref.mBuffer.position(0).limit(length));
            check(index, ms.get(INT_LE, length), checksum);
            return ms;
        }

        @Override
        public void writePage(long index, long srcAddr, int offset) throws IOException {
            LocalPool.Entry<BufRef> e = mBufRefPool.access();
            try {
                BufRef ref = e.get();
                int length = pageSize();
                MemorySegment.copy(DirectMemory.ALL, srcAddr + offset, ref.mPagePlusCRC, 0, length);
                writePageAndChecksum(ref, index, length);
            } finally {
                e.release();
            }
        }

        /**
         * @param length must be equal to pageSize (which is source pageSize - 4)
         */
        private void writePageAndChecksum(BufRef ref, long index, int length) throws IOException {
            Checksum checksum = ref.mChecksum;
            checksum.reset();
            checksum.update(ref.mBuffer.position(0).limit(length));
            MemorySegment ms = ref.mPagePlusCRC;
            ms.set(INT_LE, length, (int) checksum.getValue());
            mSource.writePage(index, ms.address());
        }

        @Override
        public void close(Throwable cause) throws IOException {
            super.close(cause);
            mBufRefPool.clear(null);
        }

        static class BufRef {
            final MemorySegment mPagePlusCRC;
            final ByteBuffer mBuffer;
            final Checksum mChecksum;

            BufRef(MemorySegment ms, Checksum checksum) {
                mPagePlusCRC = ms;
                mBuffer = ms.asByteBuffer();
                mChecksum = checksum;
            }
        }
    }

    private static class Direct extends ChecksumPageArray {
        private final int mAbsPageSize;
        private final LocalPool<? extends Checksum> mLocalChecksum;

        Direct(PageArray source, Supplier<? extends Checksum> supplier) {
            super(source, supplier);
            mAbsPageSize = Math.abs(source.directPageSize());
            mLocalChecksum = new LocalPool<>(mSupplier, -4);
        }

        @Override
        public int directPageSize() {
            // Expected to be negative.
            return mSource.directPageSize();
        }

        @Override
        public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
            if (offset != 0 || length != pageSize()) {
                try (Arena a = Arena.ofConfined()) {
                    MemorySegment ms = a.allocate(mAbsPageSize, SysInfo.pageSize());
                    readPage(index, ms.address());
                    MemorySegment.copy(ms, 0, DirectMemory.ALL, dstAddr + offset, length);
                }
            } else {
                // Assume that the caller has provided a buffer sized to match the direct page.
                int pageSize = mAbsPageSize;
                mSource.readPage(index, dstAddr, offset, pageSize);
                MemorySegment ms = MemorySegment.ofAddress(dstAddr + offset).reinterpret(pageSize);
                int storedChecksum = ms.get(INT_LE, length);
                LocalPool.Entry<? extends Checksum> entry = mLocalChecksum.access();
                try {
                    Checksum checksum = entry.get();
                    checksum.reset();
                    checksum.update(ms.asByteBuffer().limit(length));
                    check(index, storedChecksum, checksum);
                } finally {
                    entry.release();
                }
            }
        }

        @Override
        public void writePage(long index, long srcAddr, int offset) throws IOException {
            // Assume that the caller has provided a buffer sized to match the direct page.
            int pageSize = mAbsPageSize;
            MemorySegment ms = MemorySegment.ofAddress(srcAddr + offset).reinterpret(pageSize);
            pageSize -= 4;
            LocalPool.Entry<? extends Checksum> entry = mLocalChecksum.access();
            try {
                Checksum checksum = entry.get();
                checksum.reset();
                checksum.update(ms.asByteBuffer().limit(pageSize));
                ms.set(INT_LE, pageSize, (int) checksum.getValue());
            } finally {
                entry.release();
            }
            mSource.writePage(index, srcAddr, offset);
        }
    }
}
