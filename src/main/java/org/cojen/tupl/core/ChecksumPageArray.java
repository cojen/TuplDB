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

import org.cojen.tupl.io.FileIO;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.LocalPool;

/**
 * A {@link PageArray} implementation which applies a 32-bit checksum to each page, stored in
 * the last 4 bytes of the page. If the source PageArray reports a page size of 4096 bytes, the
 * CheckedPageArray reports a page size of 4092 bytes.
 *
 * @author Brian S O'Neill
 */
abstract class ChecksumPageArray extends TransformedPageArray {
    private static final ValueLayout.OfInt CRC_LAYOUT =
        ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);

    static ChecksumPageArray open(PageArray source, Supplier<Checksum> supplier) {
        return source.isDirectIO() ? new Direct(source, supplier) : new Standard(source, supplier);
    }

    final Supplier<Checksum> mSupplier;

    ChecksumPageArray(PageArray source, Supplier<Checksum> supplier) {
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

    @Override
    public PageArray open() throws IOException {
        PageArray array = mSource.open();
        return array == mSource ? this : ChecksumPageArray.open(array, mSupplier);
    }

    static void check(long index, int actualChecksum, Checksum checksum) throws IOException { 
        if (actualChecksum != (int) checksum.getValue()) {
            throw new IOException
                ("Checksum mismatch: " + Integer.toUnsignedString(actualChecksum, 16) + " != "
                 + Integer.toUnsignedString((int) checksum.getValue(), 16) + "; page=" + index);
        }
    }

    private static class Standard extends ChecksumPageArray {
        private final LocalPool<BufRef> mBufRefPool;

        Standard(PageArray source, Supplier<Checksum> supplier) {
            super(source, supplier);
            mBufRefPool = new LocalPool<>(this::allocateBufRef, -4);
        }

        private BufRef allocateBufRef() {
            return new BufRef(Arena.ofAuto().allocate(pageSize() + 4), mSupplier.get());
        }

        @Override
        public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
            int pageSize = pageSize();
            if (offset != 0 || length != pageSize) {
                byte[] page = new byte[pageSize];
                readPage(index, page);
                System.arraycopy(page, 0, dst, offset, length);
            } else {
                LocalPool.Entry<BufRef> e = mBufRefPool.access();
                try {
                    MemorySegment page = readPageAndChecksum(e.get(), index, length);
                    MemorySegment.copy(page, ValueLayout.JAVA_BYTE, 0, dst, offset, length);
                } finally {
                    e.release();
                }
            }
        }

        @Override
        public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
            int pageSize = pageSize();
            if (offset != 0 || length != pageSize()) {
                try (Arena a = Arena.ofConfined()) {
                    MemorySegment page = a.allocate(pageSize);
                    readPage(index, page.address());
                    MemorySegment.copy
                        (page, 0,
                         MemorySegment.ofAddress(dstPtr + offset).reinterpret(length), 0, length);
                }
            } else {
                LocalPool.Entry<BufRef> e = mBufRefPool.access();
                try {
                    MemorySegment page = readPageAndChecksum(e.get(), index, length);
                    MemorySegment.copy
                        (page, 0,
                         MemorySegment.ofAddress(dstPtr + offset).reinterpret(length), 0, length);
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
            check(index, ms.get(CRC_LAYOUT, length), checksum);
            return ms;
        }

        @Override
        public void writePage(long index, byte[] src, int offset) throws IOException {
            LocalPool.Entry<BufRef> e = mBufRefPool.access();
            try {
                BufRef ref = e.get();
                int length = pageSize();
                MemorySegment.copy(src, offset,
                                   ref.mPagePlusCRC, ValueLayout.JAVA_BYTE, 0, length);
                writePageAndChecksum(ref, index, length);
            } finally {
                e.release();
            }
        }

        @Override
        public void writePage(long index, long srcPtr, int offset) throws IOException {
            LocalPool.Entry<BufRef> e = mBufRefPool.access();
            try {
                BufRef ref = e.get();
                int length = pageSize();
                MemorySegment.copy(MemorySegment.ofAddress(srcPtr + offset).reinterpret(length), 0,
                                   ref.mPagePlusCRC, 0, length);
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
            ms.set(CRC_LAYOUT, length, (int) checksum.getValue());
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
        private final ThreadLocal<Checksum> mLocalChecksum;

        Direct(PageArray source, Supplier<Checksum> supplier) {
            super(source, supplier);
            mAbsPageSize = Math.abs(source.directPageSize());
            mLocalChecksum = new ThreadLocal<>();
        }

        @Override
        public int directPageSize() {
            // Expected to be negative.
            return mSource.directPageSize();
        }

        @Override
        public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
            if (offset != 0 || length != pageSize()) {
                byte[] page = new byte[mAbsPageSize];
                readPage(index, page);
                System.arraycopy(page, 0, dst, offset, length);
            } else {
                // Assume that the caller has provided a buffer sized to match the direct page.
                mSource.readPage(index, dst, offset, mAbsPageSize);
                int actualChecksum = Utils.decodeIntLE(dst, offset + length);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(dst, offset, length);
                check(index, actualChecksum, checksum);
            }
        }

        @Override
        public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
            if (offset != 0 || length != pageSize()) {
                try (Arena a = Arena.ofConfined()) {
                    MemorySegment ms = a.allocate(mAbsPageSize, FileIO.osPageSize());
                    readPage(index, ms.address());
                    MemorySegment.copy
                        (ms, 0,
                         MemorySegment.ofAddress(dstPtr + offset).reinterpret(length), 0, length);
                }
            } else {
                // Assume that the caller has provided a buffer sized to match the direct page.
                int pageSize = mAbsPageSize;
                mSource.readPage(index, dstPtr, offset, pageSize);
                MemorySegment ms = MemorySegment.ofAddress(dstPtr + offset).reinterpret(pageSize);
                int actualChecksum = ms.get(CRC_LAYOUT, length);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(ms.asByteBuffer().limit(length));
                check(index, actualChecksum, checksum);
            }
        }

        @Override
        public void writePage(long index, byte[] src, int offset) throws IOException {
            if (offset != 0) {
                byte[] page = new byte[mAbsPageSize];
                System.arraycopy(src, offset, page, 0, page.length);
                writePage(index, page);
            } else {
                Checksum checksum = checksum();
                checksum.reset();
                // Assume that the caller has provided a buffer sized to match the direct page.
                int pageSize = mAbsPageSize - 4;
                checksum.update(src, offset, pageSize);
                Utils.encodeIntLE(src, offset + pageSize, (int) checksum.getValue());
                mSource.writePage(index, src, offset);
            }
        }

        @Override
        public void writePage(long index, long srcPtr, int offset) throws IOException {
            Checksum checksum = checksum();
            checksum.reset();
            // Assume that the caller has provided a buffer sized to match the direct page.
            int pageSize = mAbsPageSize;
            MemorySegment ms = MemorySegment.ofAddress(srcPtr + offset).reinterpret(pageSize);
            pageSize -= 4;
            checksum.update(ms.asByteBuffer().limit(pageSize));
            ms.set(CRC_LAYOUT, pageSize, (int) checksum.getValue());
            mSource.writePage(index, srcPtr, offset);
        }

        private Checksum checksum() {
            Checksum checksum = mLocalChecksum.get();
            if (checksum == null) {
                checksum = mSupplier.get();
                mLocalChecksum.set(checksum);
            }
            return checksum;
        }
    }
}
