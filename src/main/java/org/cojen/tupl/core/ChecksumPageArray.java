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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.ChecksumException;

import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.unsafe.DirectAccess;
import org.cojen.tupl.unsafe.UnsafeAccess;

import org.cojen.tupl.util.LocalPool;

/**
 * A {@link PageArray} implementation which applies a 32-bit checksum to each page, stored in
 * the last 4 bytes of the page. If the source PageArray reports a page size of 4096 bytes, the
 * CheckedPageArray reports a page size of 4092 bytes.
 *
 * @author Brian S O'Neill
 */
abstract class ChecksumPageArray extends TransformedPageArray {
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

    static void check(long index, int storedChecksum, Checksum checksum) throws ChecksumException { 
        // Note that checksum failures of header pages (0 and 1) are ignored. StoredPageDb
        // performs an independent check and selects the correct header.
        if (storedChecksum != (int) checksum.getValue() && index > 1) {
            throw new ChecksumException(index, storedChecksum, (int) checksum.getValue());
        }
    }

    private static class Standard extends ChecksumPageArray {
        private final LocalPool<BufRef> mBufRefPool;

        Standard(PageArray source, Supplier<Checksum> supplier) {
            super(source, supplier);
            mBufRefPool = new LocalPool<>(() -> {
                ByteBuffer bb = ByteBuffer.allocateDirect(pageSize() + 4);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                return new BufRef(bb, mSupplier.get());
            }, -4);
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
                    long addr = readPageAndChecksum(e.get(), index, length);
                    UnsafeAccess.copy(addr, dst, offset, length);
                } finally {
                    e.release();
                }
            }
        }

        @Override
        public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
            int pageSize = pageSize();
            if (offset != 0 || length != pageSize()) {
                long page = UnsafeAccess.alloc(pageSize, false);
                try {
                    readPage(index, page);
                    UnsafeAccess.copy(page, dstPtr + offset, length);
                } finally {
                    UnsafeAccess.free(page);
                }
            } else {
                LocalPool.Entry<BufRef> e = mBufRefPool.access();
                try {
                    long addr = readPageAndChecksum(e.get(), index, length);
                    UnsafeAccess.copy(addr, dstPtr, length);
                } finally {
                    e.release();
                }
            }
        }

        /**
         * @param length must be equal to pageSize
         */
        private long readPageAndChecksum(BufRef ref, long index, int length) throws IOException {
            ByteBuffer buf = ref.mBuffer;
            long addr = ref.mAddress;
            mSource.readPage(index, addr);
            buf.position(0).limit(length);
            Checksum checksum = ref.mChecksum;
            checksum.reset();
            checksum.update(buf);
            buf.limit(buf.capacity());
            check(index, buf.getInt(), checksum);
            return addr;
        }

        @Override
        public void writePage(long index, byte[] src, int offset) throws IOException {
            LocalPool.Entry<BufRef> e = mBufRefPool.access();
            try {
                BufRef ref = e.get();
                Checksum checksum = ref.mChecksum;
                checksum.reset();
                int length = pageSize();
                checksum.update(src, offset, length);
                ByteBuffer buf = ref.mBuffer;
                buf.position(0).limit(buf.capacity());
                buf.put(src, offset, length);
                buf.putInt((int) checksum.getValue());
                mSource.writePage(index, ref.mAddress);
            } finally {
                e.release();
            }
        }

        @Override
        public void writePage(long index, long srcPtr, int offset) throws IOException {
            LocalPool.Entry<BufRef> e = mBufRefPool.access();
            try {
                BufRef ref = e.get();
                Checksum checksum = ref.mChecksum;
                checksum.reset();
                int length = pageSize();
                checksum.update(DirectAccess.ref(srcPtr + offset, length));
                ByteBuffer buf = ref.mBuffer;
                buf.limit(buf.capacity());
                UnsafeAccess.copy(srcPtr + offset, ref.mAddress, length);
                buf.putInt(length, (int) checksum.getValue());
                mSource.writePage(index, ref.mAddress);
            } finally {
                e.release();
            }
        }

        @Override
        public void close(Throwable cause) throws IOException {
            super.close(cause);
            mBufRefPool.clear(ref -> DirectAccess.delete(ref.mBuffer));
        }

        static class BufRef {
            final ByteBuffer mBuffer;
            final long mAddress;
            final Checksum mChecksum;

            BufRef(ByteBuffer buffer, Checksum checksum) {
                mBuffer = buffer;
                mAddress = DirectAccess.getAddress(buffer);
                mChecksum = checksum;
            }
        }
    }

    private static class Direct extends ChecksumPageArray {
        private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();

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
                int storedChecksum = Utils.decodeIntLE(dst, offset + mAbsPageSize - 4);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(dst, offset, length);
                check(index, storedChecksum, checksum);
            }
        }

        @Override
        public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
            if (offset != 0 || length != pageSize()) {
                long page = UnsafeAccess.alloc(mAbsPageSize, true); // aligned
                try {
                    readPage(index, page);
                    UnsafeAccess.copy(page, dstPtr + offset, length);
                } finally {
                    UnsafeAccess.free(page);
                }
            } else {
                // Assume that the caller has provided a buffer sized to match the direct page.
                mSource.readPage(index, dstPtr, offset, mAbsPageSize);
                int storedChecksum = UNSAFE.getInt(dstPtr + offset + mAbsPageSize - 4);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(DirectAccess.ref(dstPtr + offset, length));
                check(index, storedChecksum, checksum);
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
                checksum.update(src, offset, mAbsPageSize - 4);
                // Assume that the caller has provided a buffer sized to match the direct page.
                Utils.encodeIntLE(src, offset + mAbsPageSize - 4, (int) checksum.getValue());
                mSource.writePage(index, src, offset);
            }
        }

        @Override
        public void writePage(long index, long srcPtr, int offset) throws IOException {
            Checksum checksum = checksum();
            checksum.reset();
            checksum.update(DirectAccess.ref(srcPtr + offset, mAbsPageSize - 4));
            // Assume that the caller has provided a buffer sized to match the direct page.
            UNSAFE.putInt(srcPtr + offset + mAbsPageSize - 4, (int) checksum.getValue());
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
