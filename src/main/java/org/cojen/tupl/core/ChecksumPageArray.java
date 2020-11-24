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

import org.cojen.tupl.io.DirectAccess;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.UnsafeAccess;
import org.cojen.tupl.io.Utils;

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
    public void writePage(long index, byte[] src, int offset, ByteBuffer tail)
        throws IOException
    {
        // No need to support this unless double checksumming, which makes no sense.
        throw new UnsupportedOperationException();
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
        private final ThreadLocal<BufRef> mBufRef;

        Standard(PageArray source, Supplier<Checksum> supplier) {
            super(source, supplier);
            mBufRef = new ThreadLocal<>();
        }

        @Override
        public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
            int pageSize = pageSize();
            if (offset != 0 || length != pageSize()) {
                byte[] page = new byte[pageSize()];
                readPage(index, page);
                System.arraycopy(page, 0, dst, offset, length);
            } else {
                BufRef ref = bufRef();
                ByteBuffer tail = ref.mBuffer;
                tail.position(0);
                mSource.readPage(index, dst, offset, length, tail);
                int actualChecksum = tail.getInt(0);
                Checksum checksum = ref.mChecksum;
                checksum.reset();
                checksum.update(dst, offset, length);
                check(index, actualChecksum, checksum);
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
                BufRef ref = bufRef();
                ByteBuffer tail = ref.mBuffer;
                tail.position(0);
                mSource.readPage(index, dstPtr, offset, length, tail);
                int actualChecksum = tail.getInt(0);
                Checksum checksum = ref.mChecksum;
                checksum.reset();
                checksum.update(DirectAccess.ref(dstPtr + offset, length));
                check(index, actualChecksum, checksum);
            }
        }

        @Override
        public void writePage(long index, byte[] src, int offset) throws IOException {
            BufRef ref = bufRef();
            Checksum checksum = ref.mChecksum;
            checksum.reset();
            checksum.update(src, offset, pageSize());
            ByteBuffer tail = ref.mBuffer;
            tail.position(0);
            tail.putInt(0, (int) checksum.getValue());
            mSource.writePage(index, src, offset, tail);
        }

        @Override
        public void writePage(long index, long srcPtr, int offset) throws IOException {
            BufRef ref = bufRef();
            Checksum checksum = ref.mChecksum;
            checksum.reset();
            checksum.update(DirectAccess.ref(srcPtr + offset, pageSize()));
            ByteBuffer tail = ref.mBuffer;
            tail.position(0);
            tail.putInt(0, (int) checksum.getValue());
            mSource.writePage(index, srcPtr, offset, tail);
        }

        private BufRef bufRef() {
            BufRef ref = mBufRef.get();
            if (ref == null) {
                ByteBuffer bb = ByteBuffer.allocateDirect(4);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                Checksum checksum = mSupplier.get();
                ref = new BufRef(bb, checksum);
                mBufRef.set(ref);
            }
            return ref;
        }

        static class BufRef {
            final ByteBuffer mBuffer;
            final Checksum mChecksum;

            BufRef(ByteBuffer buffer, Checksum checksum) {
                mBuffer = buffer;
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
                int actualChecksum = Utils.decodeIntLE(dst, offset + mAbsPageSize - 4);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(dst, offset, length);
                check(index, actualChecksum, checksum);
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
                int actualChecksum = UNSAFE.getInt(dstPtr + offset + mAbsPageSize - 4);
                Checksum checksum = checksum();
                checksum.reset();
                checksum.update(DirectAccess.ref(dstPtr + offset, length));
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
