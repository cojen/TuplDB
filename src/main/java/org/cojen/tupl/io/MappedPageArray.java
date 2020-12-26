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

package org.cojen.tupl.io;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteBuffer;

import java.io.File;
import java.io.IOException;

import java.nio.channels.ClosedChannelException;

import java.util.EnumSet;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import org.cojen.tupl.DatabaseFullException;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * {@link PageArray} implementation which accesses a fixed sized file, fully mapped to memory.
 *
 * @author Brian S O'Neill
 */
public abstract class MappedPageArray extends PageArray {
    private static final VarHandle cMappingPtrHandle, cCauseHandle;

    static {
        try {
            cMappingPtrHandle =
                MethodHandles.lookup().findVarHandle
                (MappedPageArray.class, "mMappingPtr", long.class);

            cCauseHandle =
                MethodHandles.lookup().findVarHandle
                (MappedPageArray.class, "mCause", Throwable.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private final long mPageCount;
    private final boolean mReadOnly;

    private volatile long mMappingPtr;
    private volatile Throwable mCause;

    public static boolean isSupported() {
        return Native.SIZE_T_SIZE >= 8;
    }

    /**
     * @param file file to store pages, or null if anonymous
     * @throws UnsupportedOperationException if not running on a 64-bit platform
     */
    public static MappedPageArray open(int pageSize, long pageCount,
                                       File file, EnumSet<OpenOption> options)
        throws IOException
    {
        if (pageSize < 1 || pageCount < 0 || pageCount > Long.MAX_VALUE / pageSize) {
            throw new IllegalArgumentException();
        }

        if (!isSupported()) {
            throw new UnsupportedOperationException("Not a 64-bit platform");
        }

        if (options == null) {
            options = EnumSet.noneOf(OpenOption.class);
        }

        if (Platform.isWindows()) {
            return new WindowsMappedPageArray(pageSize, pageCount, file, options);
        } else {
            return new PosixMappedPageArray(pageSize, pageCount, file, options);
        }
    }

    MappedPageArray(int pageSize, long pageCount, EnumSet<OpenOption> options) {
        super(pageSize);
        mPageCount = pageCount;
        mReadOnly = options.contains(OpenOption.READ_ONLY);
    }

    @Override
    public final boolean isFullyMapped() {
        return true;
    }

    @Override
    public final boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() {
        return pageCount() == 0;
    }

    @Override
    public long pageCount() {
        return mPageCount;
    }

    @Override
    public void truncatePageCount(long count) {
    }

    @Override
    public void expandPageCount(long count) {
    }

    @Override
    public long pageCountLimit() {
        return mPageCount;
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length)
        throws IOException
    {
        readCheck(index);
        UnsafeAccess.copy(mappingPtr() + index * mPageSize, dst, offset, length);
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length, ByteBuffer tail)
        throws IOException
    {
        readCheck(index);
        long srcPtr = mappingPtr() + index * mPageSize;
        UnsafeAccess.copy(srcPtr, dst, offset, length);
        readTail(srcPtr + length, tail);
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length)
        throws IOException
    {
        readCheck(index);
        dstPtr += offset;
        long srcPtr = mappingPtr() + index * mPageSize;
        if (srcPtr != dstPtr) {
            UnsafeAccess.copy(srcPtr, dstPtr, length);
        }
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length, ByteBuffer tail)
        throws IOException
    {
        readCheck(index);
        dstPtr += offset;
        long srcPtr = mappingPtr() + index * mPageSize;
        if (srcPtr != dstPtr) {
            UnsafeAccess.copy(srcPtr, dstPtr, length);
        }
        readTail(srcPtr + length, tail);
    }

    private static void readTail(long srcPtr, ByteBuffer tail) {
        int rem = tail.remaining();
        int pos = tail.position();
        if (tail.isDirect()) {
            UnsafeAccess.copy(srcPtr, DirectAccess.getAddress(tail) + pos, rem);
        } else {
            UnsafeAccess.copy(srcPtr, tail.array(), tail.arrayOffset() + pos, rem);
        }
        tail.position(pos + rem);
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        writeCheck(index);
        int pageSize = mPageSize;
        UnsafeAccess.copy(src, offset, mappingPtr() + index * pageSize, pageSize);
    }

    @Override
    public void writePage(long index, byte[] src, int offset, ByteBuffer tail)
        throws IOException
    {
        writeCheck(index);
        int pageSize = mPageSize;
        long dstPtr = mappingPtr() + index * pageSize;
        int length = pageSize - tail.remaining();
        UnsafeAccess.copy(src, offset, dstPtr, length);
        writeTail(dstPtr + length, tail);
    }

    @Override
    public void writePage(long index, long srcPtr, int offset) throws IOException {
        writeCheck(index);
        srcPtr += offset;
        int pageSize = mPageSize;
        long dstPtr = mappingPtr() + index * pageSize;
        if (dstPtr != srcPtr) {
            UnsafeAccess.copy(srcPtr, dstPtr, pageSize);
        }
    }

    @Override
    public void writePage(long index, long srcPtr, int offset, ByteBuffer tail)
        throws IOException
    {
        writeCheck(index);
        srcPtr += offset;
        int pageSize = mPageSize;
        long dstPtr = mappingPtr() + index * pageSize;
        int length = pageSize - tail.remaining();
        if (dstPtr != srcPtr) {
            UnsafeAccess.copy(srcPtr, dstPtr, length);
        }
        writeTail(dstPtr + length, tail);
    }

    private static void writeTail(long dstPtr, ByteBuffer tail) {
        int rem = tail.remaining();
        int pos = tail.position();
        if (tail.isDirect()) {
            UnsafeAccess.copy(DirectAccess.getAddress(tail) + pos, dstPtr, rem);
        } else {
            UnsafeAccess.copy(tail.array(), tail.arrayOffset() + pos, dstPtr, rem);
        }
        tail.position(pos + rem);
    }

    @Override
    public long directPagePointer(long index) throws IOException {
        readCheck(index);
        return mappingPtr() + index * mPageSize;
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        readCheck(srcIndex);
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long ptr = mappingPtr();
        long dstPtr = ptr + dstIndex * pageSize;
        UnsafeAccess.copy(ptr + srcIndex * pageSize, dstPtr, pageSize);

        return dstPtr;
    }

    @Override
    public long copyPageFromPointer(long srcPointer, long dstIndex) throws IOException {
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long dstPtr = mappingPtr() + dstIndex * pageSize;
        UnsafeAccess.copy(srcPointer, dstPtr, pageSize);

        return dstPtr;
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        doSync(mappingPtr(), metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        writeCheck(index);
        doSyncPage(mappingPtr(), index);
    }

    @Override
    public final void close(Throwable cause) throws IOException {
        while (true) {
            long ptr = mMappingPtr;
            if (ptr == 0) {
                return;
            }
            cCauseHandle.compareAndSet(this, null, cause);
            if (cMappingPtrHandle.compareAndSet(this, ptr, 0)) {
                mCause = cause;
                doClose(ptr);
                return;
            }
        }
    }

    @Override
    public MappedPageArray open() throws IOException {
        return mMappingPtr == 0 ? doOpen() : this;
    }

    void setMappingPtr(long ptr) throws IOException {
        while (!cMappingPtrHandle.compareAndSet(this, 0, ptr)) {
            if (mMappingPtr != 0) {
                throw new IllegalStateException();
            }
        }
    }

    abstract MappedPageArray doOpen() throws IOException;

    abstract void doSync(long mappingPtr, boolean metadata) throws IOException;

    abstract void doSyncPage(long mappingPtr, long index) throws IOException;

    abstract void doClose(long mappingPtr) throws IOException;

    long mappingPtr() throws IOException {
        long mappingPtr = mMappingPtr;
        if (mappingPtr == 0) {
            var cce = new ClosedChannelException();
            cce.initCause(mCause);
            throw cce;
        }
        return mappingPtr;
    }

    private void readCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new IOException("Page index too high: " + index + " > " + mPageCount);
        }
    }

    private void writeCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new DatabaseFullException
                ("Mapped file length limit reached: " + (mPageCount * mPageSize));
        }
    }
}
