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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.File;
import java.io.IOException;

import java.nio.channels.ClosedChannelException;

import java.util.EnumSet;

import java.util.function.Supplier;

import org.cojen.tupl.DatabaseFullException;

import org.cojen.tupl.core.CheckedSupplier;

import org.cojen.tupl.diag.EventListener;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * {@link PageArray} implementation which accesses a fixed sized file, fully mapped to memory.
 *
 * @author Brian S O'Neill
 */
public abstract sealed class MappedPageArray extends PageArray
    permits PosixMappedPageArray, WindowsMappedPageArray
{
    private static final VarHandle cMappingAddrHandle, cCauseHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cMappingAddrHandle = lookup.findVarHandle
                (MappedPageArray.class, "mMappingAddr", long.class);

            cCauseHandle = lookup.findVarHandle
                (MappedPageArray.class, "mCause", Throwable.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private final long mPageCount;
    private final boolean mReadOnly;

    private volatile long mMappingAddr;
    private volatile Throwable mCause;

    public static boolean isSupported() {
        return ValueLayout.ADDRESS.byteSize() >= 8;
    }

    /**
     * @param file file to store pages, or null if anonymous
     * @param options can be null if none
     * @throws UnsupportedOperationException if not running on a 64-bit platform
     */
    public static Supplier<MappedPageArray> factory(int pageSize, long pageCount,
                                                    File file, EnumSet<OpenOption> options)
    {
        return factory(pageSize, pageCount, file, options, null);
    }

    /**
     * @param file file to store pages, or null if anonymous
     * @param options can be null if none
     * @param listener optional
     * @throws UnsupportedOperationException if not running on a 64-bit platform
     * @hidden
     */
    public static Supplier<MappedPageArray> factory(int pageSize, long pageCount,
                                                    File file, EnumSet<OpenOption> options,
                                                    EventListener listener)
    {
        if (pageSize < 1 || pageCount < 0 || pageCount > Long.MAX_VALUE / pageSize) {
            throw new IllegalArgumentException();
        }

        if (!isSupported()) {
            throw new UnsupportedOperationException("Not a 64-bit platform");
        }


        return (CheckedSupplier<MappedPageArray>) () -> {
            EnumSet<OpenOption> opts = options;
            if (opts == null) {
                opts = EnumSet.noneOf(OpenOption.class);
            }
            if (System.getProperty("os.name").startsWith("Windows")) {
                return new WindowsMappedPageArray(pageSize, pageCount, file, opts, listener);
            } else {
                return new PosixMappedPageArray(pageSize, pageCount, file, opts, listener);
            }
        };
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
    public void readPage(long index, long dstAddr, int offset, int length)
        throws IOException
    {
        readCheck(index);
        dstAddr += offset;
        long srcAddr = mappingAddr() + index * mPageSize;
        if (srcAddr != dstAddr) {
            MemorySegment.copy(DirectMapping.ALL, srcAddr, DirectMapping.ALL, dstAddr, length);
        }
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        writeCheck(index);
        srcAddr += offset;
        int pageSize = mPageSize;
        long dstAddr = mappingAddr() + index * pageSize;
        if (dstAddr != srcAddr) {
            MemorySegment.copy(DirectMapping.ALL, srcAddr, DirectMapping.ALL, dstAddr, pageSize);
        }
    }

    @Override
    public long directPageAddress(long index) throws IOException {
        readCheck(index);
        return mappingAddr() + index * mPageSize;
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        readCheck(srcIndex);
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long addr = mappingAddr();
        long dstAddr = addr + dstIndex * pageSize;
        MemorySegment.copy(DirectMapping.ALL, addr + srcIndex * pageSize,
                           DirectMapping.ALL, dstAddr, pageSize);

        return dstAddr;
    }

    @Override
    public long copyPageFromAddress(long srcAddr, long dstIndex) throws IOException {
        writeCheck(dstIndex);

        int pageSize = mPageSize;
        long dstAddr = mappingAddr() + dstIndex * pageSize;
        MemorySegment.copy(DirectMapping.ALL, srcAddr, DirectMapping.ALL, dstAddr, pageSize);

        return dstAddr;
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        doSync(mappingAddr(), metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        writeCheck(index);
        doSyncPage(mappingAddr(), index);
    }

    @Override
    public final void close(Throwable cause) throws IOException {
        while (true) {
            long addr = mMappingAddr;
            if (addr == 0) {
                return;
            }
            cCauseHandle.compareAndSet(this, null, cause);
            if (cMappingAddrHandle.compareAndSet(this, addr, 0)) {
                mCause = cause;
                doClose(addr);
                return;
            }
        }
    }

    @Override
    public final boolean isClosed() {
        return mMappingAddr == 0;
    }

    void setMappingAddr(long addr) throws IOException {
        while (!cMappingAddrHandle.compareAndSet(this, 0, addr)) {
            if (mMappingAddr != 0) {
                throw new IllegalStateException();
            }
        }
    }

    abstract void doSync(long mappingAddr, boolean metadata) throws IOException;

    abstract void doSyncPage(long mappingAddr, long index) throws IOException;

    abstract void doClose(long mappingAddr) throws IOException;

    long mappingAddr() throws IOException {
        long mappingAddr = mMappingAddr;
        if (mappingAddr == 0) {
            var cce = new ClosedChannelException();
            cce.initCause(mCause);
            throw cce;
        }
        return mappingAddr;
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
