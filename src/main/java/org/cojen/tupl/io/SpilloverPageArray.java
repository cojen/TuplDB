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

package org.cojen.tupl.io;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.concurrent.Future;

import java.util.function.Supplier;

import org.cojen.tupl.core.CheckedSupplier;

import org.cojen.tupl.util.Runner;

/**
 * Combines {@link PageArray PageArrays} together in a sequential fashion. This is useful for
 * supporting overflow capacity when the first array fills up. In an emergency when all drives
 * are full, a spillover page array can be defined such that the second array is a remote file,
 * or a {@link OpenOption#NON_DURABLE non-durable} file, or an anonymous {@link
 * MappedPageArray}. With this configuration in place, delete non-essential entries and then
 * {@link org.cojen.tupl.Database#compactFile compact} the database. After verifying that space
 * is available again, the original page array configuration can be swapped back in.
 *
 * @author Brian S O'Neill
 */
public class SpilloverPageArray extends PageArray {
    /**
     * @param first source for all pages lower than the spillover index
     * @param spilloverIndex index which separates the two sources
     * @param second source for all pages at or higher than the spillover index
     * @throws IllegalArgumentException if page sizes don't match or if spillover index isn't
     * greater than 0
     * @throws IllegalStateException if the highest index of the first source is higher than
     * the spillover index
     */
    public static Supplier<PageArray> factory(Supplier<? extends PageArray> first,
                                              long spilloverIndex,
                                              Supplier<? extends PageArray> second)
    {
        return (CheckedSupplier<PageArray>) () -> make(first.get(), spilloverIndex, second.get());
    }

    /**
     * @param first source for all pages lower than the spillover index
     * @param spilloverIndex index which separates the two sources
     * @param second source for all pages at or higher than the spillover index
     * @throws IllegalArgumentException if page sizes don't match or if spillover index isn't
     * greater than 0
     * @throws IllegalStateException if the highest index of the first source is higher than
     * the spillover index
     */
    public static PageArray make(PageArray first, long spilloverIndex, PageArray second)
        throws IOException
    {
        if (first.pageSize() != second.pageSize() || spilloverIndex <= 0) {
            throw new IllegalArgumentException();
        }
        long pageCount = first.pageCount();
        if (pageCount > spilloverIndex) {
            throw new IllegalStateException
                ("First page array is too large: " + pageCount + " > " + spilloverIndex);
        }
        return new SpilloverPageArray(first, spilloverIndex, second);
    }

    private final PageArray mFirst, mSecond;
    private final long mSpilloverIndex;
    private final int mDirectPageSize;
    private final boolean mReadOnly;

    private SpilloverPageArray(PageArray first, long spilloverIndex, PageArray second) {
        super(first.pageSize());
        mFirst = first;
        mSecond = second;
        mSpilloverIndex = spilloverIndex;

        int directPageSize = first.directPageSize();
        if (second.directPageSize() != directPageSize) {
            // Both must be direct to be fully direct.
            directPageSize = pageSize();
        }
        mDirectPageSize = directPageSize;

        mReadOnly = first.isReadOnly() || second.isReadOnly();
    }

    @Override
    public final int directPageSize() {
        return mDirectPageSize;
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isFullyMapped() {
        return mFirst.isFullyMapped() && mSecond.isFullyMapped();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mFirst.isEmpty() && mSecond.isEmpty();
    }

    @Override
    public long pageCount() throws IOException {
        return mSpilloverIndex + mSecond.pageCount();
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        long diff = count - mSpilloverIndex;
        if (diff > 0) {
            mSecond.truncatePageCount(diff);
        } else {
            mSecond.truncatePageCount(0);
            mFirst.truncatePageCount(count);
        }
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        long diff = count - mSpilloverIndex;
        if (diff > 0) {
            mSecond.expandPageCount(diff);
        } else {
            mFirst.expandPageCount(count);
        }
    }

    @Override
    public long pageCountLimit() throws IOException {
        long limit = mFirst.pageCountLimit();
        if (limit < 0 || limit >= mSpilloverIndex) {
            limit = mSecond.pageCountLimit();
            if (limit >= 0) {
                limit += mSpilloverIndex;
            }
        }
        return limit;
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        action(index, (pa, ix) -> pa.readPage(ix, dstAddr, offset, length));
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        action(index, (pa, ix) -> pa.writePage(ix, srcAddr, offset));
    }

    @Override
    public long evictPage(long index, long bufAddr) throws IOException {
        PageArray pa;
        if (index < mSpilloverIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mSpilloverIndex;
        }
        return pa.evictPage(index, bufAddr);
    }

    @Override
    public long directPageAddress(long index) throws IOException {
        PageArray pa;
        if (index < mSpilloverIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mSpilloverIndex;
        }
        return pa.directPageAddress(index);
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        PageArray src;
        if (srcIndex < mSpilloverIndex) {
            src = mFirst;
        } else {
            src = mSecond;
            srcIndex -= mSpilloverIndex;
        }

        PageArray dst;
        if (dstIndex < mSpilloverIndex) {
            dst = mFirst;
        } else {
            dst = mSecond;
            dstIndex -= mSpilloverIndex;
        }

        if (src == dst) {
            return dst.copyPage(srcIndex, dstIndex);
        } else {
            return dst.copyPageFromAddress(src.directPageAddress(srcIndex), dstIndex);
        }
    }

    @Override
    public long copyPageFromAddress(long srcAddr, long dstIndex) throws IOException {
        PageArray pa;
        if (dstIndex < mSpilloverIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            dstIndex -= mSpilloverIndex;
        }
        return pa.copyPageFromAddress(srcAddr, dstIndex);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        Future<?> task = Runner.current().submit(() -> {
            try {
                mFirst.sync(metadata);
            } catch (IOException e) {
                Utils.rethrow(e);
            }
        });

        Throwable ex = null;

        try {
            mSecond.sync(metadata);
        } catch (Throwable e) {
            ex = e;
        }

        try {
            task.get();
        } catch (Throwable e) {
            if (e instanceof InterruptedException) {
                e = new InterruptedIOException();
            } else {
                e = Utils.rootCause(e);
            }
            if (ex == null) {
                ex = e;
            } else {
                Utils.suppress(ex, e);
            }
        }

        if (ex != null) {
            Utils.rethrow(ex);
        }
    }

    @Override
    public void syncPage(long index) throws IOException {
        PageArray pa;
        if (index < mSpilloverIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mSpilloverIndex;
        }
        pa.syncPage(index);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        IOException ex = Utils.closeQuietly(null, mFirst, cause);
        ex = Utils.closeQuietly(ex, mSecond, cause);
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public boolean isClosed() {
        return mFirst.isClosed() || mSecond.isClosed();
    }

    private static interface Task {
        public void perform(PageArray pa, long index) throws IOException;
    }

    private void action(long index, Task task) throws IOException {
        PageArray pa;
        if (index < mSpilloverIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mSpilloverIndex;
        }
        task.perform(pa, index);
    }
}
