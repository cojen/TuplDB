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

import org.cojen.tupl.util.Runner;

/**
 * Joins {@link PageArray PageArrays} together in a sequential fashion. This is useful for
 * supporting overflow capacity when the first array fills up. In an emergency when all drives
 * are full, a joined page array can be defined such that the second array is a remote file, or
 * a {@link OpenOption#NON_DURABLE non-durable} file, or an anonymous {@link MappedPageArray}.
 * With this configuration in place, delete non-essential entries and then {@link
 * org.cojen.tupl.Database#compactFile compact} the database. After verifying that space is
 * available again, the original page array configuration can be swapped back in.
 *
 * @author Brian S O'Neill
 */
public class JoinedPageArray extends PageArray {
    /**
     * @param first array for all pages lower than the join index
     * @param joinIndex join index which separates the two page arrays
     * @param second array for all pages at or higher than the join index
     * @throws IllegalArgumentException if page sizes don't match or if join index isn't
     * greater than 0
     * @throws IllegalStateException if the highest index of the first array is higher than the
     * join index
     */
    public static PageArray join(PageArray first, long joinIndex, PageArray second)
        throws IOException
    {
        if (first.pageSize() != second.pageSize() || joinIndex <= 0) {
            throw new IllegalArgumentException();
        }
        long pageCount = first.pageCount();
        if (pageCount > joinIndex) {
            throw new IllegalStateException
                ("First page array is too large: " + pageCount + " > " + joinIndex);
        }
        return new JoinedPageArray(first, joinIndex, second);
    }

    private final PageArray mFirst, mSecond;
    private final long mJoinIndex;
    private final int mDirectPageSize;
    private final boolean mReadOnly;

    private JoinedPageArray(PageArray first, long joinIndex, PageArray second) {
        super(first.pageSize());
        mFirst = first;
        mSecond = second;
        mJoinIndex = joinIndex;

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
        return mJoinIndex + mSecond.pageCount();
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        long diff = count - mJoinIndex;
        if (diff > 0) {
            mSecond.truncatePageCount(diff);
        } else {
            mSecond.truncatePageCount(0);
            mFirst.truncatePageCount(count);
        }
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        long diff = count - mJoinIndex;
        if (diff > 0) {
            mSecond.expandPageCount(diff);
        } else {
            mFirst.expandPageCount(count);
        }
    }

    @Override
    public long pageCountLimit() throws IOException {
        long limit = mFirst.pageCountLimit();
        if (limit < 0 || limit >= mJoinIndex) {
            limit = mSecond.pageCountLimit();
            if (limit >= 0) {
                limit += mJoinIndex;
            }
        }
        return limit;
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        action(index, (pa, ix) -> pa.readPage(ix, dst, offset, length));
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
        action(index, (pa, ix) -> pa.readPage(ix, dstPtr, offset, length));
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        action(index, (pa, ix) -> pa.writePage(ix, src, offset));
    }

    @Override
    public void writePage(long index, long srcPtr, int offset) throws IOException {
        action(index, (pa, ix) -> pa.writePage(ix, srcPtr, offset));
    }

    @Override
    public byte[] evictPage(long index, byte[] buf) throws IOException {
        PageArray pa;
        if (index < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mJoinIndex;
        }
        return pa.evictPage(index, buf);
    }

    @Override
    public long evictPage(long index, long bufPtr) throws IOException {
        PageArray pa;
        if (index < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mJoinIndex;
        }
        return pa.evictPage(index, bufPtr);
    }

    @Override
    public long directPagePointer(long index) throws IOException {
        PageArray pa;
        if (index < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mJoinIndex;
        }
        return pa.directPagePointer(index);
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        PageArray src;
        if (srcIndex < mJoinIndex) {
            src = mFirst;
        } else {
            src = mSecond;
            srcIndex -= mJoinIndex;
        }

        PageArray dst;
        if (dstIndex < mJoinIndex) {
            dst = mFirst;
        } else {
            dst = mSecond;
            dstIndex -= mJoinIndex;
        }

        if (src == dst) {
            return dst.copyPage(srcIndex, dstIndex);
        } else {
            return dst.copyPageFromPointer(src.directPagePointer(srcIndex), dstIndex);
        }
    }

    @Override
    public long copyPageFromPointer(long srcPointer, long dstIndex) throws IOException {
        PageArray pa;
        if (dstIndex < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            dstIndex -= mJoinIndex;
        }
        return pa.copyPageFromPointer(srcPointer, dstIndex);
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
        if (index < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mJoinIndex;
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

    private static interface Task {
        public void perform(PageArray pa, long index) throws IOException;
    }

    private void action(long index, Task task) throws IOException {
        PageArray pa;
        if (index < mJoinIndex) {
            pa = mFirst;
        } else {
            pa = mSecond;
            index -= mJoinIndex;
        }
        task.perform(pa, index);
    }
}
