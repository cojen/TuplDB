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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@link PageArray} implementation which stripes pages in a <a
 * href="http://en.wikipedia.org/wiki/Raid_0#RAID_0">RAID 0</a> fashion.
 *
 * @author Brian S O'Neill
 */
public class StripedPageArray extends PageArray {
    private final PageArray[] mArrays;
    private final boolean mDirect;
    private final boolean mReadOnly;

    private final ExecutorService mSyncService;
    private final Syncer[] mSyncers;

    public StripedPageArray(PageArray... arrays) {
        super(pageSize(arrays));
        mArrays = arrays;
        boolean direct = false, readOnly = false;
        for (PageArray pa : arrays) {
            direct |= pa.isDirectIO();
            readOnly |= pa.isReadOnly();
        }
        mDirect = direct;
        mReadOnly = readOnly;

        mSyncService = Executors.newCachedThreadPool(new NamedThreadFactory("Syncer"));
        mSyncers = new Syncer[arrays.length - 1];

        for (int i=0; i<mSyncers.length; i++) {
            mSyncers[i] = new Syncer(arrays[i]);
        }
    }

    private static int pageSize(PageArray... arrays) {
        int pageSize = arrays[0].pageSize();
        for (int i=1; i<arrays.length; i++) {
            if (arrays[i].pageSize() != pageSize) {
                throw new IllegalArgumentException("Inconsistent page sizes");
            }
        }
        return pageSize;
    }

    @Override
    public final boolean isDirectIO() {
        return mDirect;
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() throws IOException {
        for (PageArray pa : mArrays) {
            if (!pa.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getPageCount() throws IOException {
        long count = 0;
        for (PageArray pa : mArrays) {
            count += pa.getPageCount();
            if (count < 0) {
                return Long.MAX_VALUE;
            }
        }
        return count;
    }

    @Override
    public void setPageCount(long count) throws IOException {
        int stripes = mArrays.length;
        // Divide among stripes, rounding up.
        count = (count + stripes - 1) / stripes;
        for (PageArray pa : mArrays) {
            pa.setPageCount(count);
        }
    }

    @Override
    public long getPageCountLimit() throws IOException {
        long limit = -1;

        for (PageArray pa : mArrays) {
            long subLimit = pa.getPageCountLimit();
            if (subLimit >= 0) {
                limit = limit < 0 ? subLimit : Math.min(limit, subLimit);
            }
        }

        return limit < 0 ? limit : limit * mArrays.length; 
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].readPage(index / stripes, dst, offset, length);
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].readPage(index / stripes, dstPtr, offset, length);
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].writePage(index / stripes, src, offset);
    }

    @Override
    public void writePage(long index, long srcPtr, int offset) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].writePage(index / stripes, srcPtr, offset);
    }

    @Override
    public byte[] evictPage(long index, byte[] buf) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        return arrays[(int) (index % stripes)].evictPage(index / stripes, buf);
    }

    @Override
    public long evictPage(long index, long bufPtr) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        return arrays[(int) (index % stripes)].evictPage(index / stripes, bufPtr);
    }

    @Override
    public long directPagePointer(long index) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        return arrays[(int) (index % stripes)].directPagePointer(index / stripes);
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;

        PageArray src = arrays[(int) (srcIndex % stripes)];
        srcIndex /= stripes;

        PageArray dst = arrays[(int) (dstIndex % stripes)];
        dstIndex /= stripes;

        if (src == dst) {
            return dst.copyPage(srcIndex, dstIndex);
        } else {
            return dst.copyPageFromPointer(src.directPagePointer(srcIndex), dstIndex);
        }
    }

    @Override
    public long copyPageFromPointer(long srcPointer, long dstIndex) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        return arrays[(int) (dstIndex % stripes)]
            .copyPageFromPointer(srcPointer, dstIndex / stripes);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        Syncer[] syncers = mSyncers;
        int i;
        for (i=0; i<syncers.length; i++) {
            Syncer syncer = syncers[i];
            syncer.reset(metadata);
            try {
                mSyncService.execute(syncer);
            } catch (RejectedExecutionException e) {
                if (mSyncService.isShutdown()) {
                    return;
                }
                throw new IOException(e);
            }
        }

        mArrays[i].sync(metadata);

        for (Syncer syncer : syncers) {
            syncer.check();
        }
    }

    @Override
    public void syncPage(long index) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].syncPage(index / stripes);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mSyncService.shutdown();
        IOException ex = null;
        for (PageArray pa : mArrays) {
            ex = Utils.closeQuietly(ex, pa, cause);
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public StripedPageArray open() throws IOException {
        if (!mSyncService.isShutdown()) {
            return this;
        }
        for (int i=0; i<mArrays.length; i++) {
            mArrays[i] = mArrays[i].open();
        }
        return new StripedPageArray(mArrays);
    }

    private static class Syncer implements Runnable {
        private final PageArray mArray;

        private boolean mMetadata;
        private boolean mFinished;
        private Throwable mException;

        Syncer(PageArray pa) {
            mArray = pa;
        }

        @Override
        public synchronized void run() {
            try {
                mArray.sync(mMetadata);
            } catch (Throwable e) {
                mException = e;
            } finally {
                mFinished = true;
                notify();
            }
        }

        synchronized void reset(boolean metadata) {
            mMetadata = metadata;
            mFinished = false;
            mException = null;
        }

        synchronized void check() throws IOException {
            try {
                while (!mFinished) {
                    wait();
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            Throwable e = mException;
            if (e != null) {
                throw new IOException(e.toString(), e);
            }
        }
    }
}
