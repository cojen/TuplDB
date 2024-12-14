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

import java.util.function.Supplier;

import org.cojen.tupl.core.CheckedSupplier;

import org.cojen.tupl.util.Runner;

/**
 * {@link PageArray} implementation which stripes pages in a <a
 * href="http://en.wikipedia.org/wiki/Raid_0#RAID_0">RAID 0</a> fashion.
 *
 * @author Brian S O'Neill
 */
public class StripedPageArray extends PageArray {
    private final PageArray[] mSources;
    private final int mDirectPageSize;
    private final boolean mReadOnly;

    private final Syncer[] mSyncers;

    @SafeVarargs
    public static Supplier<? extends PageArray> factory(Supplier<? extends PageArray>... factories)
    {
        if (factories.length <= 1) {
            return factories[0];
        }

        return (CheckedSupplier<PageArray>) () -> {
            var sources = new PageArray[factories.length];
            for (int i=0; i<sources.length; i++) {
                sources[i] = factories[i].get();
            }
            return new StripedPageArray(sources);
        };
    }

    public StripedPageArray(PageArray... sources) {
        super(pageSize(sources));
        mSources = sources;

        int directPageSize = sources[0].directPageSize();
        for (int i=1; i<sources.length; i++) {
            if (sources[i].directPageSize() != directPageSize) {
                directPageSize = pageSize();
                break;
            }
        }
        mDirectPageSize = directPageSize;

        boolean readOnly = false;

        for (PageArray pa : sources) {
            readOnly |= pa.isReadOnly();
        }

        mReadOnly = readOnly;

        mSyncers = new Syncer[sources.length - 1];

        for (int i=0; i<mSyncers.length; i++) {
            mSyncers[i] = new Syncer(sources[i]);
        }
    }

    private static int pageSize(PageArray... sources) {
        int pageSize = sources[0].pageSize();
        for (int i=1; i<sources.length; i++) {
            if (sources[i].pageSize() != pageSize) {
                throw new IllegalArgumentException("Inconsistent page sizes");
            }
        }
        return pageSize;
    }

    @Override
    public final int directPageSize() {
        return mDirectPageSize;
    }

    @Override
    public boolean isFullyMapped() {
        for (PageArray pa : mSources) {
            if (!pa.isFullyMapped()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() throws IOException {
        for (PageArray pa : mSources) {
            if (!pa.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long pageCount() throws IOException {
        long count = 0;
        for (PageArray pa : mSources) {
            count += pa.pageCount();
            if (count < 0) {
                return Long.MAX_VALUE;
            }
        }
        return count;
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        setPageCount(count, true);
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        setPageCount(count, false);
    }

    private void setPageCount(long count, boolean truncate) throws IOException {
        int stripes = mSources.length;
        // Divide among stripes, rounding up.
        count = (count + stripes - 1) / stripes;
        for (PageArray pa : mSources) {
            if (truncate) {
                pa.truncatePageCount(count);
            } else {
                pa.expandPageCount(count);
            }
        }
    }

    @Override
    public long pageCountLimit() throws IOException {
        long limit = -1;

        for (PageArray pa : mSources) {
            long subLimit = pa.pageCountLimit();
            if (subLimit >= 0) {
                limit = limit < 0 ? subLimit : Math.min(limit, subLimit);
            }
        }

        return limit < 0 ? limit : limit * mSources.length; 
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        sources[(int) (index % stripes)].readPage(index / stripes, dstAddr, offset, length);
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        sources[(int) (index % stripes)].writePage(index / stripes, srcAddr, offset);
    }

    @Override
    public long evictPage(long index, long bufAddr) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        return sources[(int) (index % stripes)].evictPage(index / stripes, bufAddr);
    }

    @Override
    public long directPageAddress(long index) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        return sources[(int) (index % stripes)].directPageAddress(index / stripes);
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;

        PageArray src = sources[(int) (srcIndex % stripes)];
        srcIndex /= stripes;

        PageArray dst = sources[(int) (dstIndex % stripes)];
        dstIndex /= stripes;

        if (src == dst) {
            return dst.copyPage(srcIndex, dstIndex);
        } else {
            return dst.copyPageFromAddress(src.directPageAddress(srcIndex), dstIndex);
        }
    }

    @Override
    public long copyPageFromAddress(long srcAddr, long dstIndex) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        return sources[(int) (dstIndex % stripes)].copyPageFromAddress(srcAddr, dstIndex / stripes);
    }

    @Override
    public synchronized void sync(boolean metadata) throws IOException {
        Syncer[] syncers = mSyncers;
        int i;
        for (i=0; i<syncers.length; i++) {
            Syncer syncer = syncers[i];
            syncer.reset(metadata);
            Runner.start(syncer);
        }

        mSources[i].sync(metadata);

        for (Syncer syncer : syncers) {
            syncer.check();
        }
    }

    @Override
    public void syncPage(long index) throws IOException {
        PageArray[] sources = mSources;
        int stripes = sources.length;
        sources[(int) (index % stripes)].syncPage(index / stripes);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        IOException ex = null;
        for (PageArray pa : mSources) {
            ex = Utils.closeQuietly(ex, pa, cause);
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public boolean isClosed() {
        for (PageArray pa : mSources) {
            if (pa.isClosed()) {
                return true;
            }
        }
        return false;
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
