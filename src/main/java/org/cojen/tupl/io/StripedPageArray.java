/*
 *  Copyright 2012-2013 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
    private final boolean mReadOnly;

    private final ExecutorService mSyncService;
    private final Syncer[] mSyncers;

    public StripedPageArray(PageArray... arrays) {
        super(pageSize(arrays));
        mArrays = arrays;
        boolean readOnly = false;
        for (PageArray pa : arrays) {
            readOnly |= pa.isReadOnly();
        }
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
    public void readPage(long index, /*P*/ byte[] buf, int offset, int length) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].readPage(index / stripes, buf, offset, length);
    }

    @Override
    public void writePage(long index, /*P*/ byte[] buf, int offset) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].writePage(index / stripes, buf, offset);
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
