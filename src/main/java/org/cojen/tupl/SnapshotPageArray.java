/*
 *  Copyright 2013 Brian S O'Neill
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

package org.cojen.tupl;

import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import static java.lang.System.arraycopy;

import org.cojen.tupl.io.CauseCloseable;
import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SnapshotPageArray extends PageArray {
    private final PageArray mSource;
    private final PageArray mRawSource;

    private Object mSnapshots;

    SnapshotPageArray(PageArray source, PageArray rawSource) {
        super(source.pageSize());
        mSource = source;
        // Snapshot does not decrypt pages.
        mRawSource = source;
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
    public long getPageCount() throws IOException {
        return mSource.getPageCount();
    }

    @Override
    public void setPageCount(long count) throws IOException {
        mSource.setPageCount(count);
    }

    @Override
    public void readPage(long index, byte[] buf) throws IOException {
        mSource.readPage(index, buf);
    }

    @Override
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        mSource.readPage(index, buf, offset);
    }

    @Override
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        return mSource.readPartial(index, start, buf, offset, length);
    }

    @Override
    public void writePage(long index, byte[] buf) throws IOException {
        prepareToWrite(index);
        mSource.writePage(index, buf);
    }

    @Override
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        prepareToWrite(index);
        mSource.writePage(index, buf, offset);
    }

    private void prepareToWrite(long index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        Object obj = mSnapshots;
        if (obj != null) {
            if (obj instanceof SnapshotImpl) {
                ((SnapshotImpl) obj).capture(index);
            } else for (SnapshotImpl snapshot : (SnapshotImpl[]) obj) {
                snapshot.capture(index);
            }
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mSource.close(cause);
    }

    /**
     * Supports writing a snapshot of the array, while still permitting
     * concurrent access. Snapshot data is not a valid array file. It must be
     * processed specially by the restoreFromSnapshot method.
     *
     * @param pageCount total number of pages to include in snapshot
     */
    Snapshot beginSnapshot(TempFileManager tfm, long pageCount) throws IOException {
        pageCount = Math.min(pageCount, getPageCount());
        SnapshotImpl snapshot = new SnapshotImpl(tfm, pageCount);

        synchronized (this) {
            Object obj = mSnapshots;
            if (obj == null) {
                mSnapshots = snapshot;
            } else if (obj instanceof SnapshotImpl[]) {
                SnapshotImpl[] snapshots = (SnapshotImpl[]) obj;
                SnapshotImpl[] newSnapshots = new SnapshotImpl[snapshots.length + 1];
                arraycopy(snapshots, 0, newSnapshots, 0, snapshots.length);
                newSnapshots[newSnapshots.length - 1] = snapshot;
                mSnapshots = newSnapshots;
            } else {
                mSnapshots = new SnapshotImpl[] {(SnapshotImpl) obj, snapshot};
            }
        }

        return snapshot;
    }

    synchronized void unregister(SnapshotImpl snapshot) {
        Object obj = mSnapshots;
        if (obj == snapshot) {
            mSnapshots = null;
            return;
        }
        if (!(obj instanceof SnapshotImpl[])) {
            return;
        }

        SnapshotImpl[] snapshots = (SnapshotImpl[]) obj;

        if (snapshots.length == 2) {
            if (snapshots[0] == snapshot) {
                mSnapshots = snapshots[1];
            } else if (snapshots[1] == snapshot) {
                mSnapshots = snapshots[0];
            }
            return;
        }

        int pos;
        find: {
            for (pos = 0; pos < snapshots.length; pos++) {
                if (snapshots[pos] == snapshot) {
                    break find;
                }
            }
            return;
        }

        SnapshotImpl[] newSnapshots = new SnapshotImpl[snapshots.length - 1];
        arraycopy(snapshots, 0, newSnapshots, 0, pos);
        arraycopy(snapshots, pos + 1, newSnapshots, pos, newSnapshots.length - pos);
        mSnapshots = newSnapshots;
    }

    class SnapshotImpl implements CauseCloseable, Snapshot {
        private final PageArray mRawPageArray;

        private final TempFileManager mTempFileManager;
        private final long mSnapshotPageCount;

        private final Tree mPageCopyIndex;
        private final File mTempFile;

        private final Object mSnapshotLock;

        private final Latch mCaptureLatch;
        private final byte[] mCaptureValue;

        // The highest page written by the writeTo method.
        private volatile long mProgress;
        // Always equal to or one more than mProgress.
        private long mWriteInProgress;
        private long mCaptureInProgress;

        private volatile boolean mClosed;
        private Throwable mAbortCause;

        SnapshotImpl(TempFileManager tfm, long pageCount) throws IOException {
            // Snapshot does not decrypt pages.
            mRawPageArray = SnapshotPageArray.this.mRawSource;

            mTempFileManager = tfm;
            mSnapshotPageCount = pageCount;

            int pageSize = pageSize();

            DatabaseConfig config = new DatabaseConfig()
                .pageSize(pageSize).minCacheSize(pageSize * 100);
            mPageCopyIndex = Database.openTemp(tfm, config);
            mTempFile = config.mBaseFile;

            mSnapshotLock = new Object();
            mCaptureLatch = new Latch();
            mCaptureValue = new byte[pageSize];

            // -2: Not yet started. -1: Started, but nothing written yet.
            mProgress = -2;
            mWriteInProgress = -2;
            mCaptureInProgress = -1;
        }

        @Override
        public long length() {
            return mSnapshotPageCount * pageSize();
        }

        @Override
        public void writeTo(OutputStream out) throws IOException {
            synchronized (mSnapshotLock) {
                if (mProgress > -2) {
                    throw new IllegalStateException("Snapshot already started");
                }
                if (mClosed) {
                    throw aborted(mAbortCause);
                }
                mProgress = -1;
                mWriteInProgress = -1;
            }

            Cursor c = null;
            try {
                final byte[] buffer = new byte[pageSize()];
                final byte[] key = new byte[8];
                final long count = mSnapshotPageCount;

                {
                    // Insert a terminator for findNearby efficiency.
                    byte[] k2 = new byte[8];
                    encodeLongBE(k2, 0, ~0L);
                    mPageCopyIndex.store(Transaction.BOGUS, k2, EMPTY_BYTES);
                }

                for (long index = 0; index < count; index++, increment(key, 0, 8)) {
                    synchronized (mSnapshotLock) {
                        while (true) {
                            if (mClosed) {
                                throw aborted(mAbortCause);
                            }
                            if (index == mCaptureInProgress) {
                                try {
                                    mSnapshotLock.wait();
                                } catch (InterruptedException e) {
                                    throw new InterruptedIOException();
                                }
                            } else {
                                mWriteInProgress = index;
                                break;
                            }
                        }
                    }

                    if (c == null) {
                        c = mPageCopyIndex.newCursor(Transaction.BOGUS);
                        c.find(key);
                    } else {
                        c.findNearby(key);
                    }

                    byte[] value = c.value();

                    if (value == null) {
                        mRawPageArray.readPage(index, buffer);
                        synchronized (mSnapshotLock) {
                            mProgress = index;
                            mSnapshotLock.notifyAll();
                        }
                        out.write(buffer);
                    } else {
                        synchronized (mSnapshotLock) {
                            mProgress = index;
                            mSnapshotLock.notifyAll();
                        }
                        out.write(value);
                        c.store(null);
                    }
                }
            } finally {
                if (c != null) {
                    c.reset();
                }
                close();
            }
        }

        void capture(final long index) {
            if (index >= mSnapshotPageCount) {
                return;
            }

            Cursor c = null;
            try {
                while (true) {
                    if (index <= mProgress) {
                        return;
                    }
                    synchronized (mSnapshotLock) {
                        if (mClosed || index <= mProgress) {
                            return;
                        }
                        if (index == mWriteInProgress) {
                            mSnapshotLock.wait();
                        } else {
                            byte[] key = new byte[8];
                            encodeLongBE(key, 0, index);
                            c = mPageCopyIndex.newCursor(Transaction.BOGUS);
                            c.autoload(false);
                            c.find(key);
                            if (c.value() != null) {
                                // Already captured.
                                return;
                            }
                            // Prevent main writer from catching up while page is captured.
                            mCaptureLatch.acquireExclusive();
                            mCaptureInProgress = index;
                            break;
                        }
                    }
                }

                try {
                    mRawPageArray.readPage(index, mCaptureValue);
                    c.store(mCaptureValue);
                } finally {
                    mCaptureLatch.releaseExclusive();
                }

                synchronized (mSnapshotLock) {
                    mCaptureInProgress = -1;
                    mSnapshotLock.notifyAll();
                }
            } catch (Throwable e) {
                abort(e);
            } finally {
                if (c != null) {
                    c.reset();
                }
            }
        }

        @Override
        public void close() throws IOException {
            close(null);
        }

        @Override
        public void close(Throwable cause) throws IOException {
            if (mClosed) {
                return;
            }
            synchronized (mSnapshotLock) {
                if (mClosed) {
                    return;
                }
                mProgress = ~0L;
                mWriteInProgress = ~0L;
                mCaptureInProgress = -1;
                mAbortCause = cause;
                mClosed = true;
                mSnapshotLock.notifyAll();
            }
            unregister(this);
            closeQuietly(null, mPageCopyIndex.mDatabase);
            mTempFileManager.deleteTempFile(mTempFile);
        }

        private void abort(Throwable e) {
            try {
                close(e);
            } catch (IOException e2) {
                // Ignore.
            }
        }

        private IOException aborted(Throwable cause) {
            String message = "Snapshot closed";
            if (cause != null) {
                message += ": " + cause;
            }
            return new IOException(message);
        }
    }
}
