/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import static org.cojen.tupl.Utils.*;

/**
 * Defines a persistent, array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit index, starting at zero.
 *
 * @author Brian S O'Neill
 */
abstract class PageArray extends CauseCloseable {
    final int mPageSize;

    volatile Object mSnapshots;

    PageArray(int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
        }
        mPageSize = pageSize;
    }

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public final int pageSize() {
        return mPageSize;
    }

    public PageArray rawPageArray() {
        return this;
    }

    public abstract boolean isReadOnly();

    public abstract boolean isEmpty() throws IOException;

    /**
     * Returns the total count of pages in the array.
     */
    public abstract long getPageCount() throws IOException;

    /**
     * Set the total count of pages, truncating or growing the array as necessary.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public abstract void setPageCount(long count) throws IOException;

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public void readPage(long index, byte[] buf) throws IOException {
        readPage(index, buf, 0);
    }

    /**
     * @param index zero-based page index to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public abstract void readPage(long index, byte[] buf, int offset) throws IOException;

    /**
     * Subclass should override to improve performance.
     *
     * @param index zero-based page index to read
     * @param start start of page to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length length to read
     * @return actual length read
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        int pageSize = mPageSize;
        if (start == 0 && length == pageSize) {
            readPage(index, buf, offset);
        } else {
            byte[] page = new byte[pageSize];
            readPage(index, page, 0);
            arraycopy(page, start, buf, offset, length);
        }
        return length;
    }

    /**
     * Subclass should override to improve performance.
     *
     * @param index zero-based page index to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param count number of pages to read
     * @return length read (always page size times count)
     * @throws IndexOutOfBoundsException if index is negative
     * @throws IOException if index is greater than or equal to page count
     */
    public int readCluster(long index, byte[] buf, int offset, int count) throws IOException {
        int pageSize = mPageSize;
        if (count > 0) while (true) {
            readPage(index, buf, offset);
            if (--count <= 0) {
                break;
            }
            index++;
            offset += pageSize;
        }
        return pageSize * count;
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePage(long index, byte[] buf) throws IOException {
        writePage(index, buf, 0);
    }

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public final void writePage(long index, byte[] buf, int offset) throws IOException {
        prepareToWrite(index);
        doWritePage(index, buf, offset);
    }

    /**
     * Writes a page, which is immediately flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @throws IndexOutOfBoundsException if index is negative
     */
    public void writePageDurably(long index, byte[] buf) throws IOException {
        writePageDurably(index, buf, 0);
    }

    /**
     * Writes a page, which is immediately flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IndexOutOfBoundsException if index is negative
     */
    public final void writePageDurably(long index, byte[] buf, int offset) throws IOException {
        prepareToWrite(index);
        doWritePageDurably(index, buf, offset);
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

    /**
     * Writes a page, which is lazily flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write (never negative)
     * @param buf data to write
     * @param offset offset into data buffer
     */
    abstract void doWritePage(long index, byte[] buf, int offset) throws IOException;

    /**
     * Writes a page, which is immediately flushed. The array grows automatically if
     * the index is greater than or equal to the current page count.
     *
     * @param index zero-based page index to write (never negative)
     * @param buf data to write
     * @param offset offset into data buffer
     */
    abstract void doWritePageDurably(long index, byte[] buf, int offset) throws IOException;

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public abstract void sync(boolean metadata) throws IOException;

    @Override
    public void close() throws IOException {
        close(null);
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

    class SnapshotImpl extends CauseCloseable implements Snapshot {
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
            // Snapshot does not decrypt the pages.
            mRawPageArray = rawPageArray();

            mTempFileManager = tfm;
            mSnapshotPageCount = pageCount;

            int pageSize = mPageSize;

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
            return mSnapshotPageCount * mPageSize;
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
                final byte[] buffer = new byte[mPageSize];
                final byte[] key = new byte[8];
                final long count = mSnapshotPageCount;

                {
                    // Insert a terminator for findNearby efficiency.
                    byte[] k2 = new byte[8];
                    writeLongBE(k2, 0, ~0L);
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
                            writeLongBE(key, 0, index);
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
