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

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SnapshotPageArray extends PageArray {
    private final PageArray mSource;
    private final PageArray mRawSource;
    private final PageCache mCache;

    private volatile Object mSnapshots;

    /**
     * @param cache optional
     */
    SnapshotPageArray(PageArray source, PageArray rawSource, PageCache cache) {
        super(source.pageSize());
        mSource = source;
        // Snapshot does not decrypt pages.
        mRawSource = rawSource;
        mCache = cache;
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
        synchronized (this) {
            if (mSnapshots == null) {
                mSource.setPageCount(count);
                return;
            }
        }
        throw new IllegalStateException();
    }

    @Override
    public void readPage(long index, /*P*/ byte[] buf, int offset, int length) throws IOException {
        PageCache cache = mCache;
        if (cache == null || !cache.remove(index, buf, offset, length)) {
            mSource.readPage(index, buf, offset, length);
        }
    }

    @Override
    public void writePage(long index, /*P*/ byte[] buf, int offset) throws IOException {
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

        cachePage(index, buf, offset);

        mSource.writePage(index, buf, offset);
    }

    @Override
    public void cachePage(long index, /*P*/ byte[] buf, int offset) {
        PageCache cache = mCache;
        if (cache != null) {
            cache.add(index, buf, offset, p_length(buf), true);
        }
    }

    @Override
    public void uncachePage(long index) {
        PageCache cache = mCache;
        if (cache != null) {
            cache.remove(index, p_null(), 0, 0);
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        if (mCache != null) {
            mCache.close();
        }
        mSource.close(cause);
    }

    /**
     * Supports writing a snapshot of the array, while still permitting
     * concurrent access. Snapshot data is not a valid array file. It must be
     * processed specially by the restoreFromSnapshot method.
     *
     * @param pageCount total number of pages to include in snapshot
     * @param redoPos redo log position for the snapshot
     * @param nodeCache optional
     */
    Snapshot beginSnapshot(TempFileManager tfm, long pageCount, long redoPos, NodeMap nodeCache)
        throws IOException
    {
        pageCount = Math.min(pageCount, getPageCount());

        // Snapshot does not decrypt pages.
        PageArray rawSource = mRawSource;
        if (rawSource != mSource) {
            // Cache contents are not encrypted, and so it cannot be used.
            nodeCache = null;
        }

        SnapshotImpl snapshot = new SnapshotImpl(tfm, pageCount, redoPos, nodeCache, rawSource);

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
        private final NodeMap mNodeCache;
        private final PageArray mRawPageArray;

        private final TempFileManager mTempFileManager;
        private final long mSnapshotPageCount;
        private final long mSnapshotRedoPosition;

        private final Tree mPageCopyIndex;
        private final File mTempFile;

        private final Object mSnapshotLock;

        private final Latch mCaptureLatch;
        private final byte[] mCaptureBufferArray;
        private final /*P*/ byte[] mCaptureBuffer;

        // The highest page written by the writeTo method.
        private volatile long mProgress;
        // Always equal to or one more than mProgress.
        private long mWriteInProgress;
        private long mCaptureInProgress;

        private volatile boolean mClosed;
        private Throwable mAbortCause;

        /**
         * @param nodeCache optional
         */
        SnapshotImpl(TempFileManager tfm, long pageCount, long redoPos,
                     NodeMap nodeCache, PageArray rawPageArray)
            throws IOException
        {
            mNodeCache = nodeCache;
            mRawPageArray = rawPageArray;

            mTempFileManager = tfm;
            mSnapshotPageCount = pageCount;
            mSnapshotRedoPosition = redoPos;

            int pageSize = pageSize();

            DatabaseConfig config = new DatabaseConfig()
                .pageSize(pageSize).minCacheSize(pageSize * 100);
            mPageCopyIndex = Database.openTemp(tfm, config);
            mTempFile = config.mBaseFile;

            mSnapshotLock = new Object();
            mCaptureLatch = new Latch();
            mCaptureBufferArray = new byte[pageSize];
            // Allocates if page is not an array. The copy is not actually required.
            mCaptureBuffer = p_transfer(mCaptureBufferArray);

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
        public long position() {
            return mSnapshotRedoPosition;
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

            final byte[] pageBufferArray = new byte[pageSize()];
            // Allocates if page is not an array. The copy is not actually required.
            final /*P*/ byte[] pageBuffer = p_transfer(pageBufferArray);

            Cursor c = null;
            try {
                final NodeMap nodeCache = mNodeCache;
                final byte[] key = new byte[8];
                final long count = mSnapshotPageCount;

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

                    if (value != null) {
                        c.store(null);
                    } else {
                        read: {
                            Node node;
                            if (nodeCache != null && (node = nodeCache.get(index)) != null) {
                                if (node.tryAcquireShared()) try {
                                    if (node.mId == index
                                        && node.mCachedState == Node.CACHED_CLEAN)
                                    {
                                        p_copy(node.mPage, 0, pageBuffer, 0, p_length(pageBuffer));
                                        break read;
                                    }
                                } finally {
                                    node.releaseShared();
                                }
                            }

                            mRawPageArray.readPage(index, pageBuffer);
                        }

                        value = p_copyIfNotArray(pageBuffer, pageBufferArray);
                    }

                    synchronized (mSnapshotLock) {
                        mProgress = index;
                        mSnapshotLock.notifyAll();
                    }

                    out.write(value);
                }
            } finally {
                p_delete(pageBuffer);
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
                    mRawPageArray.readPage(index, mCaptureBuffer);
                    c.store(p_copyIfNotArray(mCaptureBuffer, mCaptureBufferArray));
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
                p_delete(mCaptureBuffer);
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
            return new IOException("Snapshot closed", cause);
        }
    }
}
