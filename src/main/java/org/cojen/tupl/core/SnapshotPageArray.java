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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.ByteBuffer;

import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.arraycopy;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.CauseCloseable;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Wraps a {@link PageArray} to support database {@link Snapshot snapshots}.
 *
 * @author Brian S O'Neill
 */
final class SnapshotPageArray extends PageArray {
    final PageArray mSource;

    private volatile Object mSnapshots;

    SnapshotPageArray(PageArray source) {
        super(source.pageSize());
        mSource = source;
    }

    @Override
    public int directPageSize() {
        return mSource.directPageSize();
    }

    @Override
    public boolean isReadOnly() {
        return mSource.isReadOnly();
    }

    @Override
    public boolean isFullyMapped() {
        return mSource.isFullyMapped();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public long pageCount() throws IOException {
        return mSource.pageCount();
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        synchronized (this) {
            if (mSnapshots == null) {
                mSource.truncatePageCount(count);
                return;
            }
        }
        throw new IllegalStateException();
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        mSource.expandPageCount(count);
    }

    @Override
    public long pageCountLimit() throws IOException {
        return mSource.pageCountLimit();
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        mSource.readPage(index, dst, offset, length);
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
        mSource.readPage(index, dstPtr, offset, length);
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        preWritePage(index);
        mSource.writePage(index, src, offset);
    }

    @Override
    public void writePage(long index, byte[] src, int offset, ByteBuffer tail) throws IOException {
        // Only required by lower layers, and used by CheckedPageArray.
        throw new UnsupportedOperationException();
    }

    @Override
    public void writePage(long index, long srcPtr, int offset) throws IOException {
        preWritePage(index);
        mSource.writePage(index, srcPtr, offset);
    }

    @Override
    public byte[] evictPage(long index, byte[] buf) throws IOException {
        preWritePage(index);
        return mSource.evictPage(index, buf);
    }

    @Override
    public long evictPage(long index, long bufPtr) throws IOException {
        preWritePage(index);
        return mSource.evictPage(index, bufPtr);
    }

    private void preWritePage(long index) throws IOException {
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
    public long directPagePointer(long index) throws IOException {
        return mSource.directPagePointer(index);
    }

    public long dirtyPage(long index) throws IOException {
        preCopyPage(index);
        return mSource.dirtyPage(index);
    }

    @Override
    public long copyPage(long srcIndex, long dstIndex) throws IOException {
        preCopyPage(dstIndex);
        return mSource.copyPage(srcIndex, dstIndex);
    }

    @Override
    public long copyPageFromPointer(long srcPointer, long dstIndex) throws IOException {
        preCopyPage(dstIndex);
        return mSource.copyPageFromPointer(srcPointer, dstIndex);
    }

    private void preCopyPage(long dstIndex) throws IOException {
        if (dstIndex < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(dstIndex));
        }

        Object obj = mSnapshots;
        if (obj != null) {
            if (obj instanceof SnapshotImpl) {
                ((SnapshotImpl) obj).capture(dstIndex);
            } else for (SnapshotImpl snapshot : (SnapshotImpl[]) obj) {
                snapshot.capture(dstIndex);
            }
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        mSource.syncPage(index);
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
     * @param redoPos redo log position for the snapshot
     */
    Snapshot beginSnapshot(LocalDatabase db, long pageCount, long redoPos) throws IOException {
        pageCount = Math.min(pageCount, pageCount());

        LocalDatabase nodeCache = db;

        // Snapshot does not decrypt pages.
        PageArray rawSource = TransformedPageArray.rawSource(mSource);
        if (rawSource != mSource) {
            // Cache contents are not encrypted, and so it cannot be used.
            nodeCache = null;
        }

        TempFileManager tfm = db.mTempFileManager;

        var snapshot = new SnapshotImpl(tfm, pageCount, redoPos, nodeCache, rawSource);

        synchronized (this) {
            Object obj = mSnapshots;
            if (obj == null) {
                mSnapshots = snapshot;
            } else if (obj instanceof SnapshotImpl[]) {
                var snapshots = (SnapshotImpl[]) obj;
                var newSnapshots = new SnapshotImpl[snapshots.length + 1];
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

        var snapshots = (SnapshotImpl[]) obj;

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

        var newSnapshots = new SnapshotImpl[snapshots.length - 1];
        arraycopy(snapshots, 0, newSnapshots, 0, pos);
        arraycopy(snapshots, pos + 1, newSnapshots, pos, newSnapshots.length - pos);
        mSnapshots = newSnapshots;
    }

    // This should be declared in the SnapshotImpl class, but the Java compiler prohibits this
    // for no good reason. This also requires that the field be declared as package-private.
    static final VarHandle cProgressHandle;

    static {
        try {
            cProgressHandle =
                MethodHandles.lookup().findVarHandle
                (SnapshotImpl.class, "mProgress", long.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    class SnapshotImpl implements CauseCloseable, ReadableSnapshot {
        private final LocalDatabase mNodeCache;
        private final PageArray mRawPageArray;

        private final TempFileManager mTempFileManager;
        private final long mSnapshotPageCount;
        private final long mSnapshotRedoPosition;

        private final BTree mPageCopyIndex;
        private final File mTempFile;

        private final Latch mSnapshotLatch;

        private final Latch[] mCaptureLatches;
        private final byte[][] mCaptureBufferArrays;
        private final /*P*/ byte[][] mCaptureBuffers;

        // The highest page written by the writeTo method.
        volatile long mProgress;

        private volatile Throwable mAbortCause;

        /**
         * @param nodeCache optional
         */
        SnapshotImpl(TempFileManager tfm, long pageCount, long redoPos,
                     LocalDatabase nodeCache, PageArray rawPageArray)
            throws IOException
        {
            mNodeCache = nodeCache;
            mRawPageArray = rawPageArray;

            mTempFileManager = tfm;
            mSnapshotPageCount = pageCount;
            mSnapshotRedoPosition = redoPos;

            final int pageSize = pageSize();

            mSnapshotLatch = new Latch();

            final int slots = Runtime.getRuntime().availableProcessors() * 4;
            mCaptureLatches = new Latch[slots];
            mCaptureBufferArrays = new byte[slots][];
            /*P*/ // [
            mCaptureBuffers = new byte[slots][];
            /*P*/ // |
            /*P*/ // mCaptureBuffers = new long[slots];
            /*P*/ // ]

            for (int i=0; i<slots; i++) {
                mCaptureLatches[i] = new Latch();
                mCaptureBufferArrays[i] = new byte[pageSize];
                // Allocates if page is not an array. The copy is not actually required.
                mCaptureBuffers[i] = p_transferPage
                    (mCaptureBufferArrays[i], rawPageArray.directPageSize());
            }

            var launcher = new Launcher();
            launcher.pageSize(pageSize);
            launcher.minCacheSize(pageSize * Math.max(100, slots * 16));
            if (nodeCache != null) {
                launcher.directPageAccess(nodeCache.isDirectPageAccess());
            }
            mPageCopyIndex = LocalDatabase.openTemp(tfm, launcher);
            mTempFile = launcher.mBaseFile;

            // -2: Not yet started. -1: Started, but nothing written yet.
            mProgress = -2;
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
        public boolean isCompressible() {
            return mNodeCache != null;
        }

        @Override
        public void writeTo(OutputStream out) throws IOException {
            mSnapshotLatch.acquireExclusive();
            try {
                long progress = mProgress;
                if (progress == Long.MAX_VALUE) {
                    throw aborted(mAbortCause);
                }
                if (progress > -2) {
                    throw new IllegalStateException("Snapshot already started");
                }
                mProgress = -1;
            } finally {
                mSnapshotLatch.releaseExclusive();
            }

            final var pageBufferArray = new byte[pageSize()];
            // Allocates if page is not an array. The copy is not actually required.
            final var pageBuffer = p_transferPage(pageBufferArray, mRawPageArray.directPageSize());

            final LocalDatabase cache = mNodeCache;
            final long count = mSnapshotPageCount;

            var txn = (LocalTransaction) mPageCopyIndex.mDatabase.newTransaction();
            try {
                // Disable writes to the undo log and fragmented value trash.
                txn.lockMode(LockMode.UNSAFE);

                Cursor c = mPageCopyIndex.newCursor(txn);
                try {
                    for (long index = 0; index < count; index++) {
                        var key = new byte[8];
                        encodeLongBE(key, 0, index);
                        txn.doLockExclusive(mPageCopyIndex.id(), key);

                        c.findNearby(key);
                        byte[] value = c.value();

                        if (value != null) {
                            // Advance progress before releasing the lock.
                            advanceProgress(index);
                            c.commit(null);
                        } else {
                            read: {
                                Node node;
                                if (cache != null && (node = cache.nodeMapGet(index)) != null) {
                                    if (node.tryAcquireShared()) try {
                                        if (node.id() == index
                                            && node.mCachedState == Node.CACHED_CLEAN)
                                        {
                                            p_copy(node.mPage, 0, pageBuffer, 0, pageSize());
                                            break read;
                                        }
                                    } finally {
                                        node.releaseShared();
                                    }
                                }

                                mRawPageArray.readPage(index, pageBuffer);
                            }

                            // Advance progress after copying the captured value and before
                            // releasing the lock.
                            advanceProgress(index);
                            txn.commit();

                            value = p_copyIfNotArray(pageBuffer, pageBufferArray);
                        }

                        out.write(value);
                    }
                } catch (Throwable e) {
                    if (mProgress == Long.MAX_VALUE) {
                        throw aborted(mAbortCause);
                    }
                    throw e;
                } finally {
                    c.reset();
                    p_delete(pageBuffer);
                    close();
                }
            } finally {
                txn.reset();
            }
        }

        private void advanceProgress(long index) {
            if (!cProgressHandle.compareAndSet(this, index - 1, index)) {
                // If closed, the caller's exception handler must detect this.
                throw new IllegalStateException();
            }
        }

        void capture(final long index) {
            if (index >= mSnapshotPageCount || index <= mProgress) {
                return;
            }

            Cursor c = mPageCopyIndex.newCursor(Transaction.BOGUS);
            try {
                c.autoload(false);
                var key = new byte[8];
                encodeLongBE(key, 0, index);
                c.find(key);

                if (c.value() != null) {
                    // Already captured.
                    return;
                }

                // Lock and check again.

                Transaction txn = mPageCopyIndex.mDatabase.newTransaction();
                try {
                    c.link(txn);
                    c.load();

                    if (c.value() != null || index <= mProgress) {
                        // Already captured or writer has advanced ahead.
                        txn.reset();
                        return;
                    }

                    int slot = ThreadLocalRandom.current().nextInt(mCaptureLatches.length);

                    Latch latch = mCaptureLatches[slot];
                    latch.acquireExclusive();
                    try {
                        byte[] bufferArray = mCaptureBufferArrays[slot];
                        if (bufferArray != null) {
                            var buffer = mCaptureBuffers[slot];
                            mRawPageArray.readPage(index, buffer);
                            c.commit(p_copyIfNotArray(buffer, bufferArray));
                        }
                    } finally {
                        latch.releaseExclusive();
                    }
                } catch (Throwable e) {
                    txn.reset();
                    throw e;
                }
            } catch (Throwable e) {
                closeQuietly(this, e);
            } finally {
                c.reset();
            }
        }

        @Override
        public void close() throws IOException {
            close(null);
        }

        @Override
        public void close(Throwable cause) throws IOException {
            if (mProgress == Long.MAX_VALUE) {
                return;
            }

            mSnapshotLatch.acquireExclusive();
            try {
                if (mProgress == Long.MAX_VALUE) {
                    return;
                }

                mProgress = Long.MAX_VALUE;
                mAbortCause = cause;

                for (int i=0; i<mCaptureLatches.length; i++) {
                    Latch latch = mCaptureLatches[i];
                    latch.acquireExclusive();
                    try {
                        mCaptureBufferArrays[i] = null;
                        p_delete(mCaptureBuffers[i]);
                    } finally {
                        latch.releaseExclusive();
                    }
                }
            } finally {
                mSnapshotLatch.releaseExclusive();
            }

            unregister(this);
            closeQuietly(mPageCopyIndex.mDatabase);
            mTempFileManager.deleteTempFile(mTempFile);
        }

        private IOException aborted(Throwable cause) {
            return new IOException("Snapshot closed", cause);
        }

        // Defined by ReadableSnapshot.
        @Override
        public int pageSize() {
            return mRawPageArray.pageSize();
        }

        // Defined by ReadableSnapshot.
        @Override
        public long pageCount() {
            return mSnapshotPageCount;
        }

        // Defined by ReadableSnapshot.
        @Override
        public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
            var txn = mPageCopyIndex.mDatabase.threadLocalTransaction(DurabilityMode.NO_REDO);
            try {
                txn.lockMode(LockMode.REPEATABLE_READ);
                var key = new byte[8];
                encodeLongBE(key, 0, index);

                byte[] page = mPageCopyIndex.load(txn, key);

                if (page != null) {
                    arraycopy(page, 0, dst, offset, length);
                } else {
                    // FIXME: Check the cache first as an optimization?
                    mRawPageArray.readPage(index, dst, offset, length);
                }
            } finally {
                txn.reset();
            }
        }

        // Defined by ReadableSnapshot.
        @Override
        public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
            var txn = mPageCopyIndex.mDatabase.threadLocalTransaction(DurabilityMode.NO_REDO);
            try {
                txn.lockMode(LockMode.REPEATABLE_READ);
                var key = new byte[8];
                encodeLongBE(key, 0, index);

                byte[] page = mPageCopyIndex.load(txn, key);

                if (page != null) {
                    DirectPageOps.p_copyFromArray(page, 0, dstPtr, offset, length);
                } else {
                    // FIXME: Check the cache first as an optimization?
                    mRawPageArray.readPage(index, dstPtr, offset, length);
                }
            } finally {
                txn.reset();
            }
        }
    }
}
