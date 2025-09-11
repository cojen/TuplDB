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
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.util.concurrent.Future;

import static java.lang.System.arraycopy;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.CauseCloseable;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Wraps a {@link PageArray} to support database {@link Snapshot snapshots}.
 *
 * @author Brian S O'Neill
 */
final class SnapshotPageArray extends PageArray implements Compactable {
    final PageArray mSource;

    private volatile SnapshotImpl[] mSnapshots;

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
    public boolean compact(double target) throws IOException {
        return mSource instanceof Compactable c && c.compact(target);
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
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        mSource.readPage(index, dstAddr, offset, length);
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        preWritePage(index);
        mSource.writePage(index, srcAddr, offset);
    }

    @Override
    public long evictPage(long index, long bufAddr) throws IOException {
        preWritePage(index);
        return mSource.evictPage(index, bufAddr);
    }

    private void preWritePage(long index) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        SnapshotImpl[] snapshots = mSnapshots;
        if (snapshots != null) {
            for (var snapshot : snapshots) {
                snapshot.capture(index);
            }
        }
    }

    @Override
    public long directPageAddress(long index) throws IOException {
        return mSource.directPageAddress(index);
    }

    @Override
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
    public long copyPageFromAddress(long srcAddr, long dstIndex) throws IOException {
        preCopyPage(dstIndex);
        return mSource.copyPageFromAddress(srcAddr, dstIndex);
    }

    private void preCopyPage(long dstIndex) throws IOException {
        if (dstIndex < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(dstIndex));
        }

        SnapshotImpl[] snapshots = mSnapshots;
        if (snapshots != null) {
            for (var snapshot : snapshots) {
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

    @Override
    public boolean isClosed() {
        return mSource.isClosed();
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
            SnapshotImpl[] snapshots = mSnapshots;
            if (snapshots == null) {
                mSnapshots = new SnapshotImpl[] {snapshot};
            } else {
                var newSnapshots = new SnapshotImpl[snapshots.length + 1];
                arraycopy(snapshots, 0, newSnapshots, 0, snapshots.length);
                newSnapshots[newSnapshots.length - 1] = snapshot;
                mSnapshots = newSnapshots;
            }
        }

        return snapshot;
    }

    synchronized void unregister(SnapshotImpl snapshot) {
        SnapshotImpl[] snapshots = mSnapshots;
        if (snapshots == null) {
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

        if (snapshots.length <= 1) {
            mSnapshots = null;
        } else {
            var newSnapshots = new SnapshotImpl[snapshots.length - 1];
            arraycopy(snapshots, 0, newSnapshots, 0, pos);
            arraycopy(snapshots, pos + 1, newSnapshots, pos, newSnapshots.length - pos);
            mSnapshots = newSnapshots;
        }
    }

    class SnapshotImpl implements CauseCloseable, ReadableSnapshot {
        private final LocalDatabase mNodeCache;
        private final PageArray mRawPageArray;

        private final TempFileManager mTempFileManager;
        private final long mSnapshotPageCount;
        private final long mSnapshotRedoPosition;

        private final Copier[] mCopiers;

        private final Sequencer mSequencer;

        private final BTree mPageCopyIndex;
        private final File mTempFile;

        private OutputStream mOut;

        private volatile Object mClosed;

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

            int numCopiers = roundUpPower2(Runtime.getRuntime().availableProcessors() * 2);
            mCopiers = new Copier[numCopiers];

            mSequencer = new Sequencer(0, numCopiers);

            {
                var launcher = new Launcher();
                int pageSize = pageSize();
                launcher.pageSize(pageSize);
                launcher.minCacheSize(pageSize * Math.max(100L, numCopiers * 16L));

                mPageCopyIndex = LocalDatabase.openTemp(tfm, launcher);
                mTempFile = launcher.mBaseFile;
            }

            // Must initialize after this parent object has been initialized.
            for (int i=0; i<mCopiers.length; i++) {
                mCopiers[i] = new Copier(this, i, numCopiers);
            }
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
            // Use the sequencer latch for convenience and to ensure that mOut is visible.
            mSequencer.acquireExclusive();
            try {
                checkClosed();
                if (mOut != null) {
                    throw new IllegalStateException("Snapshot already started");
                }
                mOut = out;
            } finally {
                mSequencer.releaseExclusive();
            }

            try {
                var tasks = new Future[mCopiers.length - 1];
                for (int i=0; i<tasks.length; i++) {
                    tasks[i] = Runner.current().submit(mCopiers[i]);
                }

                mCopiers[mCopiers.length - 1].run();

                for (var task : tasks) {
                    task.get();
                }
            } catch (Exception e) {
                close(rootCause(e));
            }

            checkClosed();
            close();
        }

        void capture(long index) {
            if (index < mSnapshotPageCount) {
                mCopiers[(int) (index & (mCopiers.length - 1))].capture(index);
            }
        }

        /**
         * @return false if aborted
         */
        boolean writePage(Sequencer.Waiter waiter, long pageId, byte[] page) throws IOException {
            try {
                if (mSequencer.await(pageId, waiter)) {
                    mOut.write(page);
                    mSequencer.signal(pageId + 1);
                    return true;
                }
                return false;
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        @Override
        public void close() {
            close(null);
        }

        @Override
        public void close(Throwable cause) {
            mSequencer.acquireExclusive();
            try {
                if (mClosed == null) {
                    mClosed = cause == null ? this : cause;
                }
            } finally {
                mSequencer.releaseExclusive();
            }
            
            for (var copier : mCopiers) {
                copier.close();
            }

            // Wake up all waiting copier threads.
            mSequencer.abort();

            unregister(this);
            closeQuietly(mPageCopyIndex.mDatabase);
            mTempFileManager.deleteTempFile(mTempFile);
        }

        private void checkClosed() throws IOException {
            Object closed = mClosed;
            if (closed != null) {
                Throwable cause = null;
                if (closed instanceof Throwable t) {
                    cause = t;
                }
                if (cause instanceof InterruptedException ||
                    cause instanceof InterruptedIOException)
                {
                    throw new InterruptedIOException("Snapshot closed");
                }
                throw new IOException("Snapshot closed", cause);
            }
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
        public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
            var txn = mPageCopyIndex.mDatabase.threadLocalTransaction(DurabilityMode.NO_REDO);
            try {
                txn.lockMode(LockMode.REPEATABLE_READ);
                var key = new byte[8];
                encodeLongBE(key, 0, index);

                byte[] page = mPageCopyIndex.load(txn, key);

                if (page != null) {
                    p_copy(page, 0, dstAddr, offset, length);
                } else {
                    // TODO: Check the cache first as an optimization?
                    mRawPageArray.readPage(index, dstAddr, offset, length);
                }
            } finally {
                txn.reset();
            }
        }
    }

    static class Copier extends Latch implements Runnable {
        private static final VarHandle cProgressHandle;

        static {
            try {
                cProgressHandle =
                    MethodHandles.lookup().findVarHandle(Copier.class, "mProgress", long.class);
            } catch (Throwable e) {
                throw rethrow(e);
            }
        }

        private final SnapshotImpl mParent;
        private final long mOffset, mStride;

        private final PageArray mRawPageArray;
        private final BTree mPageCopyIndex;

        private final byte[] mCaptureBufferArray;
        private long mCaptureBufferAddr;

        // The highest page written by the writeTo method.
        private volatile long mProgress;

        /**
         * @param offset initial pageId offset
         * @param stride pageId stride
         */
        Copier(SnapshotImpl parent, long offset, long stride) {
            mParent = parent;
            mOffset = offset;
            mStride = stride;

            mRawPageArray = parent.mRawPageArray;
            mPageCopyIndex = parent.mPageCopyIndex;

            mCaptureBufferArray = new byte[mRawPageArray.pageSize()];
            // Allocates if page is not an array. The copy is not actually required.
            mCaptureBufferAddr = p_transferPage
                (mCaptureBufferArray, mRawPageArray.directPageSize());

            mProgress = Long.MIN_VALUE;
        }

        @Override
        public void run() {
            acquireExclusive();
            try {
                long progress = mProgress;
                if (progress == Long.MAX_VALUE) {
                    // Aborted.
                    return;
                }
                long init = mOffset - mStride;
                if (progress > init) {
                    throw new IllegalStateException("Snapshot already started");
                }
                mProgress = init;
            } finally {
                releaseExclusive();
            }

            final var waiter = new Sequencer.Waiter();

            final int pageSize = mParent.pageSize();
            final var pageBufferArray = new byte[pageSize];
            // Allocates if page is not an array. The copy is not actually required.
            final var pageBuffer = p_transferPage(pageBufferArray, mRawPageArray.directPageSize());

            final LocalDatabase cache = mParent.mNodeCache;
            final long count = mParent.mSnapshotPageCount;

            var txn = (LocalTransaction) mPageCopyIndex.mDatabase.newTransaction();
            try {
                // Disable writes to the undo log and fragmented value trash.
                txn.lockMode(LockMode.UNSAFE);

                Cursor c = mPageCopyIndex.newCursor(txn);
                try {
                    for (long pageId = mOffset; pageId < count; pageId += mStride) {
                        var key = new byte[8];
                        encodeLongBE(key, 0, pageId);
                        txn.doLockExclusive(mPageCopyIndex.id(), key);

                        c.findNearby(key);
                        byte[] value = c.value();

                        if (value != null) {
                            // Advance progress before releasing the lock.
                            advanceProgress(pageId - mStride, pageId);
                            c.commit(null);
                        } else {
                            read: {
                                Node node;
                                if (cache != null && (node = cache.nodeMapGet(pageId)) != null) {
                                    if (node.tryAcquireShared()) try {
                                        if (node.id() == pageId
                                            && node.mCachedState == Node.CACHED_CLEAN)
                                        {
                                            p_copy(node.mPageAddr, 0, pageBuffer, 0, pageSize);
                                            break read;
                                        }
                                    } finally {
                                        node.releaseShared();
                                    }
                                }

                                mRawPageArray.readPage(pageId, pageBuffer);
                            }

                            // Advance progress after copying the captured value and before
                            // releasing the lock.
                            advanceProgress(pageId - mStride, pageId);
                            txn.commit();

                            value = p_copyIfNotArray(pageBuffer, pageBufferArray);
                        }

                        if (!mParent.writePage(waiter, pageId, value)) {
                            break;
                        }
                    }
                } catch (Throwable e) {
                    mParent.close(e);
                } finally {
                    c.reset();
                    p_delete(pageBuffer);
                    close();
                }
            } finally {
                txn.reset();
            }
        }

        private void advanceProgress(long oldProgress, long newProgress) {
            if (!cProgressHandle.compareAndSet(this, oldProgress, newProgress)) {
                // If closed, the caller's exception handler must detect this.
                throw new IllegalStateException();
            }
        }

        void capture(final long pageId) {
            if (pageId <= mProgress) {
                return;
            }

            Cursor c = mPageCopyIndex.newCursor(Transaction.BOGUS);
            try {
                c.autoload(false);
                var key = new byte[8];
                encodeLongBE(key, 0, pageId);
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

                    if (c.value() != null || pageId <= mProgress) {
                        // Already captured or writer has advanced ahead.
                        txn.reset();
                        return;
                    }

                    acquireExclusive();
                    try {
                        var bufferAddr = mCaptureBufferAddr;
                        if (bufferAddr != p_null()) {
                            mRawPageArray.readPage(pageId, bufferAddr);
                            c.commit(p_copyIfNotArray(bufferAddr, mCaptureBufferArray));
                        }
                    } finally {
                        releaseExclusive();
                    }
                } catch (Throwable e) {
                    txn.reset();
                    throw e;
                }
            } catch (Throwable e) {
                closeQuietly(mParent, e);
            } finally {
                c.reset();
            }
        }

        void close() {
            if (mProgress != Long.MAX_VALUE) {
                mProgress = Long.MAX_VALUE;

                acquireExclusive();
                try {
                    var buffer = mCaptureBufferAddr;
                    mCaptureBufferAddr = p_null();
                    if (buffer != p_null()) {
                        p_delete(buffer);
                    }
                } finally {
                    releaseExclusive();
                }
            }
        }
    }
}
