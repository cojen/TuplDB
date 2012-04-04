/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

import java.util.EnumSet;

/**
 * Defines a persistent, array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit index, starting at zero.
 *
 * @author Brian S O'Neill
 */
class PageArray implements Closeable {
    final int mPageSize;
    final FileIO mFio;

    volatile Object mSnapshots;

    PageArray(int pageSize, File file, EnumSet<OpenOption> options) throws IOException {
        this(pageSize, JavaFileIO.open(file, options));
    }

    PageArray(int pageSize, FileIO fio) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
        }
        mPageSize = pageSize;
        mFio = fio;
    }

    public boolean isReadOnly() {
        return mFio.isReadOnly();
    }

    /**
     * Returns the fixed size of all pages in the array, in bytes.
     */
    public int pageSize() {
        return mPageSize;
    }

    public boolean isEmpty() throws IOException {
        return mFio.length() == 0;
    }

    /**
     * Returns the total count of pages in the array.
     */
    public long getPageCount() throws IOException {
        // Always round page count down. A partial last page effectively doesn't exist.
        return mFio.length() / mPageSize;
    }

    /**
     * Set the total count of pages, truncating or growing the array as necessary.
     *
     * @throws IllegalArgumentException if count is negative
     */
    public void setPageCount(long count) throws IOException {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }
        if (isReadOnly()) {
            return;
        }
        mFio.setLength(count * mPageSize);
    }

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
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        int pageSize = mPageSize;
        mFio.read(index * pageSize, buf, offset, pageSize);
    }

    /**
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
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        int pageSize = mPageSize;
        int remaining = pageSize - start;
        length = remaining <= 0 ? 0 : Math.min(remaining, length);
        mFio.read(index * pageSize + start, buf, offset, length);
        return length;
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
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        Object obj = mSnapshots;
        if (obj != null) {
            if (obj instanceof ArraySnapshot) {
                ((ArraySnapshot) obj).capture(index);
            } else for (ArraySnapshot snapshot : (ArraySnapshot[]) obj) {
                snapshot.capture(index);
            }
        }

        int pageSize = mPageSize;
        mFio.write(index * pageSize, buf, offset, pageSize);
    }

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public void sync(boolean metadata) throws IOException {
        mFio.sync(metadata);
    }

    @Override
    public void close() throws IOException {
        mFio.close();
    }

    /**
     * Supports writing a snapshot of the array, while still permitting
     * concurrent access. Snapshot data is not a valid array file. It must be
     * processed specially by the restoreFromSnapshot method.
     *
     * @param pageCount total number of pages to include in snapshot
     * @param cluster number of pages to cluster together as snapshot pages
     * @param out snapshot destination; does not require extra buffering
     */
    Snapshot beginSnapshot(TempFileManager tfm, long pageCount, int cluster, OutputStream out)
        throws IOException
    {
        ArraySnapshot snapshot = new ArraySnapshot(tfm, pageCount, cluster, out);

        synchronized (this) {
            Object obj = mSnapshots;
            if (obj == null) {
                mSnapshots = snapshot;
            } else if (obj instanceof ArraySnapshot[]) {
                ArraySnapshot[] snapshots = (ArraySnapshot[]) obj;
                ArraySnapshot[] newSnapshots = new ArraySnapshot[snapshots.length + 1];
                System.arraycopy(snapshots, 0, newSnapshots, 0, snapshots.length);
                newSnapshots[newSnapshots.length - 1] = snapshot;
                mSnapshots = newSnapshots;
            } else {
                mSnapshots = new ArraySnapshot[] {(ArraySnapshot) obj, snapshot};
            }
        }

        return snapshot;
    }

    /**
     * @param in snapshot source; does not require extra buffering; not auto-closed
     * @return page size
     */
    static int restoreFromSnapshot(File file, EnumSet<OpenOption> options, InputStream in)
        throws IOException
    {
        FileIO fio = JavaFileIO.open(file, options);
        try {
            return restoreFromSnapshot(fio, in);
        } finally {
            try {
                fio.close();
            } catch (IOException e) {
                // Ignore.
            }
        }
    }

    /**
     * @param in snapshot source; does not require extra buffering; not auto-closed
     * @return page size
     */
    static int restoreFromSnapshot(FileIO fio, InputStream in) throws IOException {
        if (fio.isReadOnly()) {
            throw new DatabaseException("Cannot restore into a read-only file");
        }
        if (fio.length() != 0) {
            throw new DatabaseException("Cannot restore into a non-empty file");
        }

        String failMessage = null;
        int pageSize = 0;
        long snapshotPageCount = 0;
        int cluster = 0;
        readHeader: try {
            DataIn din = new DataIn(in, ArraySnapshot.HEADER_SIZE);
            if (din.readLong() != ArraySnapshot.SNAPSHOT_MAGIC_NUMBER) {
                failMessage = "Magic number mismatch";
                break readHeader;
            }
            int version = din.readInt();
            if (version != ArraySnapshot.SNAPSHOT_ENCODING_VERSION) {
                failMessage = "Unknown encoding version: " + version;
                break readHeader;
            }
            pageSize = din.readInt();
            snapshotPageCount = din.readLong();
            cluster = din.readInt();
        } catch (EOFException e) {
            failMessage = "Truncated";
        }

        if (failMessage != null) {
            throw new DatabaseException("Invalid snapshot: " + failMessage);
        }

        DataIn din = new DataIn(in, 8 + pageSize * cluster);
        long remainingPageCount = snapshotPageCount;

        while (remainingPageCount > 0) {
            long clusterIndex = din.readLong();
            long index = clusterIndex * cluster;
            int count = (int) Math.min(cluster, snapshotPageCount - index);
            if (count <= 0) {
                throw new DatabaseException("Invalid data packet index: " + index);
            }
            din.readAndWriteTo(fio, index * pageSize, count * pageSize);
            remainingPageCount -= count;
        }

        return pageSize;
    }

    synchronized void unregister(ArraySnapshot snapshot) {
        Object obj = mSnapshots;
        if (obj == snapshot) {
            mSnapshots = null;
            return;
        }
        if (!(obj instanceof ArraySnapshot[])) {
            return;
        }

        ArraySnapshot[] snapshots = (ArraySnapshot[]) obj;

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

        ArraySnapshot[] newSnapshots = new ArraySnapshot[snapshots.length - 1];
        System.arraycopy(snapshots, 0, newSnapshots, 0, pos);
        System.arraycopy(snapshots, pos + 1, newSnapshots, pos, newSnapshots.length - pos);
        mSnapshots = newSnapshots;
    }

    class ArraySnapshot implements Snapshot {
        static final long SNAPSHOT_MAGIC_NUMBER = 7280926818757785542L;
        static final int SNAPSHOT_ENCODING_VERSION = 20120216;
        static final int HEADER_SIZE = 28;

        private final TempFileManager mTempFileManager;
        private final long mSnapshotPageCount;
        private final int mCluster;
        private final OutputStream mOut;
        private final Latch mSnapshotLatch;
        private final Latch mBufferLatch;
        private final byte[] mBuffer;
        private final File mTempFile;
        private final BitMapFile mBitMap;

        // The highest cluster page written by the run method.
        private long mProgress;

        private IOException mAbortCause;

        private volatile boolean mClosed;

        ArraySnapshot(TempFileManager tfm, long pageCount, int cluster, OutputStream out)
            throws IOException
        {
            mTempFileManager = tfm;
            mSnapshotPageCount = pageCount;
            mCluster = cluster;
            mOut = out;
            mSnapshotLatch = new Latch();
            mBufferLatch = new Latch();
            // For general use, first 8 bytes of buffer encode the cluster index.
            mBuffer = new byte[8 + mPageSize * mCluster];

            try {
                mTempFile = mTempFileManager.createTempFile();
                mBitMap = new BitMapFile(mTempFile);
                mTempFileManager.register(mTempFile, mBitMap);
                // Try to pre-allocate space for the bit map.
                mBitMap.clear(((pageCount + cluster - 1) / cluster) - 1);
                        
                DataUtils.writeLong(mBuffer, 0, SNAPSHOT_MAGIC_NUMBER);
                DataUtils.writeInt(mBuffer, 8, SNAPSHOT_ENCODING_VERSION);
                DataUtils.writeInt(mBuffer, 12, mPageSize);
                DataUtils.writeLong(mBuffer, 16, mSnapshotPageCount);
                DataUtils.writeInt(mBuffer, 24, mCluster);
                mOut.write(mBuffer, 0, HEADER_SIZE);
            } catch (IOException e) {
                abort(e);
                throw e;
            }
        }

        @Override
        public void finish() throws IOException {
            try {
                final long count = mSnapshotPageCount;
                final int cluster = mCluster;
                for (long index = 0; index < count; index += cluster) {
                    mSnapshotLatch.acquireExclusive();
                    if (!mClosed) {
                        mProgress = index + cluster - 1;
                        try {
                            writeCluster(index);
                        } catch (IOException e) {
                            abort(e);
                            throw e;
                        }
                    } else {
                        IOException cause = mAbortCause;
                        mSnapshotLatch.releaseExclusive();
                        String message = "Snapshot closed";
                        if (cause != null) {
                            message += ": " + cause;
                        }
                        throw new IOException(message);
                    }
                }
            } finally {
                close();
            }
        }

        void capture(long index) {
            try {
                byte[] buffer;
                mSnapshotLatch.acquireExclusive();
                if (mClosed || index >= mSnapshotPageCount || index <= mProgress) {
                    mSnapshotLatch.releaseExclusive();
                } else {
                    writeCluster(index);
                }
            } catch (IOException e) {
                abort(e);
            }
        }

        /**
         * Caller must hold mSnapshotLatch, which is released by this method.
         */
        private void writeCluster(long index) throws IOException {
            byte[] buffer;
            int len;
            try {
                int cluster = mCluster;
                long clusterIndex = index / cluster;
                if (mBitMap.set(clusterIndex)) {
                    return;
                }
                index = clusterIndex * cluster;
                buffer = mBuffer;
                mBufferLatch.acquireExclusive();
                try {
                    DataUtils.writeLong(buffer, 0, clusterIndex);
                    int count = (int) Math.min(cluster, mSnapshotPageCount - index);
                    if (count <= 0) {
                        throw new AssertionError();
                    }
                    int pageSize = mPageSize;
                    len = pageSize * count;
                    mFio.read(index * pageSize, buffer, 8, len);
                } catch (IOException e) {
                    mBufferLatch.releaseExclusive();
                    throw e;
                }
            } finally {
                mSnapshotLatch.releaseExclusive();
            }
            try {
                mOut.write(buffer, 0, 8 + len);
            } finally {
                mBufferLatch.releaseExclusive();
            }
        }

        @Override
        public void close() throws IOException {
            close(null);
        }

        private void abort(IOException e) {
            try {
                close(e);
            } catch (IOException e2) {
                // Ignore.
            }
        }

        private void close(IOException cause) throws IOException {
            if (mClosed) {
                return;
            }
            mSnapshotLatch.acquireExclusive();
            try {
                if (mClosed) {
                    return;
                }
                mAbortCause = cause;
                mClosed = true;
            } finally {
                mSnapshotLatch.releaseExclusive();
            }
            unregister(this);
            mTempFileManager.deleteTempFile(mTempFile);
        }
    }
}
