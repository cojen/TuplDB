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

import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.security.GeneralSecurityException;

import java.util.BitSet;
import java.util.EnumSet;
import java.util.zip.CRC32;

import java.util.concurrent.locks.Lock;

import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.StripedPageArray;

import static java.lang.System.arraycopy;

import static org.cojen.tupl.Utils.*;

/**
 * Low-level support for storing fixed size pages in a single file. Page size should be a
 * multiple of the file system allocation unit, which is typically 4096 bytes. The minimum
 * allowed page size is 512 bytes, and the maximum is 2^31.
 *
 * <p>Existing pages cannot be updated, and no changes are permanently applied until
 * commit is called. This design allows full recovery following a crash, by rolling back
 * all changes to the last successful commit. All changes before the commit are still
 * stored in the file, allowing the interval between commits to be quite long.
 *
 * <p>Any exception thrown while performing an operation on the DurablePageDb
 * causes it to close. This prevents further damage if the in-memory state is
 * now inconsistent with the persistent state. The DurablePageDb must be
 * re-opened to restore to a clean state.
 *
 * @author Brian S O'Neill
 */
final class DurablePageDb extends PageDb {
    /*

    Header format for first and second pages in file, which is always 512 bytes:

    +------------------------------------------+
    | long: magic number                       |
    | int:  page size                          |
    | int:  commit number                      |
    | int:  checksum                           |
    | page manager header (140 bytes)          |
    +------------------------------------------+
    | reserved (96 bytes)                      |
    +------------------------------------------+
    | extra data (256 bytes)                   |
    +------------------------------------------+

    */

    private static final long MAGIC_NUMBER = 6529720411368701212L;

    // Indexes of entries in header node.
    private static final int I_MAGIC_NUMBER     = 0;
    private static final int I_PAGE_SIZE        = I_MAGIC_NUMBER + 8;
    private static final int I_COMMIT_NUMBER    = I_PAGE_SIZE + 4;
    private static final int I_CHECKSUM         = I_COMMIT_NUMBER + 4;
    private static final int I_MANAGER_HEADER   = I_CHECKSUM + 4;
    private static final int I_EXTRA_DATA       = 256;

    private static final int MINIMUM_PAGE_SIZE = 512;

    private final Crypto mCrypto;
    private final SnapshotPageArray mPageArray;
    private final PageManager mPageManager;

    private final Latch mHeaderLatch;
    // Commit number is the highest one which has been committed.
    private int mCommitNumber;

    /**
     * @param factory optional
     * @param cache optional
     * @param crypto optional
     */
    static DurablePageDb open(boolean explicitPageSize, int pageSize,
                              File[] files, FileFactory factory, EnumSet<OpenOption> options,
                              PageCache cache, Crypto crypto, boolean destroy)
        throws IOException
    {
        while (true) {
            try {
                return new DurablePageDb
                    (openPageArray(pageSize, files, factory, options), cache, crypto, destroy);
            } catch (WrongPageSize e) {
                if (explicitPageSize) {
                    throw e.rethrow();
                }
                pageSize = e.mActual;
                explicitPageSize = true;
            }
        }
    }

    /**
     * @param cache optional
     * @param crypto optional
     */
    static DurablePageDb open(PageArray rawArray, PageCache cache, Crypto crypto, boolean destroy)
        throws IOException
    {
        try {
            return new DurablePageDb(rawArray, cache, crypto, destroy);
        } catch (WrongPageSize e) {
            throw e.rethrow();
        }
    }

    private static PageArray openPageArray(int pageSize, File[] files, FileFactory factory,
                                           EnumSet<OpenOption> options)
        throws IOException
    {
        checkPageSize(pageSize);

        if (!options.contains(OpenOption.CREATE)) {
            for (File file : files) {
                if (!file.exists()) {
                    throw new DatabaseException("File does not exist: " + file);
                }
            }
        }

        if (files.length == 0) {
            throw new IllegalArgumentException("No files provided");
        }

        if (files.length == 1) {
            return new FilePageArray(pageSize, files[0], factory, options);
        }

        PageArray[] arrays = new PageArray[files.length];
        for (int i=0; i<files.length; i++) {
            arrays[i] = new FilePageArray(pageSize, files[i], factory, options);
        }

        return new StripedPageArray(arrays);
    }

    private static void checkPageSize(int pageSize) {
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too small: " + pageSize + " < " + MINIMUM_PAGE_SIZE);
        }
    }

    static class WrongPageSize extends Exception {
        private final int mExpected;
        private final int mActual;

        WrongPageSize(int expected, int actual) {
            mExpected = expected;
            mActual = actual;
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }

        DatabaseException rethrow() throws DatabaseException {
            throw new DatabaseException
                ("Actual page size does not match configured page size: "
                 + mActual + " != " + mExpected);
        }
    }

    private DurablePageDb(final PageArray rawArray, final PageCache cache,
                          final Crypto crypto, final boolean destroy)
        throws IOException, WrongPageSize
    {
        mCrypto = crypto;

        PageArray array = crypto == null ? rawArray : new CryptoPageArray(rawArray, crypto);

        mPageArray = new SnapshotPageArray(array, rawArray, cache);
        mHeaderLatch = new Latch();

        try {
            int pageSize = mPageArray.pageSize();
            checkPageSize(pageSize);

            if (destroy || mPageArray.isEmpty()) {
                // Newly created file.
                mPageManager = new PageManager(mPageArray);
                mCommitNumber = -1;

                // Commit twice to ensure both headers have valid data.
                commit(false, new byte[pageSize], null);
                commit(false, new byte[pageSize], null);

                mPageArray.setPageCount(2);
            } else {
                // Opened an existing file.

                // Previous header commit operation might have been interrupted before final
                // header sync completed. Pages cannot be safely recycled without this.
                mPageArray.sync(false);

                final byte[] header;
                final int commitNumber;
                findHeader: {
                    byte[] header0, header1;
                    int pageSize0;
                    int commitNumber0, commitNumber1;
                    CorruptDatabaseException ex0;

                    try {
                        header0 = readHeader(0);
                        commitNumber0 = decodeIntLE(header0, I_COMMIT_NUMBER);
                        pageSize0 = decodeIntLE(header0, I_PAGE_SIZE);
                        ex0 = null;
                    } catch (CorruptDatabaseException e) {
                        header0 = null;
                        commitNumber0 = -1;
                        pageSize0 = pageSize;
                        ex0 = e;
                    }

                    if (pageSize0 != pageSize) {
                        throw new WrongPageSize(pageSize, pageSize0);
                    }

                    try {
                        header1 = readHeader(1);
                        commitNumber1 = decodeIntLE(header1, I_COMMIT_NUMBER);
                    } catch (CorruptDatabaseException e) {
                        if (ex0 != null) {
                            // File is completely unusable.
                            throw ex0;
                        }
                        header = header0;
                        commitNumber = commitNumber0;
                        break findHeader;
                    }

                    int pageSize1 = decodeIntLE(header1, I_PAGE_SIZE);
                    if (pageSize0 != pageSize1) {
                        throw new CorruptDatabaseException
                            ("Mismatched page sizes: " + pageSize0 + " != " + pageSize1);
                    }

                    if (header0 == null) {
                        header = header1;
                        commitNumber = commitNumber1;
                    } else {
                        // Modulo comparison.
                        int diff = commitNumber1 - commitNumber0;
                        if (diff > 0) {
                            header = header1;
                            commitNumber = commitNumber1;
                        } else if (diff < 0) {
                            header = header0;
                            commitNumber = commitNumber0;
                        } else {
                            throw new CorruptDatabaseException
                                ("Both headers have same commit number: " + commitNumber0);
                        }
                    }
                }

                mHeaderLatch.acquireExclusive();
                mCommitNumber = commitNumber;
                mHeaderLatch.releaseExclusive();

                mPageManager = new PageManager(mPageArray, header, I_MANAGER_HEADER);
            }
        } catch (WrongPageSize e) {
            closeQuietly(null, this);
            throw e;
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public int allocMode() {
        return 0;
    }

    @Override
    public Node allocLatchedNode(Database db, int mode) throws IOException {
        long nodeId = allocPage();
        Node node = db.allocLatchedNode(nodeId, mode);
        node.mId = nodeId;
        return node;
    }

    @Override
    public int pageSize() {
        return mPageArray.pageSize();
    }

    @Override
    public long pageCount() throws IOException {
        return mPageArray.getPageCount();
    }

    @Override
    public Stats stats() {
        Stats stats = new Stats();
        mPageManager.addTo(stats);
        return stats;
    }

    @Override
    public BitSet tracePages() throws IOException {
        BitSet pages = new BitSet();
        mPageManager.markAllPages(pages);
        mPageManager.traceFreePages(pages);
        return pages;
    }

    @Override
    public void readPage(long id, byte[] buf) throws IOException {
        readPage(id, buf, 0);
    }

    @Override
    public void readPage(long id, byte[] buf, int offset) throws IOException {
        try {
            mPageArray.readPage(id, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void readPartial(long id, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        try {
            mPageArray.readPartial(id, start, buf, offset, length);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public long allocPage() throws IOException {
        mCommitLock.readLock().lock();
        try {
            return mPageManager.allocPage();
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    @Override
    public void writePage(long id, byte[] buf) throws IOException {
        writePage(id, buf, 0);
    }

    @Override
    public void writePage(long id, byte[] buf, int offset) throws IOException {
        checkId(id);
        try {
            mPageArray.writePage(id, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void cachePage(long id, byte[] buf) throws IOException {
        mPageArray.cachePage(id, buf);
    }

    @Override
    public void cachePage(long id, byte[] buf, int offset) throws IOException {
        mPageArray.cachePage(id, buf, offset);
    }

    @Override
    public void uncachePage(long id) throws IOException {
        mPageArray.uncachePage(id);
    }

    @Override
    public void deletePage(long id) throws IOException {
        checkId(id);
        mCommitLock.readLock().lock();
        try {
            mPageManager.deletePage(id);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
        mPageArray.uncachePage(id);
    }

    @Override
    public void recyclePage(long id) throws IOException {
        checkId(id);
        mCommitLock.readLock().lock();
        try {
            mPageManager.recyclePage(id);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    @Override
    public long allocatePages(long pageCount) throws IOException {
        if (pageCount <= 0) {
            return 0;
        }

        Stats stats = new Stats();
        mPageManager.addTo(stats);
        pageCount -= stats.freePages;

        if (pageCount <= 0) {
            return 0;
        }

        final Lock lock = mCommitLock.readLock();

        for (int i=0; i<pageCount; i++) {
            lock.lock();
            try {
                mPageManager.allocAndRecyclePage();
            } catch (Throwable e) {
                throw closeOnFailure(e);
            } finally {
                lock.unlock();
            }
        }

        return pageCount;
    }

    @Override
    public boolean compactionStart(long targetPageCount) throws IOException {
        mCommitLock.writeLock().lock();
        try {
            return mPageManager.compactionStart(targetPageCount);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.writeLock().unlock();
        }
    }

    @Override
    public boolean compactionScanFreeList() throws IOException {
        try {
            return mPageManager.compactionScanFreeList(mCommitLock);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public boolean compactionVerify() throws IOException {
        // Only performs reads and so no commit lock is required. Holding it would block
        // checkpoints during reserve list scan, which is not desirable.
        return mPageManager.compactionVerify();
    }

    @Override
    public boolean compactionEnd() throws IOException {
        try {
            return mPageManager.compactionEnd(mCommitLock);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public boolean truncatePages() throws IOException {
        return mPageManager.truncatePages();
    }

    @Override
    public int extraCommitDataOffset() {
        return I_EXTRA_DATA;
    }

    @Override
    public void commit(boolean resume, byte[] header, final CommitCallback callback)
        throws IOException
    {
        mCommitLock.writeLock().lock();
        mCommitLock.readLock().lock();

        mHeaderLatch.acquireShared();
        final int commitNumber = mCommitNumber + 1;
        mHeaderLatch.releaseShared();

        // Downgrade and keep read lock. This prevents another commit from
        // starting concurrently.
        mCommitLock.writeLock().unlock();

        try {
            try {
                if (!resume) {
                    mPageManager.commitStart(header, I_MANAGER_HEADER);
                }
                if (callback != null) {
                    // Invoke the callback to ensure all dirty pages get written.
                    callback.prepare(resume, header);
                }
            } catch (DatabaseException e) {
                if (e.isRecoverable()) {
                    throw e;
                } else {
                    throw closeOnFailure(e);
                }
            }

            try {
                commitHeader(header, commitNumber);
                mPageManager.commitEnd(header, I_MANAGER_HEADER);
            } catch (Throwable e) {
                throw closeOnFailure(e);
            }
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        try {
            mHeaderLatch.acquireShared();
            try {
                mPageArray.readPartial(mCommitNumber & 1, I_EXTRA_DATA, extra, 0, extra.length);
            } finally {
                mHeaderLatch.releaseShared();
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        if (mPageArray != null) {
            mPageArray.close(cause);
        }
    }

    /**
     * Wraps the output stream if it needs to be encrypted.
     */
    OutputStream encrypt(OutputStream out) throws IOException {
        if (mCrypto != null) {
            try {
                out = mCrypto.newEncryptingStream(0, out);
            } catch (GeneralSecurityException e) {
                throw new DatabaseException(e);
            }
        }
        return out;
    }

    /**
     * Wraps the input stream if it needs to be decrypted.
     */
    InputStream decrypt(InputStream in) throws IOException {
        if (mCrypto != null) {
            try {
                in = mCrypto.newDecryptingStream(0, in);
            } catch (GeneralSecurityException e) {
                throw new DatabaseException(e);
            }
        }
        return in;
    }

    /**
     * @see SnapshotPageArray#beginSnapshot
     */
    Snapshot beginSnapshot(TempFileManager tfm, NodeMap nodeCache) throws IOException {
        byte[] header = new byte[MINIMUM_PAGE_SIZE];
        mHeaderLatch.acquireShared();
        try {
            mPageArray.readPartial(mCommitNumber & 1, 0, header, 0, header.length);
            long pageCount = PageManager.readTotalPageCount(header, I_MANAGER_HEADER);
            long redoPos = Database.readRedoPosition(header, I_EXTRA_DATA); 
            return mPageArray.beginSnapshot(tfm, pageCount, redoPos, nodeCache);
        } finally {
            mHeaderLatch.releaseShared();
        }
    }

    /**
     * @param factory optional
     * @param cache optional
     * @param crypto optional
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static PageDb restoreFromSnapshot(int pageSize, File[] files, FileFactory factory,
                                      EnumSet<OpenOption> options,
                                      PageCache cache, Crypto crypto, InputStream in)
        throws IOException
    {
        if (options.contains(OpenOption.READ_ONLY)) {
            throw new DatabaseException("Cannot restore into a read-only file");
        }

        byte[] buffer;
        PageArray pa;
        long index = 0;

        if (crypto != null) {
            buffer = new byte[pageSize];
            pa = openPageArray(pageSize, files, factory, options);
            if (!pa.isEmpty()) {
                throw new DatabaseException("Cannot restore into a non-empty file");
            }
        } else {
            // Figure out what the actual page size is.

            buffer = new byte[MINIMUM_PAGE_SIZE];
            readFully(in, buffer, 0, buffer.length);

            long magic = decodeLongLE(buffer, I_MAGIC_NUMBER);
            if (magic != MAGIC_NUMBER) {
                throw new CorruptDatabaseException("Wrong magic number: " + magic);
            }

            pageSize = decodeIntLE(buffer, I_PAGE_SIZE);
            pa = openPageArray(pageSize, files, factory, options);

            if (!pa.isEmpty()) {
                throw new DatabaseException("Cannot restore into a non-empty file");
            }

            if (pageSize != buffer.length) {
                byte[] newBuffer = new byte[pageSize];
                arraycopy(buffer, 0, newBuffer, 0, buffer.length);
                readFully(in, newBuffer, buffer.length, pageSize - buffer.length);
                buffer = newBuffer;
            }

            pa.writePage(index, buffer);
            index++;
        }

        return restoreFromSnapshot(cache, crypto, in, buffer, pa, index);
    }

    /**
     * @param cache optional
     * @param crypto optional
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static PageDb restoreFromSnapshot(PageArray pa, PageCache cache, Crypto crypto, InputStream in)
        throws IOException
    {
        if (!pa.isEmpty()) {
            throw new DatabaseException("Cannot restore into a non-empty file");
        }

        return restoreFromSnapshot(cache, crypto, in, new byte[pa.pageSize()], pa, 0);
    }

    private static PageDb restoreFromSnapshot(PageCache cache, Crypto crypto, InputStream in,
                                              byte[] buffer, PageArray pa, long index)
        throws IOException
    {
        try {
            while (true) {
                try {
                    readFully(in, buffer, 0, buffer.length);
                } catch (EOFException e) {
                    break;
                }
                pa.writePage(index, buffer);
                index++;
            }
        } finally {
            closeQuietly(null, in);
        }

        try {
            return new DurablePageDb(pa, cache, crypto, false);
        } catch (WrongPageSize e) {
            throw e.rethrow();
        }
    }

    private IOException closeOnFailure(Throwable e) throws IOException {
        throw Utils.closeOnFailure(this, e);
    }

    private static void checkId(long id) {
        if (id <= 1) {
            throw new IllegalArgumentException("Illegal page id: " + id);
        }
    }

    /**
     * @param header array length is full page
     */
    private void commitHeader(final byte[] header, final int commitNumber)
        throws IOException
    {
        final PageArray array = mPageArray;

        encodeLongLE(header, I_MAGIC_NUMBER, MAGIC_NUMBER);
        encodeIntLE (header, I_PAGE_SIZE, array.pageSize());
        encodeIntLE (header, I_COMMIT_NUMBER, commitNumber);

        // Durably write the new page store header before returning
        // from this method, to ensure that the manager doesn't start
        // returning uncommitted pages. This would prevent rollback
        // from working because the old pages would get overwritten.
        setHeaderChecksum(header);

        // Write multiple header copies in the page, in case special recovery is required.
        int dupCount = header.length / MINIMUM_PAGE_SIZE;
        for (int i=1; i<dupCount; i++) {
            arraycopy(header, 0, header, i * MINIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE);
        }

        // Ensure all writes are flushed before flushing the header. There's
        // otherwise no ordering guarantees. Metadata should also be be flushed
        // first, because the header won't affect it.
        array.sync(true);

        mHeaderLatch.acquireExclusive();
        try {
            array.writePage(commitNumber & 1, header);
            mCommitNumber = commitNumber;
        } finally {
            mHeaderLatch.releaseExclusive();
        }

        // Final sync to ensure the header is durable.
        array.syncPage(commitNumber & 1);
    }

    private static int setHeaderChecksum(byte[] header) {
        // Clear checksum field before computing.
        encodeIntLE(header, I_CHECKSUM, 0);
        CRC32 crc = new CRC32();
        crc.update(header, 0, MINIMUM_PAGE_SIZE);
        int checksum = (int) crc.getValue();
        encodeIntLE(header, I_CHECKSUM, checksum);
        return checksum;
    }

    private byte[] readHeader(int id) throws IOException {
        byte[] header = new byte[MINIMUM_PAGE_SIZE];

        try {
            mPageArray.readPartial(id, 0, header, 0, header.length);
        } catch (EOFException e) {
            throw new CorruptDatabaseException("File is smaller than expected");
        }

        long magic = decodeLongLE(header, I_MAGIC_NUMBER);
        if (magic != MAGIC_NUMBER) {
            throw new CorruptDatabaseException("Wrong magic number: " + magic);
        }

        int checksum = decodeIntLE(header, I_CHECKSUM);

        int newChecksum = setHeaderChecksum(header);
        if (newChecksum != checksum) {
            throw new CorruptDatabaseException
                ("Header checksum mismatch: " + newChecksum + " != " + checksum);
        }

        return header;
    }
}
