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

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import java.util.Arrays;
import java.util.EnumSet;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import java.util.zip.Checksum;
import java.util.zip.CRC32;

import org.cojen.tupl.ChecksumException;
import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.IncompleteRestoreException;
import org.cojen.tupl.Snapshot;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.ext.Crypto;

import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.StripedPageArray;

import org.cojen.tupl.util.Latch;

import static java.lang.System.arraycopy;

import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

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
 * <p>Any exception thrown while performing an operation on the StoredPageDb causes it to
 * close. This prevents further damage if the in-memory state is now inconsistent with the
 * persistent state. The StoredPageDb must be re-opened to restore to a clean state.
 *
 * @author Brian S O'Neill
 */
final class StoredPageDb extends PageDb implements Compactable {
    /*

    Header format for first and second pages in file, which is always 512 bytes:

    +------------------------------------------+
    | long: magic number                       |
    | int:  page size                          |
    | int:  commit number                      |
    | int:  checksum                           |
    | page manager header (140 bytes)          |
    +------------------------------------------+
    | reserved (88 bytes)                      |
    +------------------------------------------+
    | long: database id                        |
    +------------------------------------------+
    | extra data (256 bytes)                   |
    +------------------------------------------+

    */

    private static final long MAGIC_NUMBER = 6529720411368701212L;
    private static final long INCOMPLETE_RESTORE = 1295383629602457917L;

    // Indexes of entries in header node.
    private static final int I_MAGIC_NUMBER     = 0;
    private static final int I_PAGE_SIZE        = I_MAGIC_NUMBER + 8;
    private static final int I_COMMIT_NUMBER    = I_PAGE_SIZE + 4;
    private static final int I_CHECKSUM         = I_COMMIT_NUMBER + 4;
    private static final int I_MANAGER_HEADER   = I_CHECKSUM + 4;
    private static final int I_EXTRA_DATA       = 256;
    private static final int I_DATABASE_ID      = I_EXTRA_DATA - 8;

    private static final int MINIMUM_PAGE_SIZE = 512;

    private final Crypto mCrypto;
    private final SnapshotPageArray mPageArray;
    private final PageManager mPageManager;

    private final Latch mHeaderLatch;
    // Commit number is the highest one which has been committed.
    private int mCommitNumber;

    private final long mDatabaseId;

    // Is non-zero only when the header was restored from a copy.
    private int mHeaderOffset;

    /**
     * @param debugListener optional
     * @param checksumFactory optional
     * @param crypto optional
     * @param databaseId pass 0 to assign the database id automatically
     */
    static StoredPageDb open(EventListener debugListener,
                             boolean explicitPageSize, int pageSize,
                             File[] files, EnumSet<OpenOption> options,
                             Supplier<? extends Checksum> checksumFactory, Crypto crypto,
                             boolean destroy, long databaseId)
        throws IOException
    {
        while (true) {
            try {
                PageArray pa = openPageArray(pageSize, files, options);
                pa = decorate(pa, checksumFactory, crypto);
                return new StoredPageDb(debugListener, pa, crypto, destroy, databaseId);
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
     * @param debugListener optional
     * @param checksumFactory optional
     * @param crypto optional
     * @param databaseId pass 0 to assign the database id automatically
     */
    static StoredPageDb open(EventListener debugListener, PageArray rawArray,
                             Supplier<? extends Checksum> checksumFactory, Crypto crypto,
                             boolean destroy, long databaseId)
        throws IOException
    {
        try {
            PageArray pa = decorate(rawArray, checksumFactory, crypto);
            return new StoredPageDb(debugListener, pa, crypto, destroy, databaseId);
        } catch (WrongPageSize e) {
            throw e.rethrow();
        }
    }

    private static PageArray openPageArray(int pageSize, File[] files, EnumSet<OpenOption> options)
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
            return FilePageArray.factory(pageSize, files[0], options).get();
        }

        var arrays = new PageArray[files.length];

        try {
            for (int i=0; i<files.length; i++) {
                arrays[i] = FilePageArray.factory(pageSize, files[i], options).get();
            }

            return new StripedPageArray(arrays);
        } catch (Throwable e) {
            for (PageArray pa : arrays) {
                closeQuietly(pa);
            }
            throw e;
        }
    }

    private static void checkPageSize(int pageSize) {
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too small: " + pageSize + " < " + MINIMUM_PAGE_SIZE);
        }
    }

    private static PageArray decorate(PageArray pa,
                                      Supplier<? extends Checksum> checksumFactory, Crypto crypto)
    {
        if (crypto != null) {
            pa = new CryptoPageArray(pa, crypto);
        }

        if (checksumFactory != null) {
            // If encryption is also enabled, the checksum applies to the plaintext, and the
            // checksum value gets encrypted. This provides some protection against tampering
            // because the checksum value needs to change too, but it cannot be calculated
            // without knowing the plaintext first.
            pa = ChecksumPageArray.open(pa, checksumFactory);
        }

        return pa;
    }

    /**
     * @param pa must already be fully decorated
     */
    private StoredPageDb(EventListener debugListener, PageArray pa, Crypto crypto,
                         boolean destroy, long databaseId)
        throws IOException, WrongPageSize
    {
        mCrypto = crypto;
        mPageArray = new SnapshotPageArray(pa);
        mHeaderLatch = new Latch();

        try {
            int pageSize = mPageArray.pageSize();
            checkPageSize(pageSize);

            if (destroy || mPageArray.isEmpty()) {
                // Newly created file.
                mPageManager = new PageManager(mPageArray);
                mCommitNumber = -1;

                if (databaseId == 0) {
                    databaseId = generateDatabaseId(new SecureRandom());
                }

                mDatabaseId = databaseId;

                // Commit twice to ensure both headers have valid data.
                var header = p_callocPage(mPageArray.directPageSize());
                try {
                    mCommitLock.acquireExclusive();
                    try {
                        commit(false, header, null);
                        commit(false, header, null);
                    } finally {
                        mCommitLock.releaseExclusive();
                    }
                } finally {
                    p_delete(header);
                }

                mPageArray.truncatePageCount(2);
            } else {
                // Opened an existing file.

                // Previous header commit operation might have been interrupted before final
                // header sync completed. Pages cannot be safely recycled without this.
                mPageArray.sync(false);

                var header0 = p_null();
                var header1 = p_null();

                try {
                    final long headerAddr;
                    final int headerOffset;
                    final int commitNumber;
                    boolean issues = false;

                    findHeader: {
                        int headerOffset0, headerOffset1;
                        int commitNumber0, commitNumber1;
                        int pageSize0;
                        ChecksumException ex0;

                        try {
                            header0 = readHeader(debugListener, 0);
                            headerOffset0 = mHeaderOffset;
                            commitNumber0 = p_intGetLE(header0, I_COMMIT_NUMBER);
                            pageSize0 = p_intGetLE(header0, I_PAGE_SIZE);
                            ex0 = null;
                        } catch (ChecksumException e) {
                            issues = true;
                            header0 = p_null();
                            headerOffset0 = 0;
                            commitNumber0 = -1;
                            pageSize0 = pageSize;
                            ex0 = e;
                        }

                        if (pageSize0 != pageSize) {
                            throw new WrongPageSize(pageSize, pageSize0);
                        }

                        try {
                            header1 = readHeader(debugListener, 1);
                            headerOffset1 = mHeaderOffset;
                            commitNumber1 = p_intGetLE(header1, I_COMMIT_NUMBER);
                        } catch (ChecksumException e) {
                            if (ex0 != null) {
                                // File is completely unusable.
                                throw ex0;
                            }
                            issues = true;
                            headerAddr = header0;
                            headerOffset = headerOffset0;
                            commitNumber = commitNumber0;
                            break findHeader;
                        }

                        int pageSize1 = p_intGetLE(header1, I_PAGE_SIZE);
                        if (pageSize0 != pageSize1) {
                            throw new CorruptDatabaseException
                                ("Mismatched page sizes: " + pageSize0 + " != " + pageSize1);
                        }

                        if (header0 == p_null()) {
                            headerAddr = header1;
                            headerOffset = headerOffset1;
                            commitNumber = commitNumber1;
                        } else {
                            // Modulo comparison.
                            int diff = commitNumber1 - commitNumber0;
                            if (diff > 0) {
                                headerAddr = header1;
                                headerOffset = headerOffset1;
                                commitNumber = commitNumber1;
                            } else if (diff < 0) {
                                headerAddr = header0;
                                headerOffset = headerOffset0;
                                commitNumber = commitNumber0;
                            } else {
                                throw new CorruptDatabaseException
                                    ("Both headers have same commit number: " + commitNumber0);
                            }
                        }
                    }

                    mHeaderLatch.acquireExclusive();
                    mCommitNumber = commitNumber;
                    mHeaderOffset = headerOffset;
                    mHeaderLatch.releaseExclusive();

                    if (debugListener != null) {
                        debugListener.notify(EventType.DEBUG, "PAGE_SIZE: %1$d", pageSize);
                        debugListener.notify(EventType.DEBUG, "COMMIT_NUMBER: %1$d", commitNumber);
                    }

                    mPageManager = new PageManager
                        (debugListener, issues, mPageArray, headerAddr, I_MANAGER_HEADER);

                    mDatabaseId = p_longGetLE(headerAddr, I_DATABASE_ID);
                } finally {
                    p_delete(header0);
                    p_delete(header1);
                }
            }
        } catch (WrongPageSize e) {
            delete();
            closeQuietly(this);
            throw e;
        } catch (Throwable e) {
            delete();
            throw closeOnFailure(e);
        }
    }

    @Override
    long databaseId() {
        return mDatabaseId;
    }

    @Override
    void pageCache(LocalDatabase db) {
        mPageManager.pageCache(db);
    }

    @Override
    Crypto dataCrypto() {
        return mCrypto;
    }

    @Override
    Supplier<? extends Checksum> checksumFactory() {
        return TransformedPageArray.checksumFactory(mPageArray.mSource);
    }

    /**
     * Must be called when object is no longer referenced.
     */
    @Override
    void delete() {
        if (mPageManager != null) {
            mPageManager.delete();
        }
    }

    @Override
    boolean isCacheOnly() {
        if (mPageArray.mSource instanceof CompressedPageArray cpa) {
            return cpa.isCacheOnly();
        }
        return false;
    }

    @Override
    public int directPageSize() {
        return mPageArray.directPageSize();
    }

    @Override
    public int allocMode() {
        return 0;
    }

    @Override
    public Node allocLatchedNode(LocalDatabase db, int mode) throws IOException {
        long nodeId = allocPage();
        try {
            Node node = db.allocLatchedNode(mode);
            node.id(nodeId);
            return node;
        } catch (Throwable e) {
            try {
                recyclePage(nodeId);
            } catch (Throwable e2) {
                suppress(e, e2);
            }
            throw e;
        }
    }

    @Override
    public int pageSize() {
        return mPageArray.pageSize();
    }

    @Override
    public long pageCount() throws IOException {
        return mPageArray.pageCount();
    }

    @Override
    public void pageLimit(long limit) {
        mPageManager.pageLimit(limit);
    }

    @Override
    public long pageLimit() {
        return mPageManager.pageLimit();
    }

    @Override
    public void pageLimitOverride(long bytes) {
        mPageManager.pageLimitOverride(bytes);
    }

    @Override
    public Stats stats() {
        var stats = new Stats();
        mPageManager.addTo(stats);
        return stats;
    }

    @Override
    public boolean requiresCommit() {
        return mPageManager.hasDeletedOrRecycledPages();
    }

    @Override
    public void readPage(long id, long pageAddr) throws IOException {
        try {
            mPageArray.readPage(id, pageAddr, 0, pageSize());
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public long allocPage() throws IOException {
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            return mPageManager.allocPage();
        } catch (DatabaseException e) {
            if (e.isRecoverable()) {
                throw e;
            }
            throw closeOnFailure(e);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            shared.release();
        }
    }

    @Override
    public void writePage(long id, long pageAddr) throws IOException {
        checkId(id);
        mPageArray.writePage(id, pageAddr, 0);
    }

    @Override
    public long evictPage(long id, long pageAddr) throws IOException {
        checkId(id);
        return mPageArray.evictPage(id, pageAddr);
    }

    @Override
    public void deletePage(long id, boolean force) throws IOException {
        checkId(id);
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            mPageManager.deletePage(id, force);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            shared.release();
        }
    }

    @Override
    public void recyclePage(long id) throws IOException {
        checkId(id);
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            try {
                mPageManager.recyclePage(id);
            } catch (IOException e) {
                mPageManager.deletePage(id, true);
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            shared.release();
        }
    }

    @Override
    public long allocatePages(long pageCount) throws IOException {
        if (pageCount <= 0) {
            return 0;
        }

        var stats = new Stats();
        mPageManager.addTo(stats);
        pageCount -= stats.freePages;

        if (pageCount < 0) {
            return 0;
        }

        // Hint that the underlying file should preallocate as well.
        mPageArray.expandPageCount(mPageArray.pageCount() + pageCount);

        for (int i=0; i<pageCount; i++) {
            CommitLock.Shared shared = mCommitLock.acquireShared();
            try {
                mPageManager.allocAndRecyclePage();
            } catch (Throwable e) {
                throw closeOnFailure(e);
            } finally {
                shared.release();
            }
        }

        return pageCount;
    }

    @Override
    public long directPageAddress(long id) throws IOException {
        return mPageArray.directPageAddress(id);
    }

    @Override
    public long dirtyPage(long id) throws IOException {
        return mPageArray.dirtyPage(id);
    }

    @Override
    public long copyPage(long srcId, long dstId) throws IOException {
        return mPageArray.copyPage(srcId, dstId);
    }

    @Override
    public void scanFreeList(LongConsumer dst) throws IOException {
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            scanFreeList(I_MANAGER_HEADER + PageManager.I_REGULAR_QUEUE, dst);
            scanFreeList(I_MANAGER_HEADER + PageManager.I_RECYCLE_QUEUE, dst);
        } finally {
            shared.release();
        }
    }

    private void scanFreeList(int headerOffset, LongConsumer dst) throws IOException {
        PageQueueScanner.scan(mPageArray, mCommitNumber & 1, headerOffset, dst);
    }

    @Override
    public boolean compactionStart(long targetPageCount) throws IOException {
        mCommitLock.acquireExclusive();
        try {
            return mPageManager.compactionStart(targetPageCount);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.releaseExclusive();
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
    public void compactionReclaim() throws IOException {
        try {
            mPageManager.compactionReclaim();
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public boolean truncatePages() throws IOException {
        return mPageManager.truncatePages();
    }

    @Override
    public boolean compact(double target) throws IOException {
        return mPageManager.compact(target);
    }

    @Override
    public int extraCommitDataOffset() {
        return I_EXTRA_DATA;
    }

    @Override
    public void commit(boolean resume, long headerAddr, final CommitCallback callback)
        throws IOException
    {
        // Acquire a shared lock to prevent concurrent commits after callback has released
        // exclusive lock.
        CommitLock.Shared shared = mCommitLock.acquireShared();

        try {
            mHeaderLatch.acquireShared();
            final int commitNumber = mCommitNumber + 1;
            mHeaderLatch.releaseShared();

            try {
                if (!resume) {
                    mPageManager.commitStart(headerAddr, I_MANAGER_HEADER);
                }
                if (callback != null) {
                    // Invoke the callback to ensure all dirty pages get written.
                    callback.prepare(resume, headerAddr);
                }
            } catch (DatabaseException e) {
                if (e.isRecoverable()) {
                    throw e;
                } else {
                    throw closeOnFailure(e);
                }
            }

            try {
                commitHeader(headerAddr, commitNumber);
                mPageManager.commitEnd(headerAddr, I_MANAGER_HEADER);
            } catch (Throwable e) {
                throw closeOnFailure(e);
            }
        } finally {
            shared.release();
        }
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        try {
            mHeaderLatch.acquireShared();
            try {
                int offset = mHeaderOffset + I_EXTRA_DATA;
                readPartial(mCommitNumber & 1, offset, extra, 0, extra.length);
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
                out = new BufferedOutputStream(mCrypto.newEncryptingStream(out));
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
                in = mCrypto.newDecryptingStream(in);
            } catch (GeneralSecurityException e) {
                throw new DatabaseException(e);
            }
        }
        return in;
    }

    /**
     * @see SnapshotPageArray#beginSnapshot
     */
    public Snapshot beginSnapshot(LocalDatabase db) throws IOException {
        if (mPageArray.mSource instanceof CompressedPageArray cpa) {
            return cpa.beginSnapshot();
        }

        mHeaderLatch.acquireShared();
        try {
            long pageCount, redoPos;
            var header = p_allocPage(directPageSize());
            try {
                int offset = mHeaderOffset;
                mPageArray.readPage(mCommitNumber & 1, header, 0, offset + MINIMUM_PAGE_SIZE);
                pageCount = PageManager.readTotalPageCount(header, offset + I_MANAGER_HEADER);
                redoPos = LocalDatabase.readRedoPosition(header, offset + I_EXTRA_DATA); 
            } finally {
                p_delete(header);
            }
            return mPageArray.beginSnapshot(db, pageCount, redoPos);
        } finally {
            mHeaderLatch.releaseShared();
        }
    }

    // Called by CompressedPageArray.beginSnapshot.
    long snapshotRedoPos() throws IOException {
        var header = p_allocPage(directPageSize());
        try {
            mPageArray.readPage(mCommitNumber & 1, header, 0, MINIMUM_PAGE_SIZE);
            return LocalDatabase.readRedoPosition(header, I_EXTRA_DATA); 
        } finally {
            p_delete(header);
        }
    }

    /**
     * Restore into the files without checking if they're empty. Caller must delete or truncate
     * them first.
     *
     * @param checksumFactory optional
     * @param crypto optional
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static PageDb restoreFromSnapshot(int pageSize, File[] files, EnumSet<OpenOption> options,
                                      Supplier<? extends Checksum> checksumFactory, Crypto crypto,
                                      InputStream in)
        throws IOException
    {
        try (in) {
            if (options.contains(OpenOption.READ_ONLY)) {
                throw new DatabaseException("Cannot restore into a read-only file");
            }

            byte[] buffer;

            if (crypto != null) {
                buffer = new byte[pageSize];
                readFully(in, buffer, 0, buffer.length);
                if (checksumFactory != null) {
                    pageSize -= 4; // don't decrypt the checksum
                }
                decryptHeader(crypto, pageSize, buffer);
            } else {
                // Start the with minimum page size and figure out what the actual size is.
                buffer = new byte[MINIMUM_PAGE_SIZE];
                readFully(in, buffer, 0, buffer.length);
            }

            checkMagicNumber(decodeLongLE(buffer, I_MAGIC_NUMBER));

            pageSize = decodeIntLE(buffer, I_PAGE_SIZE);
            if (checksumFactory != null) {
                pageSize += 4; // need 4 bytes for the checksum
            }

            PageArray pa = openPageArray(pageSize, files, options);

            if (pageSize != buffer.length) {
                if (crypto != null) {
                    closeQuietly(pa);
                    throw new CorruptDatabaseException
                        ("Mismatched page size: " + pageSize + " != " + buffer.length);
                } else {
                    var newBuffer = new byte[pageSize];
                    arraycopy(buffer, 0, newBuffer, 0, buffer.length);
                    readFully(in, newBuffer, buffer.length, pageSize - buffer.length);
                    buffer = newBuffer;
                }
            }

            try {
                return restoreFromSnapshot(checksumFactory, crypto, in, buffer, pa);
            } catch (Throwable e) {
                closeQuietly(pa);
                throw e;
            }
        }
    }

    /**
     * Restore into the PageArray without checking if it's empty. Caller must truncate it first.
     *
     * @param checksumFactory optional
     * @param crypto optional
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static PageDb restoreFromSnapshot(PageArray pa, Supplier<? extends Checksum> checksumFactory,
                                      Crypto crypto, InputStream in)
        throws IOException
    {
        try (in) {
            var buffer = new byte[pa.pageSize()];
            readFully(in, buffer, 0, buffer.length);

            if (crypto != null) {
                int pageSize = buffer.length;
                if (checksumFactory != null) {
                    pageSize -= 4; // don't decrypt the checksum
                }
                decryptHeader(crypto, pageSize, buffer);
            }

            checkMagicNumber(decodeLongLE(buffer, I_MAGIC_NUMBER));

            int pageSize = decodeIntLE(buffer, I_PAGE_SIZE);
            if (checksumFactory != null) {
                pageSize += 4; // need 4 bytes for the checksum
            }

            if (pageSize != buffer.length) {
                closeQuietly(pa);
                throw new CorruptDatabaseException
                    ("Mismatched page size: " + pageSize + " != " + buffer.length);
            }

            return restoreFromSnapshot(checksumFactory, crypto, in, buffer, pa);
        }
    }

    /**
     * @param buffer initialized with page 0 (first header)
     */
    private static PageDb restoreFromSnapshot(Supplier<? extends Checksum> checksumFactory,
                                              Crypto crypto,
                                              InputStream in, byte[] buffer, PageArray rawArray)
        throws IOException
    {
        PageArray logicalArray = decorate(rawArray, checksumFactory, crypto);

        // Indicate that a restore is in progress. Replace with the correct magic number when
        // complete.
        encodeLongLE(buffer, I_MAGIC_NUMBER, INCOMPLETE_RESTORE);

        int commitNumber = decodeIntLE(buffer, I_COMMIT_NUMBER);
        long pageCount = decodeLongLE(buffer, I_MANAGER_HEADER + PageManager.I_TOTAL_PAGE_COUNT);

        var bufferPageAddr = p_transferPage(buffer, rawArray.directPageSize());

        try {
            // Write header and ensure that the incomplete restore state is persisted.
            writeLogicalHeader(logicalArray, buffer, bufferPageAddr);
            logicalArray.sync(false);

            // Read header 1 to determine the correct page count, and then preallocate.
            {
                readFully(in, buffer, 0, buffer.length);
                rawArray.writePage(1, p_transferArrayToPage(buffer, bufferPageAddr));

                if (crypto != null) {
                    decryptHeader(crypto, logicalArray.pageSize(), buffer);
                }

                if (decodeIntLE(buffer, I_COMMIT_NUMBER) > commitNumber) {
                    // Header 1 is newer, so it has the correct page count.
                    pageCount = decodeLongLE
                        (buffer, I_MANAGER_HEADER + PageManager.I_TOTAL_PAGE_COUNT);
                }

                rawArray.expandPageCount(pageCount);
            }

            long index = 2;
            while (true) {
                int amt = in.read(buffer);
                if (amt < 0) {
                    break;
                }
                readFully(in, buffer, amt, buffer.length - amt);
                rawArray.writePage(index, p_transferArrayToPage(buffer, bufferPageAddr));
                index++;
            }

            // Store proper magic number, indicating that the restore is complete. All data
            // pages must be durable before doing this.

            rawArray.sync(false);
            rawArray.readPage(0, bufferPageAddr);
            p_transferPageToArray(bufferPageAddr, buffer);

            if (crypto != null) {
                decryptHeader(crypto, logicalArray.pageSize(), buffer);
            }

            encodeLongLE(buffer, I_MAGIC_NUMBER, MAGIC_NUMBER);
            writeLogicalHeader(logicalArray, buffer, bufferPageAddr);
        } finally {
            p_delete(bufferPageAddr);
        }

        // Ensure newly restored snapshot is durable and also ensure that PageArray (if a
        // MappedPageArray) no longer considers itself to be empty.
        logicalArray.sync(true);

        try {
            return new StoredPageDb(null, logicalArray, crypto, false, 0);
        } catch (WrongPageSize e) {
            throw e.rethrow();
        }
    }

    private static void decryptHeader(Crypto crypto, int pageSize, byte[] buffer)
        throws DatabaseException
    {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ms = a.allocate(pageSize);
            MemorySegment.copy(buffer, 0, ms, ValueLayout.JAVA_BYTE, 0, pageSize);
            crypto.decryptPage(0, pageSize, ms.address(), 0);
            MemorySegment.copy(ms, ValueLayout.JAVA_BYTE, 0, buffer, 0, pageSize);
        } catch (GeneralSecurityException e) {
            throw new DatabaseException(e);
        }
    }

    private static void writeLogicalHeader(PageArray pa, byte[] buffer, long bufferPageAddr)
        throws IOException
    {
        if (buffer.length != pa.pageSize()) {
            // Assume that the logical page size is smaller.
            buffer = Arrays.copyOfRange(buffer, 0, pa.pageSize());
        }
        pa.writePage(0, p_transferArrayToPage(buffer, bufferPageAddr));
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
     * @param headerAddr length is full page
     */
    private void commitHeader(final long headerAddr, final int commitNumber)
        throws IOException
    {
        final PageArray array = mPageArray;

        p_longPutLE(headerAddr, I_MAGIC_NUMBER, MAGIC_NUMBER);
        p_intPutLE (headerAddr, I_PAGE_SIZE, array.pageSize());
        p_intPutLE (headerAddr, I_COMMIT_NUMBER, commitNumber);
        p_longPutLE(headerAddr, I_DATABASE_ID, mDatabaseId);

        // Durably write the new page store header before returning
        // from this method, to ensure that the manager doesn't start
        // returning uncommitted pages. This would prevent rollback
        // from working because the old pages would get overwritten.
        setHeaderChecksum(headerAddr);

        // Write multiple header copies in the page, in case special recovery is required.
        int dupCount = pageSize() / MINIMUM_PAGE_SIZE;
        for (int i=1; i<dupCount; i++) {
            p_copy(headerAddr, 0, headerAddr, i * MINIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE);
        }

        // Ensure all writes are flushed before flushing the header. There's
        // otherwise no ordering guarantees. Metadata should also be flushed
        // first, because the header won't affect it.
        array.sync(true);

        mHeaderLatch.acquireExclusive();
        try {
            array.writePage(commitNumber & 1, headerAddr);
            mCommitNumber = commitNumber;
            mHeaderOffset = 0;
        } finally {
            mHeaderLatch.releaseExclusive();
        }

        // Final sync to ensure the header is durable.
        array.syncPage(commitNumber & 1);
    }

    private static int setHeaderChecksum(long headerAddr) {
        // Clear the checksum field before computing a new one.
        p_intPutLE(headerAddr, I_CHECKSUM, 0);
        var crc = new CRC32();
        crc.update(MemorySegment.ofAddress(headerAddr)
                   .reinterpret(MINIMUM_PAGE_SIZE).asByteBuffer());
        int checksum = (int) crc.getValue();
        p_intPutLE(headerAddr, I_CHECKSUM, checksum);
        return checksum;
    }

    private long readHeader(EventListener debugListener, int id) throws IOException {
        mHeaderOffset = 0;
        var headerAddr = p_allocPage(directPageSize());

        ChecksumException ex;

        try {
            doReadHeader(headerAddr, 0, id);
            return headerAddr;
        } catch (ChecksumException e) {
            if (debugListener != null) {
                debugListener.notify(EventType.DEBUG, e.toString());
            }
            ex = e;
        } catch (Throwable e) {
            p_delete(headerAddr);
            if (e instanceof EOFException) {
                throw new CorruptDatabaseException("File is smaller than expected");
            }
            throw e;
        }

        // Try to restore the header from one of the copies.

        int offset = 0;
        while (true) {
            offset += MINIMUM_PAGE_SIZE;
            if (offset + MINIMUM_PAGE_SIZE > pageSize()) {
                break;
            }
            try {
                doReadHeader(headerAddr, offset, id);
                mHeaderOffset = offset;
                return headerAddr;
            } catch (ChecksumException e) {
                if (debugListener != null) {
                    debugListener.notify(EventType.DEBUG, e.toString());
                }
                // Try another copy.
            } catch (Throwable e) {
                break;
            }
        }

        // Unable to restore the header.
        p_delete(headerAddr);
        throw ex;
    }

    private void doReadHeader(long headerAddr, int offset, int id) throws IOException {
        mPageArray.readPage(id, headerAddr, 0, offset + MINIMUM_PAGE_SIZE);

        if (offset != 0) {
            // A copy of the header was read; move it into position.
            p_copy(headerAddr, offset, headerAddr, 0, MINIMUM_PAGE_SIZE);
        }

        checkMagicNumber(p_longGetLE(headerAddr, I_MAGIC_NUMBER));

        int storedChecksum = p_intGetLE(headerAddr, I_CHECKSUM);

        int computedChecksum = setHeaderChecksum(headerAddr);

        if (storedChecksum != computedChecksum) {
            throw new ChecksumException(id, storedChecksum, computedChecksum);
        }
    }

    private void readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        long pageAddr;
        int readLen;

        int directPageSize = directPageSize();
        if (directPageSize < 0) { // direct I/O
            readLen = pageSize();
            pageAddr = p_allocPage(directPageSize);
        } else {
            readLen = start + length;
            pageAddr = p_alloc(readLen);
        }

        try {
            mPageArray.readPage(index, pageAddr, 0, readLen);
            p_copy(pageAddr, start, buf, offset, length);
        } finally {
            p_delete(pageAddr);
        }
    }

    private static void checkMagicNumber(long magic) throws CorruptDatabaseException {
        if (magic != MAGIC_NUMBER) {
            if (magic == INCOMPLETE_RESTORE) {
                throw new IncompleteRestoreException();
            }
            throw new CorruptDatabaseException("Wrong magic number: " + magic);
        }
    }
}
