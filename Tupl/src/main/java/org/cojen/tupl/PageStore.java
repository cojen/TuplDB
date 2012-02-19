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
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import java.util.BitSet;
import java.util.EnumSet;
import java.util.UUID;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.zip.CRC32;

import static org.cojen.tupl.DataUtils.*;

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
 * <p>Any exception thrown while performing an operation on the PageStore causes it to
 * close. This prevents further damage if the in-memory state is now inconsistent with the
 * persistent state. The PageStore must be re-opened to restore to a clean state.
 *
 * @author Brian S O'Neill
 */
class PageStore implements Closeable {
    /*

    Header format for first and second pages in file, which is always 512 bytes:

    +------------------------------------------+
    | long: magic number                       |
    | int:  page size                          |
    | int:  commit number                      |
    | int:  checksum                           |
    | page manager header (96 bytes)           |
    +------------------------------------------+
    | reserved (140 bytes)                     |
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

    private final PageArray mPageArray;
    private final PageManager mPageManager;

    private final ReadWriteLock mCommitLock;
    // Commit number is the highest one which has been committed.
    private volatile int mCommitNumber;

    PageStore(File file, EnumSet<OpenOption> options, int pageSize)
        throws IOException
    {
        this(file, options, pageSize, 32);
    }

    PageStore(File file, EnumSet<OpenOption> options, int pageSize, int openFileCount)
        throws IOException
    {
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size must be at least " + MINIMUM_PAGE_SIZE + ": " + pageSize);
        }

        if (!file.exists() &&
            !options.contains(OpenOption.CREATE) &&
            !options.contains(OpenOption.FORCE_CREATE))
        {
            throw new DatabaseException("File does not exist: " + file);
        }

        mCommitLock = new ReentrantReadWriteLock(true);

        try {
            mPageArray = new PageArray(pageSize, file, options);

            open: {
                if (mPageArray.isEmpty()) {
                    // Newly created file.
                    mPageManager = new PageManager(mPageArray);
                    mCommitNumber = -1;
                    commit(null);
                    break open;
                }

                // Opened an existing file.

                byte[] header;
                findHeader: {
                    byte[] header0, header1;
                    int pageSize0;
                    int commitNumber0, commitNumber1;
                    CorruptPageStoreException ex0;

                    try {
                        header0 = readHeader(0);
                        commitNumber0 = readInt(header0, I_COMMIT_NUMBER);
                        pageSize0 = readInt(header0, I_PAGE_SIZE);
                        ex0 = null;
                    } catch (CorruptPageStoreException e) {
                        header0 = null;
                        commitNumber0 = -1;
                        pageSize0 = pageSize;
                        ex0 = e;
                    }

                    if (pageSize0 != pageSize) {
                        throw new DatabaseException
                            ("Actual page size does not match configured page size: "
                             + pageSize0 + " != " + pageSize);
                    }

                    try {
                        header1 = readHeader(1);
                        commitNumber1 = readInt(header1, I_COMMIT_NUMBER);
                    } catch (CorruptPageStoreException e) {
                        if (ex0 != null) {
                            // File is completely unusable.
                            if (options.contains(OpenOption.FORCE_CREATE)) {
                                // Re-create it.
                                mPageManager = new PageManager(mPageArray);
                                mCommitNumber = -1;
                                commit(null);
                                break open;
                            }
                            throw ex0;
                        }
                        header = header0;
                        mCommitNumber = commitNumber0;
                        break findHeader;
                    }

                    int pageSize1 = readInt(header1, I_PAGE_SIZE);
                    if (pageSize0 != pageSize1) {
                        throw new CorruptPageStoreException
                            ("Mismatched page sizes: " + pageSize0 + " != " + pageSize1);
                    }

                    if (header0 == null) {
                        header = header1;
                        mCommitNumber = commitNumber1;
                    } else {
                        // Modulo comparison.
                        int diff = commitNumber1 - commitNumber0;
                        if (diff > 0) {
                            header = header1;
                            mCommitNumber = commitNumber1;
                        } else if (diff < 0) {
                            header = header0;
                            mCommitNumber = commitNumber0;
                        } else {
                            throw new CorruptPageStoreException
                                ("Both headers have same commit number: " + commitNumber0);
                        }
                    }
                }

                mPageManager = new PageManager(mPageArray, header, I_MANAGER_HEADER);
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    public int pageSize() {
        return mPageArray.pageSize();
    }

    /**
     * Returns a snapshot of additional store stats.
     */
    public Stats stats() {
        Stats stats = new Stats();
        mPageManager.addTo(stats);
        return stats;
    }

    public static final class Stats {
        public long totalPages;
        public long freePages;

        public String toString() {
            return "PageStore.Stats {totalPages=" + totalPages + ", freePages=" + freePages + '}';
        }
    }

    /**
     * Returns a BitSet where each clear bit indicates a free page.
     */
    public BitSet tracePages() throws IOException {
        BitSet pages = new BitSet();
        mPageManager.markAllPages(pages);
        mPageManager.traceFreePages(pages);
        return pages;
    }

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     */
    public void readPage(long id, byte[] buf) throws IOException {
        readPage(id, buf, 0);
    }

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     * @param offset offset into data buffer
     */
    public void readPage(long id, byte[] buf, int offset) throws IOException {
        try {
            mPageArray.readPage(id, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    /**
     * Reads a part of a page without locking. Caller must ensure that a
     * deleted page is not read during or after a commit.
     *
     * @param id page id to read
     * @param start start of page to read
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length length to read
     */
    public void readPartial(long id, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        try {
            mPageArray.readPartial(id, start, buf, offset, length);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    /**
     * Allocates a page to be written to.
     *
     * @return page id; never zero or one
     */
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

    /**
     * Tries to allocates a page to be written to, but without ever creating a
     * new page.
     *
     * @return page id; never one; zero if no pages are available
     */
    public long tryAllocPage() throws IOException {
        mCommitLock.readLock().lock();
        try {
            return mPageManager.tryAllocPage();
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    /**
     * Writes to an allocated page, but doesn't commit it. A written page is
     * immediately readable even if not committed. An uncommitted page can be
     * deleted, but it remains readable until after a commit.
     *
     * @param id previously reserved page id
     * @param buf data to write
     * @throws IllegalArgumentException if id verification is supported and id
     * was not reserved
     */
    public void writePage(long id, byte[] buf) throws IOException {
        writePage(id, buf, 0);
    }

    /**
     * Writes to an allocated page, but doesn't commit it. A written page is
     * immediately readable even if not committed. An uncommitted page can be
     * deleted, but it remains readable until after a commit.
     *
     * @param id previously reserved page id
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IllegalArgumentException if id verification is supported and id
     * was not reserved
     */
    public void writePage(long id, byte[] buf, int offset) throws IOException {
        checkId(id);
        try {
            mPageArray.writePage(id, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    /**
     * Deletes a page, but doesn't commit it. Deleted pages are not used for
     * new writes, and they are still readable until after a commit. Caller
     * must ensure that a page is deleted at most once between commits.
     */
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
    }

    /**
     * Recycles a page for immediate re-use. Recycle should only be called for
     * pages whose contents are known to be unused. If recycle "bin" is full,
     * page might get deleted instead. Recycle bin is also emptied by commit.
     */
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

    /**
     * Allocates pages for immediate use. Even if requested page count is zero,
     * this method ensures the file system has allocated all pages.
     */
    public void allocatePages(long pageCount) throws IOException {
        mCommitLock.readLock().lock();
        try {
            mPageManager.allocatePages(pageCount);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    public Lock sharedCommitLock() {
        return mCommitLock.readLock();
    }

    /**
     * Access the exclusive commit lock, which is acquired by the commit method.
     */
    public Lock exclusiveCommitLock() {
        return mCommitLock.writeLock();
    }

    /**
     * Durably commits all writes and deletes to the underlying device.
     *
     * @param callback optional callback to run during commit
     */
    public void commit(final CommitCallback callback) throws IOException {
        mCommitLock.writeLock().lock();
        mCommitLock.readLock().lock();

        final int commitNumber = mCommitNumber + 1;

        // Downgrade and keep read lock. This prevents another commit from
        // starting concurrently.
        mCommitLock.writeLock().unlock();

        try {
            final byte[] header = new byte[pageSize()];
            mPageManager.commitStart(header, I_MANAGER_HEADER);
            // Invoke the callback to ensure all dirty pages get written.
            byte[] extra = callback == null ? null : callback.prepare();
            commitHeader(header, commitNumber, extra);
            mPageManager.commitEnd(header, I_MANAGER_HEADER);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    public static interface CommitCallback {
        /**
         * Write all reserved pages which should be committed and return extra
         * data. Extra commit data is stored in PageStore header.
         *
         * @return optional extra data to commit, up to 256 bytes
         */
        public byte[] prepare() throws IOException;
    }

    /**
     * Reads extra data that was stored with the last commit.
     *
     * @param extra optional extra data which was committed, up to 256 bytes
     */
    public void readExtraCommitData(byte[] extra) throws IOException {
        try {
            mCommitLock.readLock().lock();
            try {
                mPageArray.readPartial(mCommitNumber & 1, I_EXTRA_DATA, extra, 0, extra.length);
            } finally {
                mCommitLock.readLock().unlock();
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (mPageArray != null) {
            mPageArray.close();
        }
    }

    /**
     * @see PageArray#beginSnapshot
     */
    Snapshot beginSnapshot(TempFileManager tfm, int cluster, OutputStream out) throws IOException {
        byte[] header = new byte[MINIMUM_PAGE_SIZE];
        try {
            mCommitLock.readLock().lock();
            try {
                mPageArray.readPartial(mCommitNumber & 1, 0, header, 0, header.length);
                long pageCount = PageManager.readTotalPageCount(header, I_MANAGER_HEADER);
                return mPageArray.beginSnapshot(tfm, pageCount, cluster, out);
            } finally {
                mCommitLock.readLock().unlock();
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    /**
     * @param in snapshot source; does not require extra buffering; not auto-closed
     */
    static PageStore restoreFromSnapshot(File file, EnumSet<OpenOption> options, InputStream in)
        throws IOException
    {
        int pageSize = PageArray.restoreFromSnapshot(file, options, in);
        return new PageStore(file, options, pageSize);
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
    private void commitHeader(final byte[] header, final int commitNumber, final byte[] extra)
        throws IOException
    {
        final PageArray array = mPageArray;

        writeLong(header, I_MAGIC_NUMBER, MAGIC_NUMBER);
        writeInt (header, I_PAGE_SIZE, array.pageSize());
        writeInt (header, I_COMMIT_NUMBER, commitNumber);

        if (extra != null) {
            // Exception is thrown if extra data exceeds header length.
            System.arraycopy(extra, 0, header, I_EXTRA_DATA, extra.length);
        } 

        // Durably write the new page store header before returning
        // from this method, to ensure that the manager doesn't start
        // returning uncommitted pages. This would prevent rollback
        // from working because the old pages would get overwritten.
        setHeaderChecksum(header);

        // Write multiple header copies in the page, in case special recovery is required.
        int dupCount = header.length / MINIMUM_PAGE_SIZE;
        for (int i=1; i<dupCount; i++) {
            System.arraycopy(header, 0, header, i * MINIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE);
        }

        // Boost thread priorty during sync, to ensure it completes quickly.
        // Some file systems don't sync on a "snapshot", but continue sync'ng
        // pages which are dirtied during the sync itself.
        final Thread t = Thread.currentThread();
        final int original = t.getPriority();
        try {
            if (original != Thread.MAX_PRIORITY) {
                try {
                    t.setPriority(Thread.MAX_PRIORITY);
                } catch (SecurityException e) {
                }
            }

            // Ensure all writes are flushed before flushing the header. There's
            // otherwise no ordering guarantees.
            array.sync(false);
            array.writePage(commitNumber & 1, header);
            array.sync(true);
        } finally {
            if (t.getPriority() != original) {
                try {
                    t.setPriority(original);
                } catch (SecurityException e) {
                }
            }
        }

        mCommitNumber = commitNumber;
    }

    private static int setHeaderChecksum(byte[] header) {
        // Clear checksum field before computing.
        writeInt(header, I_CHECKSUM, 0);
        CRC32 crc = new CRC32();
        crc.update(header, 0, MINIMUM_PAGE_SIZE);
        int checksum = (int) crc.getValue();
        writeInt(header, I_CHECKSUM, checksum);
        return checksum;
    }

    private byte[] readHeader(int id) throws IOException {
        byte[] header = new byte[MINIMUM_PAGE_SIZE];

        try {
            mPageArray.readPartial(id, 0, header, 0, header.length);
        } catch (EOFException e) {
            throw new CorruptPageStoreException("File is smaller than expected");
        }

        long magic = readLong(header, I_MAGIC_NUMBER);
        if (magic != MAGIC_NUMBER) {
            throw new CorruptPageStoreException("Wrong magic number: " + magic);
        }

        int checksum = readInt(header, I_CHECKSUM);

        int newChecksum = setHeaderChecksum(header);
        if (newChecksum != checksum) {
            throw new CorruptPageStoreException
                ("Header checksum mismatch: " + newChecksum + " != " + checksum);
        }

        return header;
    }
}
