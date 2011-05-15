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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.UUID;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.zip.CRC32;

/**
 * Low-level support for storing fixed size pages in pair of files. Page size should be a
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
 * <p>Commit operations on FilePageStore can take a long time to finish, since new writes
 * are being made into the same file which is being durably written. Use {@link
 * DualFilePageStore} to reduce the impact concurrent writes have on commit performance.
 *
 * @author Brian S O'Neill
 */
public class DualFilePageStore implements PageStore {
    /*

    Header format for first page in each file, which is always 512 bytes:

    +------------------------------------------+
    | long: magic number                       |
    | long: file uid                           |
    | int:  page size                          |
    | int:  commit number                      |
    | int:  checksum                           |
    | allocator header (48 bytes)              |
    +------------------------------------------+
    | reserved (180 bytes)                     |
    +------------------------------------------+
    | extra data (256 bytes)                   |
    +------------------------------------------+

    */

    private static final long MAGIC_NUMBER = 7359069613485731323L;

    // Indexes of entries in header node.
    private static final int I_MAGIC_NUMBER = 0;
    private static final int I_FILE_UID = 8;
    private static final int I_PAGE_SIZE = I_FILE_UID + 8;
    private static final int I_COMMIT_NUMBER = I_PAGE_SIZE + 4;
    private static final int I_CHECKSUM = I_COMMIT_NUMBER + 4;
    private static final int I_ALLOCATOR_HEADER = I_CHECKSUM + 4;
    private static final int I_EXTRA_DATA = 256;

    private static final int MINIMUM_PAGE_SIZE = 512;
    static final int DEFAULT_PAGE_SIZE = 4096;

    private final PageArray mPageArray0;
    private final PageArray mPageArray1;

    private final long mUid;

    private final ReadWriteLock mCommitLock;
    // Commit number is the highest one which has been committed.
    private int mCommitNumber;

    private static class Allocator {
        final PageAllocator mPageAllocator;
        final long mFlag;

        Allocator(PageAllocator pageAllocator, int flag) {
            mPageAllocator = pageAllocator;
            mFlag = flag;
        }

        long allocPage() throws IOException {
            return (mPageAllocator.allocPage() << 1) | mFlag;
        }
    }

    private final Allocator mAllocator0;
    private final Allocator mAllocator1;

    private Allocator mActivateAllocator;

    private IdStack mPendingDeletes;

    private static class FPA extends FilePageArray {
        FPA(File file, boolean readOnly, int pageSize, int openFileCount) throws IOException {
            super(file, readOnly, pageSize, openFileCount);
        }

        @Override
        protected int readPageSize(RandomAccessFile raf) throws IOException {
            long length = raf.length();

            if (length >= MINIMUM_PAGE_SIZE) {
                raf.seek(I_PAGE_SIZE);
                int pageSize = raf.readInt();
                if (pageSize >= MINIMUM_PAGE_SIZE && pageSize <= length) {
                    // Magic number and checksum will be examined later, so no
                    // harm in possibly returning a bugus page size.
                    return pageSize;
                }
            }

            return 0;
        }
    }

    public DualFilePageStore(File file0, File file1) throws IOException {
        this(file0, file1, false);
    }

    public DualFilePageStore(File file0, File file1, boolean readOnly) throws IOException {
        this(file0, file1, readOnly, DEFAULT_PAGE_SIZE);
    }

    public DualFilePageStore(File file0, File file1, boolean readOnly, int pageSize)
        throws IOException
    {
        this(file0, file1, readOnly, pageSize, 32);
    }

    public DualFilePageStore(File file0, File file1, boolean readOnly,
                             int pageSize, int openFileCount)
        throws IOException
    {
        if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size must be at least " + MINIMUM_PAGE_SIZE + ": " + pageSize);
        }

        if (file0.getCanonicalPath().equals(file1.getCanonicalPath())) {
            throw new IllegalArgumentException
                ("Files are the same: " + file0 + ", " + file1);
        }

        long uid;
        PageAllocator allocator0, allocator1;
        int commitNumber0, commitNumber1;

        try {
            // FPA is an inner class which can read the existing page size.
            mPageArray0 = new FPA(file0, readOnly, pageSize, openFileCount);
            mPageArray1 = new FPA(file1, readOnly, pageSize, openFileCount);

            if (mPageArray0.getPageCount() == 0 && mPageArray1.getPageCount() == 0) {
                // Newly created files.
                allocator0 = new PageAllocator(mPageArray0, 1);
                allocator1 = new PageAllocator(mPageArray1, 1);

                UUID uuid = UUID.randomUUID();
                uid = (uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits()) & ~1;

                commitNumber0 = 0;
                commitNumber1 = 1;

                commitHeader(allocator0, uid,     commitNumber0, null);
                commitHeader(allocator1, uid | 1, commitNumber1, null);
            } else {
                // Opened existing files.
                byte[] header0 = readHeader(mPageArray0);
                byte[] header1 = readHeader(mPageArray1);

                long uid0 = DataIO.readLong(header0, I_FILE_UID);
                long uid1 = DataIO.readLong(header1, I_FILE_UID);

                if ((uid0 & ~1) != (uid1 & ~1)) {
                    throw new CorruptPageStoreException
                        ("Mismatched file identifiers: " + uid0 + ", " + uid1);
                }

                if (((uid0 & 1) != 0) || ((uid1 & 1) == 0)) {
                    throw new CorruptPageStoreException
                        ("Malformed file identifiers: " + uid0 + ", " + uid1);
                }

                uid = uid0;

                if (mPageArray0.pageSize() != mPageArray1.pageSize()) {
                    throw new CorruptPageStoreException
                        ("Mismatched file page sizes: " +
                         mPageArray0.pageSize() + " != " + mPageArray1.pageSize());
                }

                commitNumber0 = DataIO.readInt(header0, I_COMMIT_NUMBER);
                commitNumber1 = DataIO.readInt(header1, I_COMMIT_NUMBER);

                if (Math.abs(commitNumber0 - commitNumber1) != 1) {
                    throw new CorruptPageStoreException
                        ("Non-consecutive commit numbers: " +
                         commitNumber0 + ", " + commitNumber1);
                }

                if ((commitNumber0 & 1) != 0) {
                    throw new CorruptPageStoreException
                        ("Illegal commit number for file 0: " + commitNumber0);
                }

                allocator0 = new PageAllocator(mPageArray0, header0, I_ALLOCATOR_HEADER);
                allocator1 = new PageAllocator(mPageArray1, header1, I_ALLOCATOR_HEADER);
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }

        mUid = uid;

        mCommitLock = new ReentrantReadWriteLock(true);

        mCommitLock.writeLock().lock();
        try {
            mAllocator0 = new Allocator(allocator0, 0);
            mAllocator1 = new Allocator(allocator1, 1);

            if (commitNumber0 < commitNumber1) {
                mCommitNumber = commitNumber1;
                mActivateAllocator = mAllocator0;
            } else {
                mCommitNumber = commitNumber0;
                mActivateAllocator = mAllocator1;
            }
        } finally {
            mCommitLock.writeLock().unlock();
        }
    }

    @Override
    public int pageSize() {
        return mPageArray0.pageSize();
    }

    @Override
    public Stats stats() {
        // FIXME
        throw null;
    }

    @Override
    public void readPage(long id, byte[] buf) throws IOException {
        readPage(id, buf, 0);
    }

    @Override
    public void readPage(long id, byte[] buf, int offset) throws IOException {
        PageArray array = (id & 1) == 0 ? mPageArray0 : mPageArray1;
        try {
            array.readPage(id >> 1, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void readPartial(long id, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        PageArray array = (id & 1) == 0 ? mPageArray0 : mPageArray1;
        try {
            array.readPartial(id >> 1, start, buf, offset, length);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public long writePage(byte[] buf) throws IOException {
        return writePage(buf, 0);
    }

    @Override
    public long writePage(byte[] buf, int offset) throws IOException {
        long id = reservePage();
        writeReservedPage(id, buf, offset);
        return id;
    }

    @Override
    public long reservePage() throws IOException {
        mCommitLock.readLock().lock();
        try {
            return mActivateAllocator.allocPage();
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    @Override
    public void writeReservedPage(long id, byte[] buf) throws IOException {
        writeReservedPage(id, buf, 0);
    }

    @Override
    public void writeReservedPage(long id, byte[] buf, int offset) throws IOException {
        PageArray array = (id & 1) == 0 ? mPageArray0 : mPageArray1;
        try {
            array.writePage(id >> 1, buf, offset);
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void deletePage(long id) throws IOException {
        Allocator allocator = (id & 1) == 0 ? mAllocator0 : mAllocator1;
        id >>= 1;
        mCommitLock.readLock().lock();
        try {
            IdStack pending;
            if (allocator == mActivateAllocator || (pending = mPendingDeletes) == null) {
                allocator.mPageAllocator.deletePage(id);
            } else {
                // Don't delete until commit finishes.
                synchronized (pending) {
                    pending.add(id);
                }
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }
    }

    @Override
    public Lock sharedCommitLock() {
        return mCommitLock.readLock();
    }

    @Override
    public Lock exclusiveCommitLock() {
        return mCommitLock.writeLock();
    }

    @Override
    public void commit(CommitCallback callback) throws IOException {
        final IdStack pendingDeletes = new IdStack();
        long uid = mUid;

        mCommitLock.writeLock().lock();
        mCommitLock.readLock().lock();

        int commitNumber = mCommitNumber + 1;

        final PageAllocator lastAllocator;
        if (mActivateAllocator == mAllocator0) {
            uid &= ~1;
            lastAllocator = mAllocator0.mPageAllocator;
            mActivateAllocator = mAllocator1;
        } else {
            uid |= 1;
            lastAllocator = mAllocator1.mPageAllocator;
            mActivateAllocator = mAllocator0;
        }

        // Deletes against the last file must be queued during commit, since
        // they are not part of the commit.
        mPendingDeletes = pendingDeletes;

        // Downgrade and keep read lock. This prevents another commit from
        // starting concurrently.
        mCommitLock.writeLock().unlock();

        // At this point, all allocations go to the other file. The previously
        // active file will become the highest committed file. The first flush
        // operation is slow, and it is performed without locks held. Since the
        // last file is not being used for allocations, and deletes are queued,
        // it won't be modified during the flush.

        try {
            // Invoke the callback to ensure all dirty pages get written.
            byte[] extra = callback == null ? null : callback.prepare();

            // Ensure all writes are flushed before flushing the
            // header. There's otherwise no ordering guarantees. This is also
            // the slowest step in the entire commit sequence.
            lastAllocator.pageArray().flush();

            commitHeader(lastAllocator, uid, commitNumber, extra);

            mCommitNumber = commitNumber;
        } catch (Throwable e) {
            throw closeOnFailure(e);
        } finally {
            mCommitLock.readLock().unlock();
        }

        // Now apply all the pending deletes that occurred during the commit.

        mCommitLock.writeLock().lock();
        try {
            mPendingDeletes = null;
        } finally {
            mCommitLock.writeLock().unlock();
        }

        synchronized (pendingDeletes) {
            int size = pendingDeletes.size();
            for (int i=0; i<size; i++) {
                lastAllocator.deletePage(pendingDeletes.remove());
            }
        }
    }

    @Override
    public void readExtraCommitData(byte[] extra) throws IOException {
        try {
            mCommitLock.readLock().lock();
            try {
                // Select the array which was last committed.
                PageArray array = (mCommitNumber & 1) == 0 ? mPageArray0 : mPageArray1;
                array.readPartial(0, I_EXTRA_DATA, extra, 0, extra.length);
            } finally {
                mCommitLock.readLock().unlock();
            }
        } catch (Throwable e) {
            throw closeOnFailure(e);
        }
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;

        for (PageArray pa : new PageArray[] {mPageArray0, mPageArray1}) {
            if (pa != null) {
                try {
                    pa.close();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private IOException closeOnFailure(Throwable e) throws IOException {
        throw Utils.closeOnFailure(this, e);
    }

    private static void commitHeader(PageAllocator allocator,
                                     long uid,
                                     int commitNumber,
                                     byte[] extra)
        throws IOException
    {
        final PageArray array = allocator.pageArray();
        final byte[] header = new byte[array.pageSize()];

        DataIO.writeLong(header, I_MAGIC_NUMBER, MAGIC_NUMBER);
        DataIO.writeLong(header, I_FILE_UID, uid);
        DataIO.writeInt(header, I_PAGE_SIZE, array.pageSize());
        DataIO.writeInt(header, I_COMMIT_NUMBER, commitNumber);

        if (extra != null) {
            // Exception is thrown if extra data exceeds header length.
            System.arraycopy(extra, 0, header, I_EXTRA_DATA, extra.length);
        } 

        allocator.commit(header, I_ALLOCATOR_HEADER, new PageAllocator.CommitReady() {
            @Override
            public void ready(int headerSize) throws IOException {
                // Durably write the new page store header before returning
                // from this method, to ensure that the allocator doesn't start
                // returning uncommitted pages. This would prevent rollback
                // from working because the old pages would get overwitten.
                setHeaderChecksum(header);
                array.writePage(0, header);
                array.flush();
            }
        });
    }

    private static int setHeaderChecksum(byte[] header) {
        // Clear checksum field before computing.
        DataIO.writeInt(header, I_CHECKSUM, 0);
        CRC32 crc = new CRC32();
        crc.update(header, 0, MINIMUM_PAGE_SIZE);
        int checksum = (int) crc.getValue();
        DataIO.writeInt(header, I_CHECKSUM, checksum);
        return checksum;
    }

    private static byte[] readHeader(PageArray array) throws IOException {
        byte[] header = new byte[MINIMUM_PAGE_SIZE];

        try {
            array.readPartial(0, 0, header, 0, header.length);
        } catch (EOFException e) {
            throw new CorruptPageStoreException("File is smaller than expected");
        }

        long magic = DataIO.readLong(header, I_MAGIC_NUMBER);
        if (magic != MAGIC_NUMBER) {
            throw new CorruptPageStoreException("Wrong magic number: " + magic);
        }

        int checksum = DataIO.readInt(header, I_CHECKSUM);

        int newChecksum = setHeaderChecksum(header);
        if (newChecksum != checksum) {
            throw new CorruptPageStoreException
                ("Header checksum mismatch: " + newChecksum + " != " + checksum);
        }

        return header;
    }
}
