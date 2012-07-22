/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.IOException;

import java.util.BitSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Brian S O'Neill
 * @see DurablePageDb
 * @see NonPageDb
 */
abstract class PageDb extends CauseCloseable {
    final ReadWriteLock mCommitLock;

    PageDb() {
        // Should be fair in order for exclusive lock request to de-prioritize
        // itself by timing out and retrying. See Database.checkpoint.
        mCommitLock = new ReentrantReadWriteLock(true);
    }

    /**
     * Returns a copy of the database id.
     */
    public abstract byte[] databaseId();

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    public abstract int pageSize();

    /**
     * Returns a snapshot of additional store stats.
     */
    public abstract Stats stats();

    public static final class Stats {
        public long totalPages;
        public long freePages;

        public String toString() {
            return "PageDb.Stats {totalPages=" + totalPages + ", freePages=" + freePages + '}';
        }
    }

    /**
     * Returns a BitSet where each clear bit indicates a free page.
     */
    public abstract BitSet tracePages() throws IOException;

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     */
    public abstract void readPage(long id, byte[] buf) throws IOException;

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     * @param offset offset into data buffer
     */
    public abstract void readPage(long id, byte[] buf, int offset) throws IOException;

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
    public abstract void readPartial(long id, int start, byte[] buf, int offset, int length)
        throws IOException;

    /**
     * Allocates a page to be written to.
     *
     * @return page id; never zero or one
     */
    public abstract long allocPage() throws IOException;

    /**
     * Tries to allocates a page to be written to, but without ever creating a
     * new page.
     *
     * @return page id; never one; zero if no pages are available
     */
    public abstract long tryAllocPage() throws IOException;

    /**
     * Returns the amount of recycled pages available for allocation.
     */
    public abstract long allocPageCount();

    /**
     * Writes to an allocated page, but doesn't commit it. A written page is
     * immediately readable even if not committed. An uncommitted page can be
     * deleted, but it remains readable until after a commit.
     *
     * @param id previously allocated page id
     * @param buf data to write
     */
    public abstract void writePage(long id, byte[] buf) throws IOException;

    /**
     * Writes to an allocated page, but doesn't commit it. A written page is
     * immediately readable even if not committed. An uncommitted page can be
     * deleted, but it remains readable until after a commit.
     *
     * @param id previously allocated page id
     * @param buf data to write
     * @param offset offset into data buffer
     */
    public abstract void writePage(long id, byte[] buf, int offset) throws IOException;

    /**
     * Deletes a page, but doesn't commit it. Deleted pages are not used for
     * new writes, and they are still readable until after a commit. Caller
     * must ensure that a page is deleted at most once between commits.
     */
    public abstract void deletePage(long id) throws IOException;

    /**
     * Allocates pages for immediate use. Even if requested page count is zero,
     * this method ensures the file system has allocated all pages.
     */
    public abstract void allocatePages(long pageCount) throws IOException;

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
    public abstract void commit(final CommitCallback callback) throws IOException;

    public static interface CommitCallback {
        /**
         * Write all allocated pages which should be committed and return extra
         * data. Extra commit data is stored in PageDb header.
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
    public abstract void readExtraCommitData(byte[] extra) throws IOException;
}
