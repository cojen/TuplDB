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
import java.io.IOException;

import java.util.BitSet;

import java.util.concurrent.locks.Lock;

/**
 * Defines storage as an array of fixed sized pages. Each page is uniquely
 * identified by a 64-bit number. Page zero and one are reserved for internal
 * use.
 *
 * @author Brian S O'Neill
 */
interface PageStore extends Closeable {
    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    public int pageSize();

    /**
     * Returns a snapshot of additional store stats.
     */
    public Stats stats();

    /**
     * Returns a BitSet where each clear bit indicates a free page.
     */
    public BitSet tracePages() throws IOException;

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     */
    public void readPage(long id, byte[] buf) throws IOException;

    /**
     * Reads a page without locking. Caller must ensure that a deleted page
     * is not read during or after a commit.
     *
     * @param id page id to read
     * @param buf receives read data
     * @param offset offset into data buffer
     */
    public void readPage(long id, byte[] buf, int offset) throws IOException;

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
        throws IOException;

    /**
     * Writes a page, but doesn't commit it. A written page is immediately
     * readable even if not committed. An uncommitted page can be deleted, but
     * it remains readable until after a commit.
     *
     * @param buf data to write
     * @return page id; never zero or one
     */
    public long writePage(byte[] buf) throws IOException;

    /**
     * Writes a page, but doesn't commit it. A written page is immediately
     * readable even if not committed. An uncommitted page can be deleted, but
     * it remains readable until after a commit.
     *
     * @param buf data to write
     * @param offset offset into data buffer
     * @return page id; never zero or one
     */
    public long writePage(byte[] buf, int offset) throws IOException;

    /**
     * Reserves a page to be written later. Reserved page cannot be written
     * to after a commit.
     *
     * @return page id; never zero or one
     */
    public long reservePage() throws IOException;

    /**
     * Writes to a previously reserved page, but doesn't commit it. A written
     * page is immediately readable even if not committed. An uncommitted
     * page can be deleted, but it remains readable until after a commit.
     *
     * @param id previously reserved page id
     * @param buf data to write
     * @throws IllegalArgumentException if id verification is supported and id
     * was not reserved
     */
    public void writeReservedPage(long id, byte[] buf) throws IOException;

    /**
     * Writes to a previously reserved page, but doesn't commit it. A written
     * page is immediately readable even if not committed. An uncommitted
     * page can be deleted, but it remains readable until after a commit.
     *
     * @param id previously reserved page id
     * @param buf data to write
     * @param offset offset into data buffer
     * @throws IllegalArgumentException if id verification is supported and id
     * was not reserved
     */
    public void writeReservedPage(long id, byte[] buf, int offset) throws IOException;

    /**
     * Deletes a page, but doesn't commit it. Deleted pages are not used for
     * new writes, and they are still readable until after a commit. Caller
     * must ensure that a page is deleted at most once between commits.
     */
    public void deletePage(long id) throws IOException;

    /**
     * Preallocates pages for use later. Preallocation is not permanent until
     * after commit is called.
     */
    public void preallocate(long pageCount) throws IOException;

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    public Lock sharedCommitLock();

    /**
     * Access the exclusive commit lock, which is acquired by the commit method.
     */
    public Lock exclusiveCommitLock();

    /**
     * Durably commits all writes and deletes to the underlying device.
     *
     * @param callback optional callback to run during commit
     */
    public void commit(CommitCallback callback) throws IOException;

    /**
     * Reads extra data that was stored with the last commit.
     *
     * @param extra optional extra data which was committed, up to 256 bytes
     */
    public void readExtraCommitData(byte[] extra) throws IOException;

    public static final class Stats {
        public long totalPages;
        public long freePages;

        public String toString() {
            return "PageStore.Stats {totalPages=" + totalPages + ", freePages=" + freePages + '}';
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
}
