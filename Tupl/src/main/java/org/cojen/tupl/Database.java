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
import java.io.File;
import java.io.IOException;

import java.util.concurrent.locks.Lock;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class Database implements Closeable {
    private static final int DEFAULT_CACHED_NODES = 1000;

    private final TreeNodeStore mNodeStore;

    private final DurabilityMode mDurabilityMode;
    private final long mDefaultLockTimeoutNanos;
    private final LockManager mLockManager;

    public Database(DatabaseConfig config) throws IOException {
        File baseFile = config.mBaseFile;
        if (baseFile == null) {
            throw new IllegalArgumentException("No base file provided");
        }
        if (baseFile.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + baseFile);
        }

        String basePath = baseFile.getPath();
        File file0 = new File(basePath + ".0.db");
        File file1 = new File(basePath + ".1.db");

        DualFilePageStore pageStore;
        if (config.mPageSize <= 0) {
            pageStore = new DualFilePageStore(file0, file1, config.mReadOnly);
        } else {
            pageStore = new DualFilePageStore(file0, file1, config.mReadOnly, config.mPageSize);
        }

        int minCache = config.mMinCache;
        int maxCache = config.mMaxCache;

        if (maxCache == 0) {
            maxCache = minCache;
            if (maxCache == 0) {
                minCache = maxCache = DEFAULT_CACHED_NODES;
            }
        }

        mDurabilityMode = config.mDurabilityMode;
        mDefaultLockTimeoutNanos = config.mLockTimeoutNanos;
        mLockManager = new LockManager(mDefaultLockTimeoutNanos);

        mNodeStore = new TreeNodeStore(mLockManager, pageStore, minCache, maxCache);
    }

    /**
     * Returns the given named index, creating it if necessary.
     */
    public Index openIndex(byte[] name) throws IOException {
        return mNodeStore.openIndex(name.clone());
    }

    /**
     * Returns the given named index, creating it if necessary. Name is UTF-8
     * encoded.
     */
    public Index openIndex(String name) throws IOException {
        return mNodeStore.openIndex(name.getBytes("UTF-8"));
    }

    public Transaction newTransaction() {
        return new Transaction
            (mLockManager, mDurabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);
    }

    public Transaction newTransaction(DurabilityMode durabilityMode) {
        if (durabilityMode == null) {
            durabilityMode = mDurabilityMode;
        }
        return new Transaction
            (mLockManager, durabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);
    }

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @throws IllegalArgumentException if id is zero
     */
    public Index indexById(long id) throws IOException {
        if (id == 0) {
            throw new IllegalArgumentException("Invalid id: " + id);
        }

        // FIXME
        throw null;
    }

    /**
     * Preallocates pages for use later.
     */
    public void preallocate(long bytes) throws IOException {
        mNodeStore.preallocate(bytes);
    }

    /**
     * Durably commit all changes to the database, while still allowing
     * concurrent access. Commit can be called by any thread, although only one
     * is permitted in at a time.
     */
    public void commit() throws IOException {
        mNodeStore.commit(false);
    }

    /**
     * Close the database without committing recent changes.
     */
    @Override
    public void close() throws IOException {
        mNodeStore.close();
    }
}
