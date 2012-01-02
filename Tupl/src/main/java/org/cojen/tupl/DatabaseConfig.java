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

import java.io.File;

import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig {
    File mBaseFile;
    long mMinCachedBytes;
    long mMaxCachedBytes;
    DurabilityMode mDurabilityMode;
    long mLockTimeoutNanos;
    long mFlushThresholdBytes;
    boolean mFileSync;
    boolean mReadOnly;
    int mPageSize;

    public DatabaseConfig() {
        setDurabilityMode(null);
        setLockTimeout(1, TimeUnit.SECONDS);
        setFlushThreshold(-1);
    }

    /**
     * Set the base file name for the database, which is required.
     */
    public DatabaseConfig setBaseFile(File file) {
        mBaseFile = file;
        return this;
    }

    /**
     * Set the minimum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public DatabaseConfig setMinCacheSize(long minBytes) {
        mMinCachedBytes = minBytes;
        return this;
    }

    /**
     * Set the maximum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public DatabaseConfig setMaxCacheSize(long maxBytes) {
        mMaxCachedBytes = maxBytes;
        return this;
    }

    /**
     * Set the default transaction durability mode, which is {@link
     * DurabilityMode#SYNC SYNC} if not overridden.
     */
    public DatabaseConfig setDurabilityMode(DurabilityMode durabilityMode) {
        if (durabilityMode == null) {
            durabilityMode = DurabilityMode.SYNC;
        }
        mDurabilityMode = durabilityMode;
        return this;
    }

    /**
     * Set the default lock acquisition timeout, which is 1 second if not
     * overridden. A negative timeout is infinite.
     */
    public DatabaseConfig setLockTimeoutMillis(long timeoutMillis) {
        return setLockTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Set the default lock acquisition timeout, which is 1 second if not
     * overridden. A negative timeout is infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    public DatabaseConfig setLockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
        return this;
    }

    public DatabaseConfig setFlushThreshold(long thresholdBytes) {
        mFlushThresholdBytes = thresholdBytes;
        return this;
    }

    /**
     * Set true to ensure all writes the main database file are immediately
     * durable. This option typically reduces overall performance, but
     * checkpoints complete more quickly. As a result, the main database file
     * requires less pre-allocated pages and is smaller.
     */
    public DatabaseConfig setFileWriteSync(boolean fileSync) {
        mFileSync = fileSync;
        return this;
    }

    public DatabaseConfig setReadOnly(boolean readOnly) {
        mReadOnly = readOnly;
        return this;
    }

    public DatabaseConfig setPageSize(int size) {
        mPageSize = size;
        return this;
    }
}
