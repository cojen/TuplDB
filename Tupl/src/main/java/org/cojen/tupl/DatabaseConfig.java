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

import java.lang.management.ManagementFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import java.util.EnumSet;
import java.util.Properties;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig {
    File mBaseFile;
    boolean mMkdirs;
    File mDataFile;
    long mMinCachedBytes;
    long mMaxCachedBytes;
    DurabilityMode mDurabilityMode;
    long mLockTimeoutNanos;
    long mCheckpointRateNanos;
    ScheduledExecutorService mCheckpointExecutor;
    boolean mFileSync;
    boolean mReadOnly;
    int mPageSize;

    public DatabaseConfig() {
        createFilePath(true);
        durabilityMode(null);
        lockTimeout(1, TimeUnit.SECONDS);
        checkpointRate(1, TimeUnit.SECONDS);
    }

    /**
     * Set the base file name for the database, which is required. The base
     * file must reside in an ordinary file directory.
     */
    public DatabaseConfig baseFile(File file) {
        mBaseFile = file;
        return this;
    }

    /**
     * Set true to create directories for the base and data file, if they don't
     * already exist. Default is true.
     */
    public DatabaseConfig createFilePath(boolean mkdirs) {
        mMkdirs = mkdirs;
        return this;
    }

    /**
     * Set the data file for the database, which by default resides in the same
     * directory as the base file. The data file can be in a separate
     * directory, and it can even be a raw block device.
     */
    public DatabaseConfig dataFile(File file) {
        mDataFile = file;
        return this;
    }

    /**
     * Set the minimum cache size, overriding the default.
     *
     * @param minBytes cache size, in bytes
     */
    public DatabaseConfig minCacheSize(long minBytes) {
        mMinCachedBytes = minBytes;
        return this;
    }

    /**
     * Set the maximum cache size, overriding the default.
     *
     * @param maxBytes cache size, in bytes
     */
    public DatabaseConfig maxCacheSize(long maxBytes) {
        mMaxCachedBytes = maxBytes;
        return this;
    }

    /**
     * Set the default transaction durability mode, which is {@link
     * DurabilityMode#SYNC SYNC} if not overridden.
     */
    public DatabaseConfig durabilityMode(DurabilityMode durabilityMode) {
        if (durabilityMode == null) {
            durabilityMode = DurabilityMode.SYNC;
        }
        mDurabilityMode = durabilityMode;
        return this;
    }

    /**
     * Set the default lock acquisition timeout, which is 1 second if not
     * overridden. A negative timeout is infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    public DatabaseConfig lockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
        return this;
    }

    /**
     * Set the rate at which {@link Database#checkpoint checkpoints} are
     * automatically performed. Default rate is 1 second. Pass a negative value
     * to disable automatic checkpoints.
     */
    public DatabaseConfig checkpointRate(long rate, TimeUnit unit) {
        mCheckpointRateNanos = Utils.toNanos(rate, unit);
        return this;
    }

    /**
     * Set an executor which runs automatic checkpoints. If not set, a
     * dedicated thread is created to run checkpoints.
     */
    public DatabaseConfig checkpointExecutor(ScheduledExecutorService executor) {
        mCheckpointExecutor = executor;
        return this;
    }

    /**
     * Set true to ensure all writes the main database file are immediately
     * durable, although not checkpointed. This option typically reduces
     * overall performance, but checkpoints complete more quickly. As a result,
     * the main database file requires less pre-allocated pages and is smaller.
     */
    public DatabaseConfig syncWrites(boolean fileSync) {
        mFileSync = fileSync;
        return this;
    }

    public DatabaseConfig readOnly(boolean readOnly) {
        mReadOnly = readOnly;
        return this;
    }

    public DatabaseConfig pageSize(int size) {
        mPageSize = size;
        return this;
    }

    /**
     * Checks that base and data files are valid and returns the applicable
     * data file.
     */
    File dataFile() {
        if (mBaseFile == null) {
            throw new IllegalArgumentException("No base file provided");
        }
        if (mBaseFile.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + mBaseFile);
        }

        File dataFile = mDataFile;
        if (dataFile == null) {
            dataFile = new File(mBaseFile.getPath() + ".db");
        }
        if (dataFile.isDirectory()) {
            throw new IllegalArgumentException("Data file is a directory: " + dataFile);
        }

        return dataFile;
    }

    EnumSet<OpenOption> createOpenOptions() {
        EnumSet<OpenOption> options = EnumSet.noneOf(OpenOption.class);
        if (mReadOnly) {
            options.add(OpenOption.READ_ONLY);
        }
        if (mFileSync) {
            options.add(OpenOption.SYNC_IO);
        }
        options.add(OpenOption.CREATE);
        return options;
    }

    void writeInfo(Writer w) throws IOException {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        String user;
        try {
            user = System.getProperty("user.name");
        } catch (SecurityException e) {
            user = null;
        }

        Properties props = new Properties();

        if (pid != null) {
            set(props, "lastOpenedByProcess", pid);
        }
        if (user != null) {
            set(props, "lastOpenedByUser", user);
        }

        set(props, "baseFile", mBaseFile);
        set(props, "createFilePath", mMkdirs);
        set(props, "dataFile", mDataFile);
        set(props, "minCacheSize", mMinCachedBytes);
        set(props, "maxCacheSize", mMaxCachedBytes);
        set(props, "durabilityMode", mDurabilityMode);
        set(props, "lockTimeoutNanos", mLockTimeoutNanos);
        set(props, "checkpointRateNanos", mCheckpointRateNanos);
        set(props, "syncWrites", mFileSync);
        set(props, "pageSize", mPageSize);

        props.store(w, Database.class.getName());
    }

    private static void set(Properties props, String name, Object value) {
        if (value != null) {
            props.setProperty(name, String.valueOf(value));
        }
    }
}
