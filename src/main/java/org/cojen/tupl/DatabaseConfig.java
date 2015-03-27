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

import java.lang.management.ManagementFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;

import java.util.EnumSet;
import java.util.Properties;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.ext.ReplicationManager;

import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.Utils.*;

/**
 * Configuration options used when {@link Database#open opening} a database.
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    File mBaseFile;
    boolean mMkdirs;
    File[] mDataFiles;
    boolean mMapDataFiles;
    transient PageArray mDataPageArray;
    FileFactory mFileFactory;
    long mMinCachedBytes;
    long mMaxCachedBytes;
    long mSecondaryCacheSize;
    DurabilityMode mDurabilityMode;
    LockUpgradeRule mLockUpgradeRule;
    long mLockTimeoutNanos;
    long mCheckpointRateNanos;
    long mCheckpointSizeThreshold;
    long mCheckpointDelayThresholdNanos;
    transient EventListener mEventListener;
    boolean mFileSync;
    boolean mReadOnly;
    int mPageSize;
    boolean mCachePriming;
    transient ReplicationManager mReplManager;
    int mMaxReplicaThreads;
    transient Crypto mCrypto;

    // Fields are set as a side-effect of constructing a replicated Database.
    transient long mReplRecoveryStartNanos;
    transient long mReplInitialTxnId;

    public DatabaseConfig() {
        createFilePath(true);
        durabilityMode(null);
        lockTimeout(1, TimeUnit.SECONDS);
        checkpointRate(1, TimeUnit.SECONDS);
        checkpointSizeThreshold(1024 * 1024);
        checkpointDelayThreshold(1, TimeUnit.MINUTES);
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public DatabaseConfig baseFile(File file) {
        mBaseFile = file == null ? null : abs(file);
        return this;
    }

    /**
     * Set the base file name for the database, which must reside in an
     * ordinary file directory. If no base file is provided, database is
     * non-durable and cannot exceed the size of the cache.
     */
    public DatabaseConfig baseFilePath(String path) {
        mBaseFile = path == null ? null : abs(new File(path));
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
        dataFiles(file);
        return this;
    }

    /**
     * Stripe the database data file across several files, expected to be on
     * separate devices. The data files can refer to ordinary files or to raw
     * block devices.
     */
    public DatabaseConfig dataFiles(File... files) {
        if (files == null || files.length == 0) {
            mDataFiles = null;
        } else {
            File[] dataFiles = new File[files.length];
            for (int i=0; i<files.length; i++) {
                dataFiles[i] = abs(files[i]);
            }
            mDataFiles = dataFiles;
            mDataPageArray = null;
        }
        return this;
    }

    /**
     * Enable memory mapping of the data files. Not recommended for 32-bit platforms or for
     * databases which don't fit entirely in main memory.
     */
    public DatabaseConfig mapDataFiles(boolean mapped) {
        mMapDataFiles = mapped;
        return this;
    }

    /**
     * Use a custom storage layer instead of the default data file.
     */
    public DatabaseConfig dataPageArray(PageArray array) {
        mDataPageArray = array;
        if (array != null) {
            int expected = mDataPageArray.pageSize();
            if (mPageSize != 0 && mPageSize != expected) {
                throw new IllegalArgumentException
                    ("Page size doesn't match data page array: " + mPageSize + " != " + expected);
            }
            mDataFiles = null;
            mPageSize = expected;
        }
        return this;
    }

    /**
     * Optionally define a custom factory for every file and directory created by the
     * database.
     */
    public DatabaseConfig fileFactory(FileFactory factory) {
        mFileFactory = factory;
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
     * Set the size of the secondary off-heap cache, which is empty by default. A secondary
     * cache is slower than a primary cache, but a very large primary cache can cause high
     * garbage collection overhead. The -XX:MaxDirectMemorySize Java option might be required
     * when specifying a secondary cache.
     *
     * @param size secondary cache size, in bytes
     */
    public DatabaseConfig secondaryCacheSize(long size) {
        if (size < 0) {
            // Reserve use of negative size.
            throw new IllegalArgumentException();
        }
        mSecondaryCacheSize = size;
        return this;
    }

    /**
     * Set the default transaction durability mode, which is {@link
     * DurabilityMode#SYNC SYNC} if not overridden. If database itself is
     * non-durabile, durability modes are ignored.
     */
    public DatabaseConfig durabilityMode(DurabilityMode durabilityMode) {
        if (durabilityMode == null) {
            durabilityMode = DurabilityMode.SYNC;
        }
        mDurabilityMode = durabilityMode;
        return this;
    }

    /**
     * Set the default lock upgrade rule, which is {@link LockUpgradeRule#STRICT STRICT} if not
     * overridden.
     */
    public DatabaseConfig lockUpgradeRule(LockUpgradeRule lockUpgradeRule) {
        if (lockUpgradeRule == null) {
            lockUpgradeRule = LockUpgradeRule.STRICT;
        }
        mLockUpgradeRule = lockUpgradeRule;
        return this;
    }

    /**
     * Set the default lock acquisition timeout, which is 1 second if not
     * overridden. A negative timeout is infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    public DatabaseConfig lockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = toNanos(timeout, unit);
        return this;
    }

    /**
     * Set the rate at which {@link Database#checkpoint checkpoints} are
     * automatically performed. Default rate is 1 second. Pass a negative value
     * to disable automatic checkpoints.
     *
     * @param unit required unit if rate is more than zero
     */
    public DatabaseConfig checkpointRate(long rate, TimeUnit unit) {
        mCheckpointRateNanos = toNanos(rate, unit);
        return this;
    }

    /**
     * Set the minimum redo log size required for an automatic {@link Database#checkpoint
     * checkpoint} to actually be performed. Default is 1 MiB. If database is used primarily
     * for non-transactional operations, the threshold should be set to zero.
     */
    public DatabaseConfig checkpointSizeThreshold(long bytes) {
        mCheckpointSizeThreshold = bytes;
        return this;
    }

    /**
     * Set the maximum delay before an automatic {@link Database#checkpoint checkpoint} is
     * performed, regardless of the redo log size threshold. Default is 1 minute, and a
     * negative delay is infinite. If database is used primarily for non-transactional
     * operations, the threshold should be set to zero.
     *
     * @param unit required unit if delay is more than zero
     */
    public DatabaseConfig checkpointDelayThreshold(long delay, TimeUnit unit) {
        mCheckpointDelayThresholdNanos = toNanos(delay, unit);
        return this;
    }

    /**
     * Set a listener which receives notifications of actions being performed
     * by the database.
     */
    public DatabaseConfig eventListener(EventListener listener) {
        mEventListener = listener;
        return this;
    }

    /**
     * Set true to ensure all writes to the main database file are immediately
     * durable, although not checkpointed. This option typically reduces
     * overall performance, but checkpoints complete more quickly. As a result,
     * the main database file requires less pre-allocated pages and is smaller.
     */
    public DatabaseConfig syncWrites(boolean fileSync) {
        mFileSync = fileSync;
        return this;
    }

    /*
    public DatabaseConfig readOnly(boolean readOnly) {
        mReadOnly = readOnly;
        return this;
    }
    */

    /**
     * Set the page size, which is 4096 bytes by default.
     */
    public DatabaseConfig pageSize(int size) {
        if (mDataPageArray != null) {
            int expected = mDataPageArray.pageSize();
            if (expected != size) {
                throw new IllegalArgumentException
                    ("Page size doesn't match data page array: " + size + " != " + expected);
            }
        }
        mPageSize = size;
        return this;
    }

    /**
     * Enable automatic cache priming, which writes a priming set into a special file when the
     * database is cleanly shutdown. When opened again, the priming set is applied and the file
     * is deleted. Option has no effect if database is non-durable.
     *
     * @see Database#createCachePrimer
     */
    public DatabaseConfig cachePriming(boolean priming) {
        mCachePriming = priming;
        return this;
    }

    /**
     * Enable replication by providing a {@link ReplicationManager} instance.
     */
    public DatabaseConfig replicate(ReplicationManager manager) {
        mReplManager = manager;
        return this;
    }

    /**
     * If replication is enabled, specify the maximum number of threads to process incoming
     * changes. Default is the number of available processors. If a negative number is
     * provided, the actual number applied is {@code (-num * availableProcessors)}.
     */
    public DatabaseConfig maxReplicaThreads(int num) {
        mMaxReplicaThreads = num;
        return this;
    }

    /**
     * Enable full encryption of the data files, transaction logs, snapshots, and cache priming
     * sets. Option has no effect if database is non-durable. If replication is enabled,
     * encryption is not applied to the replication stream. A {@link ReplicationManager}
     * implementation must perform its own encryption.
     *
     * <p>Allocated but never used pages within the data files are unencrypted, although they
     * contain no information. Temporary files used by in-progress snapshots contain encrypted
     * content.
     */
    public DatabaseConfig encrypt(Crypto crypto) {
        mCrypto = crypto;
        return this;
    }

    @Override
    public DatabaseConfig clone() {
        try {
            return (DatabaseConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw rethrow(e);
        }
    }

    /**
     * Optionally returns a new or shared page cache.
     */
    PageCache pageCache(EventListener listener) {
        long size = mSecondaryCacheSize;
        if (size <= 0) {
            return null;
        }

        if (listener != null) {
            listener.notify(EventType.CACHE_INIT_BEGIN,
                            "Initializing %1$d bytes for secondary cache", size);
        }

        return new PartitionedPageCache(size, mPageSize);

        // Note: The page cache could be shared with other Database instances, if they have the
        // same page size. The upper 2 bytes of the page id are unused, and so the cache can be
        // shared up to 65536 times. A closed database can free up its slot if all of its
        // lingering cache entries are explicitly removed.
    }

    /**
     * Performs configuration check and returns the applicable data files. Null is returned
     * when base file is null or if a custom PageArray should be used.
     */
    File[] dataFiles() {
        if (mReplManager != null) {
            long encoding = mReplManager.encoding();
            if (encoding == 0) {
                throw new IllegalArgumentException
                    ("Illegal replication manager encoding: " + encoding);
            }
        }

        File[] dataFiles = mDataFiles;
        if (mBaseFile == null) {
            if (dataFiles != null && dataFiles.length > 0) {
                throw new IllegalArgumentException
                    ("Cannot specify data files when no base file is provided");
            }
            return null;
        }

        if (mBaseFile.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + mBaseFile);
        }

        if (mDataPageArray != null) {
            // Return after the base file checks have been performed.
            return null;
        }

        if (dataFiles == null || dataFiles.length == 0) {
            dataFiles = new File[] {new File(mBaseFile.getPath() + ".db")};
        }

        for (File dataFile : dataFiles) {
            if (dataFile.isDirectory()) {
                throw new IllegalArgumentException("Data file is a directory: " + dataFile);
            }
        }

        return dataFiles;
    }

    EnumSet<OpenOption> createOpenOptions() {
        EnumSet<OpenOption> options = EnumSet.noneOf(OpenOption.class);
        if (mReadOnly) {
            options.add(OpenOption.READ_ONLY);
        }
        if (mFileSync) {
            options.add(OpenOption.SYNC_IO);
        }
        if (mMapDataFiles) {
            options.add(OpenOption.MAPPED);
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
        set(props, "mapDataFiles", mMapDataFiles);

        if (mDataFiles != null && mDataFiles.length > 0) {
            if (mDataFiles.length == 1) {
                set(props, "dataFile", mDataFiles[0]);
            } else {
                StringBuilder b = new StringBuilder();
                b.append('[');
                for (int i=0; i<mDataFiles.length; i++) {
                    if (i > 0) {
                        b.append(", ");
                    }
                    b.append(mDataFiles[i]);
                }
                b.append(']');
                props.setProperty("dataFiles", b.toString());
            }
        }

        set(props, "minCacheSize", mMinCachedBytes);
        set(props, "maxCacheSize", mMaxCachedBytes);
        set(props, "secondaryCacheSize", mSecondaryCacheSize);
        set(props, "durabilityMode", mDurabilityMode);
        set(props, "lockTimeoutNanos", mLockTimeoutNanos);
        set(props, "checkpointRateNanos", mCheckpointRateNanos);
        set(props, "checkpointSizeThreshold", mCheckpointSizeThreshold);
        set(props, "checkpointDelayThresholdNanos", mCheckpointDelayThresholdNanos);
        set(props, "syncWrites", mFileSync);
        set(props, "pageSize", mPageSize);
        set(props, "cachePriming", mCachePriming);

        props.store(w, Database.class.getName());
    }

    private static void set(Properties props, String name, Object value) {
        if (value != null) {
            props.setProperty(name, String.valueOf(value));
        }
    }

    private static File abs(File file) {
        return file.getAbsoluteFile();
    }
}
