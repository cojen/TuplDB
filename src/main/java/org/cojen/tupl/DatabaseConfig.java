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

package org.cojen.tupl;

import java.lang.management.ManagementFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

import java.lang.reflect.Method;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.TimeUnit;

import java.util.function.BiConsumer;

import org.cojen.tupl.ext.RecoveryHandler;
import org.cojen.tupl.ext.ReplicationManager;
import org.cojen.tupl.ext.TransactionHandler;

import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.repl.DatabaseReplicator;
import org.cojen.tupl.repl.ReplicatorConfig;

import static org.cojen.tupl.Utils.*;

/**
 * Configuration options used when {@link Database#open opening} a database.
 *
 * @author Brian S O'Neill
 */
public class DatabaseConfig implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    private static volatile Method cDirectOpen;
    private static volatile Method cDirectDestroy;
    private static volatile Method cDirectRestore;

    File mBaseFile;
    boolean mMkdirs;
    File[] mDataFiles;
    boolean mMapDataFiles;
    transient PageArray mDataPageArray;
    FileFactory mFileFactory;
    long mMinCachedBytes;
    long mMaxCachedBytes;
    transient RecoveryHandler mRecoveryHandler;
    long mSecondaryCacheSize;
    DurabilityMode mDurabilityMode;
    LockUpgradeRule mLockUpgradeRule;
    long mLockTimeoutNanos;
    long mCheckpointRateNanos;
    long mCheckpointSizeThreshold;
    long mCheckpointDelayThresholdNanos;
    int mMaxCheckpointThreads;
    transient EventListener mEventListener;
    BiConsumer<Database, Index> mIndexOpenListener;
    boolean mFileSync;
    boolean mReadOnly;
    int mPageSize;
    Boolean mDirectPageAccess;
    boolean mCachePriming;
    ReplicatorConfig mReplConfig;
    transient ReplicationManager mReplManager;
    int mMaxReplicaThreads;
    transient Crypto mCrypto;
    transient TransactionHandler mTxnHandler;
    Map<String, ? extends Object> mDebugOpen;

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
     * garbage collection overhead. The {@code -XX:MaxDirectMemorySize} Java option might be
     * required when specifying a secondary cache.
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
     * Specify the maximum number of threads for performing checkpointing, to speed it up. This
     * option is most useful when combined with the {@link #syncWrites syncWrites} option, or
     * when using {@link OpenOption#DIRECT_IO DIRECT_IO}. The default number of threads is
     * one. If a negative number is provided, the actual number applied is {@code (-num *
     * availableProcessors)}.
     */
    public DatabaseConfig maxCheckpointThreads(int num) {
        mMaxCheckpointThreads = num;
        return this;
    }

    /**
     * Set a listener which receives notifications of actions being performed by the
     * database. Listener implementation must be thread-safe.
     */
    public DatabaseConfig eventListener(EventListener listener) {
        mEventListener = listener;
        return this;
    }

    /**
     * Set a listener which is called when named indexes are opened. Listener implementation
     * must be thread-safe.
     */
    public DatabaseConfig indexOpenListener(BiConsumer<Database, Index> listener) {
        mIndexOpenListener = listener;
        return this;
    }

    /**
     * Set true to ensure all writes to the main database file are immediately durable,
     * although not checkpointed. This option typically reduces overall performance, but
     * checkpoints complete more quickly. As a result, the main database file requires less
     * pre-allocated pages and is smaller. Also consider specifying more {@link
     * #maxCheckpointThreads checkpoint threads} when using this option.
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
     * Set true to allocate all pages off the Java heap, offering increased performance and
     * reduced garbage collection activity. By default, direct page access is enabled if
     * supported.
     */
    public DatabaseConfig directPageAccess(boolean direct) {
        mDirectPageAccess = direct;
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
     * Enable replication using the given configuration. The base file and event listener are
     * set automatically for the given config object, when the database is opened.
     */
    public DatabaseConfig replicate(ReplicatorConfig config) {
        mReplConfig = config;
        mReplManager = null;
        return this;
    }

    /**
     * Enable replication with an explicit {@link ReplicationManager} instance.
     */
    public DatabaseConfig replicate(ReplicationManager manager) {
        mReplManager = manager;
        mReplConfig = null;
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
     * Install a transaction recovery handler, which receives unfinished transactions which
     * were {@link Transaction#prepare prepared} for two-phase commit. When replication is
     * configured, the handler is invoked when the database has become the replication leader.
     * Otherwise, the handler is invoked when the database is opened. The handler is
     * responsible for finishing the transactions, by completing the necessary commit actions,
     * or by fully rolling back. All unfinished transactions are passed to the handler via a
     * single dedicated thread.
     */
    public DatabaseConfig recoveryHandler(RecoveryHandler handler) {
        mRecoveryHandler = handler;
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

    /**
     * Provide a handler for custom transactional operations.
     */
    public DatabaseConfig customTransactionHandler(TransactionHandler handler) {
        mTxnHandler = handler;
        return this;
    }

    public TransactionHandler getCustomTransactionHandler() {
        return mTxnHandler;
    }

    /**
     * Opens the database in read-only mode for debugging purposes, and then closes it. The
     * format of the printed messages and the supported properties are subject to change.
     *
     * <ul>
     * <li>traceUndo=true to print all recovered undo log messages
     * <li>traceRedo=true to print all recovered redo log messages
     * </ul>
     *
     * @param out pass null to print to standard out
     * @param properties optional
     */
    public void debugOpen(PrintStream out, Map<String, ? extends Object> properties)
        throws IOException
    {
        if (out == null) {
            out = System.out;
        }

        if (properties == null) {
            properties = Collections.emptyMap();
        }

        DatabaseConfig config = clone();

        config.eventListener(new EventPrinter(out));
        config.mReadOnly = true;
        config.mDebugOpen = properties;

        if (config.mDirectPageAccess == null) {
            config.directPageAccess(false);
        }

        Database.open(config).close();
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
        options.add(OpenOption.RANDOM_ACCESS);
        if (mReadOnly) {
            options.add(OpenOption.READ_ONLY);
        }
        if (mFileSync) {
            options.add(OpenOption.SYNC_IO);
        }
        if (mMapDataFiles) {
            options.add(OpenOption.MAPPED);
        }
        if (mMkdirs) {
            options.add(OpenOption.CREATE);
        }
        return options;
    }

    void writeInfo(BufferedWriter w) throws IOException {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        String user;
        try {
            user = System.getProperty("user.name");
        } catch (SecurityException e) {
            user = null;
        }

        Map<String, String> props = new TreeMap<>();

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
                set(props, "dataFiles", b);
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
        set(props, "directPageAccess", mDirectPageAccess);
        set(props, "cachePriming", mCachePriming);

        w.write('#');
        w.write(Database.class.getName());
        w.newLine();

        w.write('#');
        w.write(java.time.ZonedDateTime.now().toString());
        w.newLine();

        for (Map.Entry<String, String> line : props.entrySet()) {
            w.write(line.getKey());
            w.write('=');
            w.write(line.getValue());
            w.newLine();
        }
    }

    private static void set(Map<String, String> props, String name, Object value) {
        if (value != null) {
            props.put(name, value.toString());
        }
    }

    private static File abs(File file) {
        return file.getAbsoluteFile();
    }

    final Database open(boolean destroy, InputStream restore) throws IOException {
        boolean openedReplicator = false;

        if (mReplConfig != null && mReplManager == null
            && mBaseFile != null && !mBaseFile.isDirectory())
        {
            if (mEventListener != null) {
                mReplConfig.eventListener(new ReplicationEventListener(mEventListener));
            }
            mReplConfig.baseFilePath(mBaseFile.getPath() + ".repl");
            mReplConfig.createFilePath(mMkdirs);
            mReplManager = DatabaseReplicator.open(mReplConfig);
            openedReplicator = true;
        }

        try {
            return doOpen(destroy, restore);
        } catch (Throwable e) {
            if (openedReplicator) {
                try {
                    mReplManager.close();
                } catch (Throwable e2) {
                    suppress(e, e2);
                }
                mReplManager = null;
            }

            throw e;
        }
    }

    private Database doOpen(boolean destroy, InputStream restore) throws IOException {
        if (!destroy && restore == null && mReplManager != null) shouldRestore: {
            // If no data files exist, attempt to restore from a peer.

            File[] dataFiles = dataFiles();
            if (dataFiles == null) {
                // No data files are expected.
                break shouldRestore;
            }

            for (File file : dataFiles) if (file.exists()) {
                // Don't restore if any data files are found to exist.
                break shouldRestore;
            }

            // ReplicationManager returns null if no restore should be performed.
            restore = mReplManager.restoreRequest(mEventListener);
        }

        Method m;
        Object[] args;
        if (restore != null) {
            args = new Object[] {this, restore};
            m = directRestoreMethod();
        } else {
            args = new Object[] {this};
            if (destroy) {
                m = directDestroyMethod();
            } else {
                m = directOpenMethod();
            }
        }

        Throwable e1 = null;
        if (m != null) {
            try {
                return (Database) m.invoke(null, args);
            } catch (Exception e) {
                handleDirectException(e);
                e1 = e;
            }
        }

        try {
            if (restore != null) {
                return LocalDatabase.restoreFromSnapshot(this, restore);
            } else if (destroy) {
                return LocalDatabase.destroy(this);
            } else {
                return LocalDatabase.open(this);
            }
        } catch (Throwable e2) {
            e1 = Utils.rootCause(e1);
            e2 = Utils.rootCause(e2);
            if (e1 == null || (e2 instanceof Error && !(e1 instanceof Error))) {
                // Throw the second, considering it to be more severe.
                Utils.suppress(e2, e1);
                throw Utils.rethrow(e2);
            } else {
                Utils.suppress(e1, e2);
                throw Utils.rethrow(e1);
            }
        }
    }

    private Class<?> directOpenClass() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        try {
            return Class.forName("org.cojen.tupl._LocalDatabase");
        } catch (Exception e) {
            handleDirectException(e);
            return null;
        }
    }

    private Method directOpenMethod() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        Method m = cDirectOpen;
        if (m == null) {
            cDirectOpen = m = findMethod("open", DatabaseConfig.class);
        }
        return m;
    }

    private Method directDestroyMethod() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        Method m = cDirectDestroy;
        if (m == null) {
            cDirectDestroy = m = findMethod("destroy", DatabaseConfig.class);
        }
        return m;
    }

    private Method directRestoreMethod() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        Method m = cDirectRestore;
        if (m == null) {
            cDirectRestore = m = findMethod
                ("restoreFromSnapshot", DatabaseConfig.class, InputStream.class);
        }
        return m;
    }

    private void handleDirectException(Exception e) throws IOException {
        if (e instanceof RuntimeException || e instanceof IOException) {
            throw rethrow(e);
        }
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }
        if (cause instanceof RuntimeException || cause instanceof IOException) {
            throw rethrow(cause);
        }
        if (mDirectPageAccess == Boolean.TRUE) {
            throw new DatabaseException("Unable open with direct page access", cause);
        }
    }

    private Method findMethod(String name, Class<?>... paramTypes) throws IOException {
        Class<?> directClass = directOpenClass();
        if (directClass != null) {
            try {
                return directClass.getDeclaredMethod(name, paramTypes);
            } catch (Exception e) {
                handleDirectException(e);
            }
        }
        return null;
    }
}
