/*
 *  Copyright 2019 Cojen.org
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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.lang.reflect.Method;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.Crypto;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventPrinter;
import org.cojen.tupl.EventType;
import org.cojen.tupl.LockUpgradeRule;

import org.cojen.tupl.ev.ChainedEventListener;
import org.cojen.tupl.ev.ReplicationEventListener;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;
import org.cojen.tupl.ext.ReplicationManager;

import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.repl.DatabaseReplicator;
import org.cojen.tupl.repl.ReplicatorConfig;
import org.cojen.tupl.repl.StreamReplicator;

import static org.cojen.tupl.core.Utils.*;

/**
 * Encapsulates config options and supports opening the database.
 *
 * @author Brian S O'Neill
 */
public final class Launcher implements Cloneable {
    private static volatile Method cDirectOpen;
    private static volatile Method cDirectDestroy;
    private static volatile Method cDirectRestore;

    File mBaseFile;
    boolean mMkdirs;
    File[] mDataFiles;
    boolean mMapDataFiles;
    PageArray mDataPageArray;
    FileFactory mFileFactory;
    long mMinCacheBytes;
    long mMaxCacheBytes;
    long mSecondaryCacheSize;
    DurabilityMode mDurabilityMode;
    LockUpgradeRule mLockUpgradeRule;
    long mLockTimeoutNanos;
    long mCheckpointRateNanos;
    long mCheckpointSizeThreshold;
    long mCheckpointDelayThresholdNanos;
    int mMaxCheckpointThreads;
    EventListener mEventListener;
    boolean mFileSync;
    boolean mReadOnly;
    int mPageSize;
    Boolean mDirectPageAccess;
    boolean mCachePriming;
    ReplicatorConfig mReplConfig;
    ReplicationManager mReplManager;
    int mMaxReplicaThreads;
    Crypto mCrypto;
    Map<String, CustomHandler> mCustomHandlers;
    Map<String, PrepareHandler> mPrepareHandlers;

    // Set only when calling debugOpen, and then it's discarded.
    Map<String, ? extends Object> mDebugOpen;

    // Set only when not replicated and unfinished transactions were recovered.
    LHashTable.Obj mUnfinished;

    // These fields are set as a side-effect of constructing a replicated Database.
    long mReplRecoveryStartNanos;
    long mReplInitialTxnId;

    public Launcher() {
        createFilePath(true);
        durabilityMode(null);
        lockTimeout(1, TimeUnit.SECONDS);
        checkpointRate(1, TimeUnit.SECONDS);
        checkpointSizeThreshold(1024 * 1024);
        checkpointDelayThreshold(1, TimeUnit.MINUTES);
    }

    public void baseFile(File file) {
        mBaseFile = file;
    }

    public void createFilePath(boolean mkdirs) {
        mMkdirs = mkdirs;
    }

    public void dataFiles(File... files) {
        if (files == null || files.length == 0) {
            mDataFiles = null;
        } else {
            mDataFiles = files;
            mDataPageArray = null;
        }
    }

    public void mapDataFiles(boolean mapped) {
        mMapDataFiles = mapped;
    }

    public void dataPageArray(PageArray array) {
        mDataPageArray = array;
        if (array != null) {
            int expected = mDataPageArray.pageSize();
            if (mPageSize != 0 && mPageSize != expected) {
                throw new IllegalStateException
                    ("Page size doesn't match data page array: " + mPageSize + " != " + expected);
            }
            mDataFiles = null;
            mPageSize = expected;
        }
    }

    public void fileFactory(FileFactory factory) {
        mFileFactory = factory;
    }

    public void minCacheSize(long minBytes) {
        mMinCacheBytes = minBytes;
    }

    public void maxCacheSize(long maxBytes) {
        mMaxCacheBytes = maxBytes;
    }

    public void secondaryCacheSize(long size) {
        if (size < 0) {
            // Reserve use of negative size.
            throw new IllegalArgumentException();
        }
        mSecondaryCacheSize = size;
    }

    public void durabilityMode(DurabilityMode durabilityMode) {
        if (durabilityMode == null) {
            durabilityMode = DurabilityMode.SYNC;
        }
        mDurabilityMode = durabilityMode;
    }

    public void lockUpgradeRule(LockUpgradeRule lockUpgradeRule) {
        if (lockUpgradeRule == null) {
            lockUpgradeRule = LockUpgradeRule.STRICT;
        }
        mLockUpgradeRule = lockUpgradeRule;
    }

    public void lockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = toNanos(timeout, unit);
    }

    public void checkpointRate(long rate, TimeUnit unit) {
        mCheckpointRateNanos = toNanos(rate, unit);
    }

    public void checkpointSizeThreshold(long bytes) {
        mCheckpointSizeThreshold = bytes;
    }

    public void checkpointDelayThreshold(long delay, TimeUnit unit) {
        mCheckpointDelayThresholdNanos = toNanos(delay, unit);
    }

    public void maxCheckpointThreads(int num) {
        mMaxCheckpointThreads = num;
    }

    public void eventListener(EventListener listener) {
        mEventListener = listener;
    }

    public void eventListeners(EventListener... listeners) {
        mEventListener = ChainedEventListener.make(listeners);
    }

    public void syncWrites(boolean fileSync) {
        mFileSync = fileSync;
    }

    /*
    public void readOnly(boolean readOnly) {
        mReadOnly = readOnly;
    }
    */

    public void pageSize(int size) {
        if (mDataPageArray != null) {
            int expected = mDataPageArray.pageSize();
            if (expected != size) {
                throw new IllegalStateException
                    ("Page size doesn't match data page array: " + size + " != " + expected);
            }
        }
        mPageSize = size;
    }

    public void directPageAccess(boolean direct) {
        mDirectPageAccess = direct;
    }

    public void cachePriming(boolean priming) {
        mCachePriming = priming;
    }

    public void replicate(ReplicatorConfig config) {
        mReplConfig = config;
        mReplManager = null;
    }

    public void replicate(StreamReplicator repl) {
        mReplManager = repl == null ? null : new DatabaseReplicator(repl);
        mReplConfig = null;
    }

    public void maxReplicaThreads(int num) {
        mMaxReplicaThreads = num;
    }

    public void encrypt(Crypto crypto) {
        mCrypto = crypto;
    }

    public void customHandlers(Map<String, ? extends CustomHandler> handlers) {
        mCustomHandlers = mapClone(handlers);
    }

    public void prepareHandlers(Map<String, ? extends PrepareHandler> handlers) {
        mPrepareHandlers = mapClone(handlers);
    }

    /**
     * @return null if map is null or empty
     */
    static <H> Map<String, H> mapClone(Map<String, ? extends H> map) {
        return map == null || map.isEmpty() ? null : new HashMap<>(map);
    }

    static <H> LHashTable.Obj<H> newByIdMap(Map<String, ? extends H> map) {
        return map == null || map.isEmpty() ? null : new LHashTable.Obj<>(map.size());
    }

    public void debugOpen(PrintStream out, Map<String, ? extends Object> properties)
        throws IOException
    {
        if (out == null) {
            out = System.out;
        }

        if (properties == null) {
            properties = Collections.emptyMap();
        }

        Launcher launcher = clone();

        launcher.eventListener(new EventPrinter(out));
        launcher.mReadOnly = true;
        launcher.mDebugOpen = properties;

        if (launcher.mDirectPageAccess == null) {
            launcher.directPageAccess(false);
        }

        launcher.open(false, null).close();
    }

    @Override
    public Launcher clone() {
        try {
            return (Launcher) super.clone();
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

        return new StripedPageCache(size, mPageSize);
    }

    /**
     * Performs configuration check and returns the applicable data files. Null is returned
     * when base file is null or if a custom PageArray should be used.
     */
    File[] dataFiles() {
        if (mReplManager != null) {
            long encoding = mReplManager.encoding();
            if (encoding == 0) {
                throw new IllegalStateException
                    ("Illegal replication manager encoding: " + encoding);
            }
        }

        File[] dataFiles = mDataFiles;
        if (mBaseFile == null) {
            if (dataFiles != null && dataFiles.length > 0) {
                throw new IllegalStateException
                    ("Cannot specify data files when no base file is provided");
            }
            return null;
        }

        if (mBaseFile.isDirectory()) {
            throw new IllegalStateException("Base file is a directory: " + mBaseFile);
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
                throw new IllegalStateException("Data file is a directory: " + dataFile);
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

    /**
     * @return true if mReplManager was assigned a new replicator
     */
    private boolean openReplicator() throws IOException {
        if (mReplManager != null || mReplConfig == null
            || mBaseFile == null || mBaseFile.isDirectory())
        {
            return false;
        }

        ReplicatorConfig replConfig = mReplConfig.clone();

        if (mEventListener != null) {
            replConfig.eventListener(new ReplicationEventListener(mEventListener));
        }

        replConfig.baseFilePath(mBaseFile.getPath() + ".repl");
        replConfig.createFilePath(mMkdirs);

        mReplManager = DatabaseReplicator.open(replConfig);

        return true;
    }

    public final Database open(boolean destroy, InputStream restore) throws IOException {
        Launcher launcher = clone();
        boolean openedReplicator = launcher.openReplicator();
        try {
            return launcher.doOpen(destroy, restore);
        } catch (Throwable e) {
            if (openedReplicator) {
                try {
                    launcher.mReplManager.close();
                } catch (Throwable e2) {
                    suppress(e, e2);
                }
            }
            throw e;
        }
    }

    private Database doOpen(boolean destroy, InputStream restore) throws IOException {
        if (!destroy && restore == null && mReplManager != null) shouldRestore: {
            // If no data files exist, attempt to restore from a peer.

            File[] dataFiles = dataFiles();
            if (dataFiles == null) {
                if (mDataPageArray == null || !mDataPageArray.isEmpty()) {
                    // No data files are expected.
                    break shouldRestore;
                }
            } else {
                for (File file : dataFiles) {
                    if (file.exists()) {
                        // Don't restore if any data files are found to exist.
                        break shouldRestore;
                    }
                }
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
            e1 = rootCause(e1);
            e2 = rootCause(e2);
            if (e1 == null || (e2 instanceof Error && !(e1 instanceof Error))) {
                // Throw the second, considering it to be more severe.
                suppress(e2, e1);
                throw rethrow(e2);
            } else {
                suppress(e1, e2);
                throw rethrow(e1);
            }
        }
    }

    private Class<?> directOpenClass() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        String name = getClass().getName();
        name = name.substring(0, name.lastIndexOf('.') + 1) + "_LocalDatabase";
        try {
            return Class.forName(name);
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
            cDirectOpen = m = findMethod("open", Launcher.class);
        }
        return m;
    }

    private Method directDestroyMethod() throws IOException {
        if (mDirectPageAccess == Boolean.FALSE) {
            return null;
        }
        Method m = cDirectDestroy;
        if (m == null) {
            cDirectDestroy = m = findMethod("destroy", Launcher.class);
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
                ("restoreFromSnapshot", Launcher.class, InputStream.class);
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

