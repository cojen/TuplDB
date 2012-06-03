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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.Node.*;
import static org.cojen.tupl.Utils.*;

/**
 * Main database class, containing a collection of transactional indexes. Call
 * {@link #open open} to obtain a Database instance.
 *
 * @author Brian S O'Neill
 */
public final class Database implements Closeable {
    private static final int DEFAULT_CACHED_NODES = 1000;
    // +2 for registry and key map root nodes, +1 for one user index, and +2
    // for usage list to function correctly. It always assumes that the least
    // recently used node points to a valid, more recently used node.
    private static final int MIN_CACHED_NODES = 5;

    // Approximate byte overhead per node. Influenced by many factors,
    // including pointer size and child node references. This estimate assumes
    // 32-bit pointers.
    private static final int NODE_OVERHEAD = 100;

    private static int nodeCountFromBytes(long bytes, int pageSize) {
         pageSize += NODE_OVERHEAD;
         long count = (bytes + pageSize - 1) / pageSize;
         return count <= Integer.MAX_VALUE ? (int) count : Integer.MAX_VALUE;
    }

    private static long byteCountFromNodes(int nodes, int pageSize) {
        return nodes * (long) (pageSize + NODE_OVERHEAD);
    }

    private static final int ENCODING_VERSION = 20120525;

    private static final int I_ENCODING_VERSION        = 0;
    private static final int I_ROOT_PAGE_ID            = I_ENCODING_VERSION + 4;
    private static final int I_MASTER_UNDO_LOG_PAGE_ID = I_ROOT_PAGE_ID + 8;
    private static final int I_TRANSACTION_ID          = I_MASTER_UNDO_LOG_PAGE_ID + 8;
    private static final int I_REDO_LOG_ID             = I_TRANSACTION_ID + 8;
    private static final int HEADER_SIZE               = I_REDO_LOG_ID + 8;

    static final byte KEY_TYPE_INDEX_NAME = 0;
    static final byte KEY_TYPE_INDEX_ID = 1;

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private final LockedFile mLockFile;

    final DurabilityMode mDurabilityMode;
    final long mDefaultLockTimeoutNanos;
    final LockManager mLockManager;
    final RedoLog mRedoLog;
    final PageDb mPageDb;

    private final BufferPool mSpareBufferPool;

    private final Latch mUsageLatch;
    private int mMaxNodeCount;
    private int mNodeCount;
    private Node mMostRecentlyUsed;
    private Node mLeastRecentlyUsed;

    private final Lock mSharedCommitLock;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    // The root tree, which maps tree ids to other tree root node ids.
    private final Tree mRegistry;
    // Maps tree name keys to ids.
    private final Tree mRegistryKeyMap;
    // Maps tree names to open trees.
    private final Map<byte[], Tree> mOpenTrees;
    private final LHashTable.Obj<Tree> mOpenTreesById;

    private final OrderedPageAllocator mAllocator;

    private final FragmentCache mFragmentCache;
    final int mMaxFragmentedEntrySize;

    // Fragmented values which are transactionally deleted go here.
    private volatile FragmentedTrash mFragmentedTrash;

    private final Object mTxnIdLock = new Object();
    // The following fields are guarded by mTxnIdLock.
    private long mTxnId;
    private UndoLog mTopUndoLog;
    private int mUndoLogCount;

    private final Object mCheckpointLock = new Object();

    private volatile Checkpointer mCheckpointer;

    private final TempFileManager mTempFileManager;

    volatile boolean mClosed;

    /**
     * Open a database, creating it if necessary.
     */
    public static Database open(DatabaseConfig config) throws IOException {
        config = config.clone();
        Database db = new Database(config, false);
        db.startCheckpointer(config);
        return db;
    }

    /**
     * Delete the contents of an existing database, and replace it with an
     * empty one. When using a raw block device for the data file, this method
     * must be used to format it.
     */
    public static Database destroy(DatabaseConfig config) throws IOException {
        config = config.clone();
        if (config.mReadOnly) {
            throw new IllegalArgumentException("Cannot destroy read-only database");
        }
        Database db = new Database(config, true);
        db.startCheckpointer(config);
        return db;
    }

    /**
     * @param config cloned config
     */
    private Database(DatabaseConfig config, boolean destroy) throws IOException {
        File baseFile = config.mBaseFile;
        File[] dataFiles = config.dataFiles();

        int pageSize = config.mPageSize;
        if (pageSize <= 0) {
            config.pageSize(pageSize = DEFAULT_PAGE_SIZE);
        }

        int minCache, maxCache;
        cacheSize: {
            long minCachedBytes = config.mMinCachedBytes;
            long maxCachedBytes = config.mMaxCachedBytes;

            if (maxCachedBytes == 0) {
                maxCachedBytes = minCachedBytes;
                if (maxCachedBytes == 0) {
                    minCache = maxCache = DEFAULT_CACHED_NODES;
                    break cacheSize;
                }
            }

            if (minCachedBytes > maxCachedBytes) {
                throw new IllegalArgumentException
                    ("Minimum cache size exceeds maximum: " +
                     minCachedBytes + " > " + maxCachedBytes);
            }

            minCache = nodeCountFromBytes(minCachedBytes, pageSize);
            maxCache = nodeCountFromBytes(maxCachedBytes, pageSize);

            minCache = Math.max(MIN_CACHED_NODES, minCache);
            maxCache = Math.max(MIN_CACHED_NODES, maxCache);
        }

        // Update config such that info file is correct.
        config.mMinCachedBytes = byteCountFromNodes(minCache, pageSize);
        config.mMaxCachedBytes = byteCountFromNodes(maxCache, pageSize);

        mUsageLatch = new Latch();
        mMaxNodeCount = maxCache;

        mDurabilityMode = config.mDurabilityMode;
        mDefaultLockTimeoutNanos = config.mLockTimeoutNanos;
        mLockManager = new LockManager(mDefaultLockTimeoutNanos);

        if (baseFile != null && !config.mReadOnly && config.mMkdirs) {
            baseFile.getParentFile().mkdirs();
            for (File f : dataFiles) {
                f.getParentFile().mkdirs();
            }
        }

        // Create lock file and write info file of properties.
        if (baseFile == null) {
            mLockFile = null;
        } else {
            mLockFile = new LockedFile(new File(baseFile.getPath() + ".lock"), config.mReadOnly);
            if (!config.mReadOnly) {
                File infoFile = new File(baseFile.getPath() + ".info");
                Writer w = new BufferedWriter
                    (new OutputStreamWriter(new FileOutputStream(infoFile), "UTF-8"));
                try {
                    config.writeInfo(w);
                } finally {
                    w.close();
                }
            }
        }

        EnumSet<OpenOption> options = config.createOpenOptions();
        if (baseFile != null && destroy) {
            // Delete old redo log files.
            Utils.deleteNumberedFiles(baseFile, ".redo.");
        }

        if (dataFiles == null) {
            mPageDb = new NonPageDb(pageSize);
        } else {
            mPageDb = new DurablePageDb(pageSize, dataFiles, options, destroy);
        }

        mSharedCommitLock = mPageDb.sharedCommitLock();

        try {
            // Pre-allocate nodes. They are automatically added to the usage
            // list, and so nothing special needs to be done to allow them to
            // get used. Since the initial state is clean, evicting these
            // nodes does nothing.
            try {
                for (int i=minCache; --i>=0; ) {
                    allocLatchedNode(true).releaseExclusive();
                }
            } catch (OutOfMemoryError e) {
                mMostRecentlyUsed = null;
                mLeastRecentlyUsed = null;

                throw new OutOfMemoryError
                    ("Unable to allocate the minimum required number of cached nodes: " +
                     minCache);
            }

            int spareBufferCount = Runtime.getRuntime().availableProcessors();
            mSpareBufferPool = new BufferPool(mPageDb.pageSize(), spareBufferCount);

            mSharedCommitLock.lock();
            try {
                mCommitState = CACHED_DIRTY_0;
            } finally {
                mSharedCommitLock.unlock();
            }

            byte[] header = new byte[HEADER_SIZE];
            mPageDb.readExtraCommitData(header);

            mRegistry = new Tree(this, Tree.REGISTRY_ID, null, null, loadRegistryRoot(header));
            mOpenTrees = new TreeMap<byte[], Tree>(KeyComparator.THE);
            mOpenTreesById = new LHashTable.Obj<Tree>(16);

            synchronized (mTxnIdLock) {
                mTxnId = readLong(header, I_TRANSACTION_ID);
            }
            long redoLogId = readLong(header, I_REDO_LOG_ID);

            // Initialized, but not open yet.
            mRedoLog = baseFile == null ? null : new RedoLog(baseFile, redoLogId);

            mRegistryKeyMap = openInternalTree(Tree.REGISTRY_KEY_MAP_ID, true);

            mAllocator = new OrderedPageAllocator
                (mPageDb, null /*openInternalTree(Tree.PAGE_ALLOCATOR_ID, true)*/);

            mFragmentCache = new FragmentCache(this, mMaxNodeCount);

            {
                Tree tree = openInternalTree(Tree.FRAGMENTED_TRASH_ID, false);
                if (tree != null) {
                    mFragmentedTrash = new FragmentedTrash(tree);
                }
            }

            // Limit maximum fragmented entry size to guarantee that 2 entries
            // fit. Each also requires 2 bytes for pointer and up to 3 bytes
            // for value length field.
            mMaxFragmentedEntrySize = (pageSize - Node.TN_HEADER_SIZE - (2 + 3 + 2 + 3)) >> 1;

            if (mRedoLog != null) {
                // Perform recovery by examining redo and undo logs.

                UndoLog masterUndoLog;
                LHashTable.Obj<UndoLog> undoLogs;
                {
                    long nodeId = readLong(header, I_MASTER_UNDO_LOG_PAGE_ID);
                    if (nodeId == 0) {
                        masterUndoLog = null;
                        undoLogs = null;
                    } else {
                        masterUndoLog = UndoLog.recoverMasterUndoLog(this, nodeId);
                        undoLogs = masterUndoLog.recoverLogs();
                    }
                }

                // TODO: Recovery should only need to perform at most one checkpoint.
                // The current approach is fine, but it's just a bit messy.

                if (redoReplay(undoLogs)) {
                    // Make sure old redo log is deleted. Process might have exited
                    // before last checkpoint could delete it.
                    mRedoLog.deleteOldFile(redoLogId - 1);

                    // Checkpoint now to ensure all old redo log entries are durable.
                    checkpoint(true);

                    while (mRedoLog.isReplayMode()) {
                        // Last checkpoint was interrupted, so apply next log file too.
                        redoReplay(undoLogs);
                        checkpoint(true);
                    }
                }

                if (masterUndoLog != null) {
                    // Rollback or truncate all remaining undo logs. They were
                    // never explicitly rolled back, or they were committed but
                    // not cleaned up. This also deletes the master undo log.
                    if (masterUndoLog.processRemaining(undoLogs)) {
                        // Checkpoint again to ensure that undo logs don't get
                        // re-applied following a restart.
                        checkpoint(true);
                    }
                }
            }

            // Delete lingering fragmented values after undo logs have been
            // processed, ensuring deletes were committed.
            if (mFragmentedTrash != null && mFragmentedTrash.emptyAllTrash()) {
                checkpoint(true);
            }

            mTempFileManager = baseFile == null ? null : new TempFileManager(baseFile);
        } catch (Throwable e) {
            Utils.closeQuietly(null, this);
            throw Utils.rethrow(e);
        }
    }

    private void startCheckpointer(DatabaseConfig config) {
        if (mRedoLog == null && mTempFileManager == null) {
            // Nothing is durable and nothing to ever clean up 
            return;
        }

        mCheckpointer = new Checkpointer(this, config.mCheckpointRateNanos);

        // Register objects to automatically shutdown.
        mCheckpointer.register(mRedoLog);
        mCheckpointer.register(mTempFileManager);

        mCheckpointer.start();
    }

    // TODO: remove test method
    void trace() throws IOException {
        java.util.BitSet pages = mPageDb.tracePages();
        mRegistry.mRoot.tracePages(this, pages);
        mRegistryKeyMap.mRoot.tracePages(this, pages);
        synchronized (mOpenTrees) {
            for (Tree tree : mOpenTrees.values()) {
                tree.mRoot.tracePages(this, pages);
            }
        }
        System.out.println(pages);
        System.out.println("lost: " + pages.cardinality());
        System.out.println(mPageDb.stats());
    }

    private boolean redoReplay(LHashTable.Obj<UndoLog> undoLogs) throws IOException {
        RedoLogTxnScanner scanner = new RedoLogTxnScanner();
        if (!mRedoLog.replay(scanner) ||
            !mRedoLog.replay(new RedoLogApplier(this, scanner, undoLogs)))
        {
            return false;
        }

        long redoTxnId = scanner.highestTxnId();
        if (redoTxnId != 0) synchronized (mTxnIdLock) {
            // Subtract for modulo comparison.
            if (mTxnId == 0 || (redoTxnId - mTxnId) > 0) {
                mTxnId = redoTxnId;
            }
        }

        return true;
    }

    /**
     * Returns the given named index, returning null if not found.
     *
     * @return shared Index instance; null if not found
     */
    public Index findIndex(byte[] name) throws IOException {
        return openIndex(name.clone(), false);
    }

    /**
     * Returns the given named index, returning null if not found. Name is UTF-8
     * encoded.
     *
     * @return shared Index instance; null if not found
     */
    public Index findIndex(String name) throws IOException {
        return openIndex(name.getBytes("UTF-8"), false);
    }

    /**
     * Returns the given named index, creating it if necessary.
     *
     * @return shared Index instance
     */
    public Index openIndex(byte[] name) throws IOException {
        return openIndex(name.clone(), true);
    }

    /**
     * Returns the given named index, creating it if necessary. Name is UTF-8
     * encoded.
     *
     * @return shared Index instance
     */
    public Index openIndex(String name) throws IOException {
        return openIndex(name.getBytes("UTF-8"), true);
    }

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @throws IllegalArgumentException if id is reserved
     */
    public Index indexById(long id) throws IOException {
        if (Tree.isInternal(id)) {
            throw new IllegalArgumentException("Invalid id: " + id);
        }

        Index index;

        final Lock commitLock = sharedCommitLock();
        commitLock.lock();
        try {
            synchronized (mOpenTrees) {
                LHashTable.ObjEntry<Tree> entry = mOpenTreesById.get(id);
                if (entry != null) {
                    return entry.value;
                }
            }

            byte[] idKey = new byte[9];
            idKey[0] = KEY_TYPE_INDEX_ID;
            writeLong(idKey, 1, id);

            byte[] name = mRegistryKeyMap.load(null, idKey);

            if (name == null) {
                return null;
            }

            index = openIndex(name, false);
        } catch (Throwable e) {
            throw Utils.closeOnFailure(this, e);
        } finally {
            commitLock.unlock();
        }

        if (index == null) {
            // Registry needs to be repaired to fix this.
            throw new DatabaseException("Unable to find index in registry");
        }

        return index;
    }

    /**
     * Returns an index by its identifier, returning null if not found.
     *
     * @param id big-endian encoded long integer
     * @throws IllegalArgumentException if id is malformed or reserved
     */
    public Index indexById(byte[] id) throws IOException {
        if (id.length != 8) {
            throw new IllegalArgumentException("Expected 8 byte identifier: " + id.length);
        }
        return indexById(readLong(id, 0));
    }

    /**
     * Allows access to internal indexes which can use the redo log.
     */
    Index anyIndexById(long id) throws IOException {
        if (id == Tree.REGISTRY_KEY_MAP_ID) {
            return mRegistryKeyMap;
        } else if (id == Tree.FRAGMENTED_TRASH_ID) {
            return fragmentedTrash().mTrash;
        }
        return indexById(id);
    }

    /**
     * Returns a Cursor which maps all available index names to
     * identifiers. Identifiers are long integers, big-endian encoded.
     * Attempting to store anything into the Cursor causes an {@link
     * UnmodifiableViewException} to be thrown.
     */
    public Cursor allIndexes() throws IOException {
        return new IndexesCursor(mRegistryKeyMap.newCursor(null));
    }

    /**
     * Returns a new Transaction with the {@link DatabaseConfig#durabilityMode default}
     * durability mode.
     */
    public Transaction newTransaction() {
        return doNewTransaction(mDurabilityMode);
    }

    /**
     * Returns a new Transaction with the given durability mode. If null, the
     * {@link DatabaseConfig#durabilityMode default} is used.
     */
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return doNewTransaction(durabilityMode == null ? mDurabilityMode : durabilityMode);
    }

    private Transaction doNewTransaction(DurabilityMode durabilityMode) {
        return new Transaction
            (this, durabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);
    }

    /**
     * Convenience method which returns a transaction intended for locking, and
     * not for making modifications.
     */
    Transaction newLockTransaction() {
        return new Transaction(this, DurabilityMode.NO_LOG, LockMode.UPGRADABLE_READ, -1);
    }

    /**
     * Caller must hold commit lock. This ensures that highest transaction id
     * is persisted correctly by checkpoint.
     *
     * @param txnId pass zero to select a transaction id
     * @return non-zero transaction id
     */
    long register(UndoLog undo, long txnId) {
        synchronized (mTxnIdLock) {
            UndoLog top = mTopUndoLog;
            if (top != null) {
                undo.mPrev = top;
                top.mNext = undo;
            }
            mTopUndoLog = undo;
            mUndoLogCount++;

            while (txnId == 0) {
                txnId = ++mTxnId;
            }
            return txnId;
        }
    }

    /**
     * Caller must hold commit lock. This ensures that highest transaction id
     * is persisted correctly by checkpoint.
     *
     * @return non-zero transaction id
     */
    long nextTransactionId() {
        long txnId;
        do {
            synchronized (mTxnIdLock) {
                txnId = ++mTxnId;
            }
        } while (txnId == 0);
        return txnId;
    }

    /**
     * Called only by UndoLog.
     */
    void unregister(UndoLog log) {
        synchronized (mTxnIdLock) {
            UndoLog prev = log.mPrev;
            UndoLog next = log.mNext;
            if (prev != null) {
                prev.mNext = next;
                log.mPrev = null;
            }
            if (next != null) {
                next.mPrev = prev;
                log.mNext = null;
            } else if (log == mTopUndoLog) {
                mTopUndoLog = prev;
            }
            mUndoLogCount--;
        }
    }

    /**
     * Preallocates pages for immediate use.
     */
    public void preallocate(long bytes) throws IOException {
        int pageSize = pageSize();
        long pageCount = (bytes + pageSize - 1) / pageSize;
        if (pageCount > 0) {
            mPageDb.allocatePages(pageCount);
            checkpoint(true);
        }
    }

    /**
     * Support for capturing a snapshot (hot backup) of the database, while
     * still allowing concurrent modifications. The snapshot contains all data
     * up to the last checkpoint. Call {@link #restoreFromSnapshot restoreFromSnapshot}
     * to recreate a Database from the snapshot.
     *
     * @param out snapshot destination; does not require extra buffering
     * @return a snapshot control object, which must be closed when no longer needed
     */
    public Snapshot beginSnapshot(OutputStream out) throws IOException {
        if (!(mPageDb instanceof DurablePageDb)) {
            throw new UnsupportedOperationException("Snapshot only allowed for durable databases");
        }
        DurablePageDb pageDb = (DurablePageDb) mPageDb;
        int cluster = Math.max(1, 65536 / pageSize());
        return pageDb.beginSnapshot(mTempFileManager, cluster, out);
    }

    /**
     * @param in snapshot source; does not require extra buffering; not auto-closed
     */
    public static Database restoreFromSnapshot(DatabaseConfig config, InputStream in)
        throws IOException
    {
        File[] dataFiles = config.dataFiles();
        if (dataFiles == null) {
            throw new UnsupportedOperationException("Restore only allowed for durable databases");
        }
        if (!config.mReadOnly && config.mMkdirs) {
            for (File f : dataFiles) {
                f.getParentFile().mkdirs();
            }
        }
        EnumSet<OpenOption> options = config.createOpenOptions();
        // Delete old redo log files.
        Utils.deleteNumberedFiles(config.mBaseFile, ".redo.");
        DurablePageDb.restoreFromSnapshot(dataFiles, options, in).close();
        return Database.open(config);
    }

    /**
     * Flushes, but does not sync, all non-flushed transactions. Transactions
     * committed with {@link DurabilityMode#NO_FLUSH no-flush} effectively
     * become {@link DurabilityMode#NO_SYNC no-sync} durable.
     */
    public void flush() throws IOException {
        if (!mClosed && mRedoLog != null) {
            mRedoLog.flush();
        }
    }

    /**
     * Persists all non-flushed and non-sync'd transactions. Transactions
     * committed with {@link DurabilityMode#NO_FLUSH no-flush} and {@link
     * DurabilityMode#NO_SYNC no-sync} effectively become {@link
     * DurabilityMode#SYNC sync} durable.
     */
    public void sync() throws IOException {
        if (!mClosed && mRedoLog != null) {
            mRedoLog.sync();
        }
    }

    /**
     * Durably sync and checkpoint all changes to the database. In addition to
     * ensuring that all transactions are durable, checkpointing ensures that
     * non-transactional modifications are durable.
     */
    public void checkpoint() throws IOException {
        if (!mClosed && mRedoLog != null) {
            checkpoint(false);
        }
    }

    /**
     * Closes the database, ensuring durability of committed transactions. No
     * checkpoint is performed by this method, and so non-transactional
     * modifications can be lost.
     */
    @Override
    public void close() throws IOException {
        mClosed = true;

        Checkpointer c = mCheckpointer;
        if (c != null) {
            c.close();
            c = null;
        }

        if (mOpenTrees != null) {
            synchronized (mOpenTrees) {
                mOpenTrees.clear();
                mOpenTreesById.clear(0);
            }
        }

        mSharedCommitLock.lock();
        try {
            closeNodeCache();

            if (mAllocator != null) {
                mAllocator.clearDirtyNodes();
            }

            IOException ex = null;

            ex = Utils.closeQuietly(ex, mRedoLog);
            ex = Utils.closeQuietly(ex, mPageDb);
            ex = Utils.closeQuietly(ex, mLockFile);

            if (ex != null) {
                throw ex;
            }
        } finally {
            mSharedCommitLock.unlock();
        }
    }

    void checkClosed() throws DatabaseException {
        if (mClosed) {
            throw new DatabaseException("Closed");
        }
    }

    /**
     * @param rootId pass zero to create
     * @return unlatched and unevictable root node
     */
    private Node loadTreeRoot(long rootId) throws IOException {
        Node rootNode = allocLatchedNode(false);
        try {
            if (rootId == 0) {
                rootNode.asEmptyRoot();
            } else {
                try {
                    rootNode.read(this, rootId);
                } catch (IOException e) {
                    makeEvictable(rootNode);
                    throw e;
                }
            }
        } finally {
            rootNode.releaseExclusive();
        }
        return rootNode;
    }

    /**
     * Loads the root registry node, or creates one if store is new. Root node
     * is not eligible for eviction.
     */
    private Node loadRegistryRoot(byte[] header) throws IOException {
        int version = readInt(header, I_ENCODING_VERSION);

        long rootId;
        if (version == 0) {
            rootId = 0;
        } else {
            if (version != ENCODING_VERSION) {
                throw new CorruptDatabaseException("Unknown encoding version: " + version);
            }
            rootId = readLong(header, I_ROOT_PAGE_ID);
        }

        return loadTreeRoot(rootId);
    }

    private Tree openInternalTree(long treeId, boolean create) throws IOException {
        final Lock commitLock = sharedCommitLock();
        commitLock.lock();
        try {
            byte[] treeIdBytes = new byte[8];
            writeLong(treeIdBytes, 0, treeId);
            byte[] rootIdBytes = mRegistry.load(Transaction.BOGUS, treeIdBytes);
            long rootId;
            if (rootIdBytes != null) {
                rootId = readLong(rootIdBytes, 0);
            } else {
                if (!create) {
                    return null;
                }
                rootId = 0;
            }
            return new Tree(this, treeId, treeIdBytes, null, loadTreeRoot(rootId));
        } finally {
            commitLock.unlock();
        }
    }

    private Index openIndex(byte[] name, boolean create) throws IOException {
        checkClosed();

        final Lock commitLock = sharedCommitLock();
        commitLock.lock();
        try {
            synchronized (mOpenTrees) {
                Tree tree = mOpenTrees.get(name);
                if (tree != null) {
                    return tree;
                }
            }

            byte[] nameKey = newKey(KEY_TYPE_INDEX_NAME, name);
            byte[] treeIdBytes = mRegistryKeyMap.load(null, nameKey);
            long treeId;

            if (treeIdBytes != null) {
                treeId = readLong(treeIdBytes, 0);
            } else if (!create) {
                return null;
            } else synchronized (mOpenTrees) {
                treeIdBytes = mRegistryKeyMap.load(null, nameKey);
                if (treeIdBytes != null) {
                    treeId = readLong(treeIdBytes, 0);
                } else {
                    treeIdBytes = new byte[8];

                    try {
                        do {
                            treeId = Tree.randomId();
                            writeLong(treeIdBytes, 0, treeId);
                        } while (!mRegistry.insert(Transaction.BOGUS, treeIdBytes,
                                                   Utils.EMPTY_BYTES));

                        if (!mRegistryKeyMap.insert(null, nameKey, treeIdBytes)) {
                            mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                            throw new DatabaseException("Unable to insert index name");
                        }

                        byte[] idKey = newKey(KEY_TYPE_INDEX_ID, treeIdBytes);

                        if (!mRegistryKeyMap.insert(null, idKey, name)) {
                            mRegistryKeyMap.delete(null, nameKey);
                            mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                            throw new DatabaseException("Unable to insert index id");
                        }
                    } catch (IOException e) {
                        throw Utils.closeOnFailure(this, e);
                    }
                }
            }

            // Use a transaction to ensure that only one thread loads the
            // requested index. Nothing is written into it.
            Transaction txn = newLockTransaction();
            try {
                // Pass the transaction to acquire the lock.
                byte[] rootIdBytes = mRegistry.load(txn, treeIdBytes);

                synchronized (mOpenTrees) {
                    Tree tree = mOpenTrees.get(name);
                    if (tree != null) {
                        // Another thread got the lock first and loaded the index.
                        return tree;
                    }
                }

                long rootId = (rootIdBytes == null || rootIdBytes.length == 0) ? 0
                    : readLong(rootIdBytes, 0);
                Tree tree = new Tree(this, treeId, treeIdBytes, name, loadTreeRoot(rootId));

                synchronized (mOpenTrees) {
                    mOpenTrees.put(name, tree);
                    mOpenTreesById.insert(treeId).value = tree;
                }

                return tree;
            } finally {
                txn.reset();
            }
        } finally {
            commitLock.unlock();
        }
    }

    private static byte[] newKey(byte type, byte[] payload) {
        byte[] key = new byte[1 + payload.length];
        key[0] = type;
        System.arraycopy(payload, 0, key, 1, payload.length);
        return key;
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageDb.pageSize();
    }

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    Lock sharedCommitLock() {
        return mSharedCommitLock;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an id
     * of zero and a clean state.
     */
    Node allocLatchedNode() throws IOException {
        return allocLatchedNode(true);
    }

    /**
     * @param evictable true if allocated node can be automatically evicted
     */
    private Node allocLatchedNode(boolean evictable) throws IOException {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        alloc: try {
            int max = mMaxNodeCount;

            if (max == 0) {
                break alloc;
            }

            if (mNodeCount < max) {
                checkClosed();
                Node node = new Node(pageSize());
                node.acquireExclusive();
                mNodeCount++;
                if (evictable) {
                    if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                        mLeastRecentlyUsed = node;
                    } else {
                        mMostRecentlyUsed.mMoreUsed = node;
                    }
                    mMostRecentlyUsed = node;
                }
                return node;
            }

            if (!evictable && mLeastRecentlyUsed.mMoreUsed == mMostRecentlyUsed) {
                // Cannot allow list to shrink to less than two elements.
                break alloc;
            }

            do {
                Node node = mLeastRecentlyUsed;
                (mLeastRecentlyUsed = node.mMoreUsed).mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;

                if (node.tryAcquireExclusive() && (node = Node.evict(node, this)) != null) {
                    if (!evictable) {
                        // Detach from linked list.
                        (mMostRecentlyUsed = node.mLessUsed).mMoreUsed = null;
                        node.mLessUsed = null;
                    }
                    // Return with latch still held.
                    return node;
                }
            } while (--max > 0);
        } finally {
            usageLatch.releaseExclusive();
        }

        checkClosed();

        // FIXME: Throw a better exception. Also, try all nodes again, but with
        // stronger latch request before giving up.
        throw new DatabaseException("Cache is full");
    }

    /**
     * Unlinks all nodes from each other in usage list, and prevents new nodes
     * from being allocated.
     */
    private void closeNodeCache() {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            // Prevent new allocations.
            mMaxNodeCount = 0;

            Node node = mLeastRecentlyUsed;
            mLeastRecentlyUsed = null;
            mMostRecentlyUsed = null;

            while (node != null) {
                Node next = node.mMoreUsed;
                node.mLessUsed = null;
                node.mMoreUsed = null;

                // Make node appear to be evicted.
                node.mId = 0;

                // Attempt to unlink child nodes, making them appear to be evicted.
                if (node.tryAcquireExclusive()) {
                    Node[] childNodes = node.mChildNodes;
                    if (childNodes != null) {
                        Arrays.fill(childNodes, null);
                    }
                    node.releaseExclusive();
                }

                node = next;
            }
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     *
     * @param forTree tree which is allocating the node
     */
    Node allocDirtyNode(Tree forTree) throws IOException {
        Node node = allocLatchedNode(true);
        try {
            dirty(node, mAllocator.allocPage(forTree, node));
            return node;
        } catch (IOException e) {
            node.releaseExclusive();
            throw e;
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, marked
     * dirty and unevictable. Caller must hold commit lock.
     *
     * @param forTree tree which is allocating the node
     */
    Node allocUnevictableNode(Tree forTree) throws IOException {
        Node node = allocLatchedNode(false);
        try {
            dirty(node, mAllocator.allocPage(forTree, node));
            return node;
        } catch (IOException e) {
            makeEvictable(node);
            node.releaseExclusive();
            throw e;
        }
    }

    /**
     * Allow a Node which was allocated as unevictable to be evictable,
     * starting off as the most recently used.
     */
    void makeEvictable(Node node) {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            if (mMaxNodeCount == 0) {
                // Closed.
                return;
            }
            if (node.mMoreUsed != null || node.mLessUsed != null) {
                throw new IllegalStateException();
            }
            (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
            mMostRecentlyUsed = node;
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Allow a Node which was allocated as evictable to be unevictable.
     */
    void makeUnevictable(Node node) {
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            if (mMaxNodeCount == 0) {
                // Closed.
                return;
            }
            Node lessUsed = node.mLessUsed;
            Node moreUsed = node.mMoreUsed;
            if (lessUsed == null) {
                (mLeastRecentlyUsed = moreUsed).mLessUsed = null;
            } else if (moreUsed == null) {
                (mMostRecentlyUsed = lessUsed).mMoreUsed = null;
            } else {
                node.mLessUsed.mMoreUsed = moreUsed;
                node.mMoreUsed.mLessUsed = lessUsed;
            }
            node.mMoreUsed = null;
            node.mLessUsed = null;
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(Node node) {
        return node.mCachedState != mCommitState && node.mId != Node.STUB_ID;
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method does
     * nothing if node is already dirty. Latch is never released by this method,
     * even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(Tree tree, Node node) throws IOException {
        if (node.mCachedState == mCommitState || node.mId == Node.STUB_ID) {
            return false;
        } else {
            doMarkDirty(tree, node);
            return true;
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method does
     * nothing if node is already dirty. Latch is never released by this method,
     * even if an exception is thrown.
     */
    void markUndoLogDirty(Node node) throws IOException {
        if (node.mCachedState != mCommitState) {
            long oldId = node.mId;
            long newId = mAllocator.allocPage(null, node);
            mPageDb.deletePage(oldId);
            node.write(this);
            dirty(node, newId);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(Tree tree, Node node) throws IOException {
        long oldId = node.mId;
        long newId = mAllocator.allocPage(tree, node);
        if (oldId != 0) {
            mPageDb.deletePage(oldId);
        }
        if (node.mCachedState != CACHED_CLEAN) {
            node.write(this);
        }
        if (node == tree.mRoot && tree.mIdBytes != null) {
            byte[] newEncodedId = new byte[8];
            writeLong(newEncodedId, 0, newId);
            mRegistry.store(Transaction.BOGUS, tree.mIdBytes, newEncodedId);
        }
        dirty(node, newId);
    }

    /**
     * Caller must hold commit lock and exclusive latch on node.
     */
    private void dirty(Node node, long newId) {
        node.mId = newId;
        node.mCachedState = mCommitState;
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. This method
     * should only be called for nodes whose existing data is not needed.
     */
    void redirty(Node node) {
        node.mCachedState = mCommitState;
    }

    /**
     * Similar to markDirty method except no new page is reserved, and old page
     * is not immediately deleted. Caller must hold commit lock and exclusive
     * latch on node. Latch is never released by this method, even if an
     * exception is thrown.
     */
    void prepareToDelete(Node node) throws IOException {
        // Hello. My name is Íñigo Montoya. You killed my father. Prepare to die. 
        byte state = node.mCachedState;
        if (state != CACHED_CLEAN && state != mCommitState) {
            node.write(this);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Latch is
     * always released by this method, even if an exception is thrown.
     */
    void deleteNode(Tree fromTree, Node node) throws IOException {
        try {
            deletePage(fromTree, node.mId, node.mCachedState);

            node.mId = 0;
            // TODO: child node array should be recycled
            node.mChildNodes = null;

            // When node is re-allocated, it will be evicted. Ensure that eviction
            // doesn't write anything.
            node.mCachedState = CACHED_CLEAN;
        } finally {
            node.releaseExclusive();
        }

        // Indicate that node is least recently used, allowing it to be
        // re-allocated immediately without evicting another node. Node must be
        // unlatched at this point, to prevent it from being immediately
        // promoted to most recently used by allocLatchedNode.
        final Latch usageLatch = mUsageLatch;
        usageLatch.acquireExclusive();
        try {
            Node lessUsed = node.mLessUsed;
            if (lessUsed != null) {
                Node moreUsed = node.mMoreUsed;
                if ((lessUsed.mMoreUsed = moreUsed) == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
                (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
                mLeastRecentlyUsed = node;
            }
        } finally {
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Caller must hold commit lock.
     */
    void deletePage(Tree fromTree, long id, int cachedState) throws IOException {
        if (id != 0) {
            if (cachedState == mCommitState) {
                // Newly reserved page was never used, so recycle it.
                mAllocator.recyclePage(fromTree, id);
            } else {
                // Old data must survive until after checkpoint.
                mPageDb.deletePage(id);
            }
        }
    }

    /**
     * Indicate that non-root node is most recently used. Root node is not
     * managed in usage list and cannot be evicted. Caller must hold any latch
     * on node. Latch is never released by this method, even if an exception is
     * thrown.
     */
    void used(Node node) {
        // Node latch is only required for this check. Dirty nodes are evicted
        // in FIFO order, which helps spread out the write workload.
        if (node.mCachedState != CACHED_CLEAN) {
            return;
        }

        // Because this method can be a bottleneck, don't wait for exclusive
        // latch. If node is popular, it will get more chances to be identified
        // as most recently used. This strategy works well enough because cache
        // eviction is always a best-guess approach.
        final Latch usageLatch = mUsageLatch;
        if (usageLatch.tryAcquireExclusive()) {
            Node moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                Node lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            usageLatch.releaseExclusive();
        }
    }

    /**
     * Breakup a large value into separate pages, returning a new value which
     * encodes the page references. Caller must hold commit lock.
     *
     * Returned value begins with a one byte header:
     *
     * 0b0000_ffip
     *
     * The leading 4 bits define the encoding type, which must be 0. The 'f'
     * bits define the full value length field size: 2, 4, 6, or 8 bytes. The
     * array is limited to a 4 byte length, and so only the 2 and 4 byte forms
     * apply. The 'i' bit defines the inline content length field size: 0 or 2
     * bytes. The 'p' bit is clear if direct pointers are used, and set for
     * indirect pointers. Pointers are always 6 bytes.
     *
     * @param caller optional tree node which is latched and calling this method
     * @param forTree tree which is storing large value
     * @param max maximum allowed size for returned byte array; must not be
     * less than 11 (can be 9 if full value length is < 65536)
     * @return null if max is too small
     */
    byte[] fragment(Node caller, Tree forTree, byte[] value, int max) throws IOException {
        int pageSize = pageSize();
        int pageCount = value.length / pageSize;
        int remainder = value.length % pageSize;

        if (value.length >= 65536) {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 4 + 6);
        } else if (pageCount == 0 && remainder <= (max - (1 + 2 + 2))) {
            // Entire value fits inline. It didn't really need to be
            // encoded this way, but do as we're told.
            byte[] newValue = new byte[(1 + 2 + 2) + value.length];
            newValue[0] = 0x02; // ff=0, i=1, p=0
            writeShort(newValue, 1, value.length);     // full length
            writeShort(newValue, 1 + 2, value.length); // inline length
            System.arraycopy(value, 0, newValue, (1 + 2 + 2), value.length);
            return newValue;
        } else {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 2 + 6);
        }

        if (max < 0) {
            return null;
        }

        int pointerSpace = pageCount * 6;

        byte[] newValue;
        if (remainder <= max && remainder < 65536
            && (pointerSpace <= (max + (6 - 2) - remainder)))
        {
            // Remainder fits inline, minimizing internal fragmentation. All
            // extra pages will be full. All pointers fit too; encode direct.

            byte header;
            int offset;
            if (value.length >= 65536) {
                header = 0x06; // ff = 1, i=1
                offset = 1 + 4;
            } else {
                header = 0x02; // ff = 0, i=1
                offset = 1 + 2;
            }

            int poffset = offset + 2 + remainder;
            newValue = new byte[poffset + pointerSpace];
            if (pageCount > 0) {
                int voffset = remainder;
                while (true) {
                    Node node = allocDirtyNode(forTree);
                    try {
                        mFragmentCache.put(caller, node);
                        writeInt48(newValue, poffset, node.mId);
                        System.arraycopy(value, voffset, node.mPage, 0, pageSize);
                        if (pageCount == 1) {
                            break;
                        }
                    } finally {
                        node.releaseExclusive();
                    }
                    pageCount--;
                    poffset += 6;
                    voffset += pageSize;
                }
            }

            newValue[0] = header;
            writeShort(newValue, offset, remainder); // inline length
            System.arraycopy(value, 0, newValue, offset + 2, remainder);
        } else {
            // Remainder doesn't fit inline, so don't encode any inline
            // content. Last extra page will not be full.
            pageCount++;
            pointerSpace += 6;

            byte header;
            int offset;
            if (value.length >= 65536) {
                header = 0x04; // ff = 1, i=0
                offset = 1 + 4;
            } else {
                header = 0x00; // ff = 0, i=0
                offset = 1 + 2;
            }

            if (pointerSpace <= (max + 6)) {
                // All pointers fit, so encode as direct.
                newValue = new byte[offset + pointerSpace];
                if (pageCount > 0) {
                    int voffset = 0;
                    while (true) {
                        Node node = allocDirtyNode(forTree);
                        try {
                            mFragmentCache.put(caller, node);
                            writeInt48(newValue, offset, node.mId);
                            if (pageCount > 1) {
                                System.arraycopy(value, voffset, node.mPage, 0, pageSize);
                            } else {
                                System.arraycopy(value, voffset, node.mPage, 0, remainder);
                                break;
                            }
                        } finally {
                            node.releaseExclusive();
                        }
                        pageCount--;
                        offset += 6;
                        voffset += pageSize;
                    }
                }
            } else {
                // Use indirect pointers.
                newValue = new byte[offset + 6];
                // FIXME
                throw null;
            }

            newValue[0] = header;
        }

        // Encode full length field.
        if (value.length >= 65536) {
            writeInt(newValue, 1, value.length);
        } else {
            writeShort(newValue, 1, value.length);
        }

        return newValue;
    }

    /**
     * Reconstruct a fragmented value.
     *
     * @param caller optional tree node which is latched and calling this method
     */
    byte[] reconstruct(Node caller, byte[] fragmented, int off, int len) throws IOException {
        int header = fragmented[off++];
        int vLen;
        switch ((header >> 2) & 0x03) {
        default:
            vLen = readUnsignedShort(fragmented, off);
            break;

        case 1:
            vLen = readInt(fragmented, off);
            if (vLen < 0) {
                throw new DatabaseException("Value is too large: " + (vLen & 0xffffffffL));
            }
            break;

        case 2:
            long vLenL = readUnsignedInt48(fragmented, off);
            if (vLenL > Integer.MAX_VALUE) {
                throw new DatabaseException("Value is too large: " + vLenL);
            }
            vLen = (int) vLenL;
            break;

        case 3:
            vLenL = readLong(fragmented, off);
            if (vLenL < 0 || vLenL > Integer.MAX_VALUE) {
                // TODO: Special handling for printing unsigned long.
                throw new DatabaseException("Value is too large: " + vLenL);
            }
            vLen = (int) vLenL;
            break;
        }

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        byte[] value;
        try {
            value = new byte[vLen];
        } catch (OutOfMemoryError e) {
            throw new DatabaseException("Value is too large: " + vLen, e);
        }

        int vOff = 0;
        if ((header & 0x02) != 0) {
            // Inline content.
            int inLen = readUnsignedShort(fragmented, off);
            off += 2;
            len -= 2;
            System.arraycopy(fragmented, off, value, vOff, inLen);
            off += inLen;
            len -= inLen;
            vOff += inLen;
            vLen -= inLen;
        }

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = readUnsignedInt48(fragmented, off);
                off += 6;
                len -= 6;
                Node node = mFragmentCache.get(caller, nodeId);
                try {
                    byte[] page = node.mPage;
                    int pLen = Math.min(vLen, page.length);
                    System.arraycopy(page, 0, value, vOff, pLen);
                    vOff += pLen;
                    vLen -= pLen;
                } finally {
                    node.releaseShared();
                }
            }
        } else {
            // Indirect pointers.
            // FIXME
        }

        return value;
    }

    /**
     * Delete the extra pages of a fragmented value. Caller must hold commit
     * lock.
     *
     * @param caller optional tree node which is latched and calling this method
     * @param fromTree tree which is deleting the large value
     */
    void deleteFragments(Node caller, Tree fromTree, byte[] fragmented, int off, int len)
        throws IOException
    {
        int header = fragmented[off++];

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        if ((header & 0x02) != 0) {
            // Skip inline content.
            int inLen = 2 + readUnsignedShort(fragmented, off);
            off += inLen;
            len -= inLen;
        }

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = readUnsignedInt48(fragmented, off);
                off += 6;
                len -= 6;
                Node node = mFragmentCache.remove(caller, nodeId);
                if (node != null) {
                    deleteNode(fromTree, node);
                } else {
                    // Page is clean if not in a Node, and so it must survive
                    // until after the next checkpoint.
                    mPageDb.deletePage(nodeId);
                }
            }
        } else {
            // Indirect pointers.
            // FIXME
        }
    }

    /**
     * Obtain the trash for transactionally deleting fragmented values.
     */
    FragmentedTrash fragmentedTrash() throws IOException {
        FragmentedTrash trash = mFragmentedTrash;
        if (trash != null) {
            return trash;
        }
        synchronized (mOpenTrees) {
            if ((trash = mFragmentedTrash) != null) {
                return trash;
            }
            Tree tree = openInternalTree(Tree.FRAGMENTED_TRASH_ID, true);
            return mFragmentedTrash = new FragmentedTrash(tree);
        }
    }

    byte[] removeSpareBuffer() throws InterruptedIOException {
        return mSpareBufferPool.remove();
    }

    void addSpareBuffer(byte[] buffer) {
        mSpareBufferPool.add(buffer);
    }

    void readPage(long id, byte[] page) throws IOException {
        mPageDb.readPage(id, page);
    }

    void writePage(long id, byte[] page) throws IOException {
        mPageDb.writePage(id, page);
    }

    private void checkpoint(boolean force) throws IOException {
        // Checkpoint lock ensures consistent state between page store and logs.
        synchronized (mCheckpointLock) {
            final Node root = mRegistry.mRoot;

            // Exclusive commit lock must be acquired first, to prevent deadlock.

            // If the commit lock cannot be immediately obtained, it's due to a
            // shared lock being held for a long time. While waiting for the
            // exclusive lock, all other shared requests are queued. By waiting
            // a timed amount and giving up, the exclusive lock request is
            // effectively de-prioritized. For each retry, the timeout is
            // doubled, to ensure that the checkpoint request is not starved.
            try {
                Lock commitLock = mPageDb.exclusiveCommitLock();
                long timeoutMillis = 1;
                while (!commitLock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    timeoutMillis <<= 1;
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }

            root.acquireShared();

            if (!force && root.mCachedState == CACHED_CLEAN) {
                // Root is clean, so nothing to do.
                root.releaseShared();
                mPageDb.exclusiveCommitLock().unlock();
                return;
            }

            // TODO: I don't like all this activity with exclusive commit lock
            // held. New RedoLog file can probably be created optimistically.
            // UndoLog can be refactored to store into a special Tree, but this
            // requires more features to be added to Tree first. Specifically,
            // large values and appending to them.

            final long redoLogId;
            try {
                redoLogId = mRedoLog.openNewFile();
            } catch (IOException e) {
                root.releaseShared();
                mPageDb.exclusiveCommitLock().unlock();
                throw e;
            }

            final UndoLog masterUndoLog;
            final long masterUndoLogId;
            synchronized (mTxnIdLock) {
                int count = mUndoLogCount;
                if (count == 0) {
                    masterUndoLog = null;
                    masterUndoLogId = 0;
                } else {
                    final int stateToFlush = mCommitState;
                    masterUndoLog = UndoLog.newMasterUndoLog(this);
                    byte[] workspace = null;
                    for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
                        workspace = log.writeToMaster(masterUndoLog, workspace);
                    }
                    // Release latch to allow flush to acquire and release it.
                    masterUndoLogId = masterUndoLog.releaseNodeLatch();
                }
            }

            mPageDb.commit(new PageDb.CommitCallback() {
                @Override
                public byte[] prepare() throws IOException {
                    return flush(redoLogId, masterUndoLogId);
                }
            });

            if (masterUndoLog != null) {
                // Delete the master undo log, which won't take effect until
                // the next checkpoint.
                masterUndoLog.acquireNodeLatch(masterUndoLogId);
                masterUndoLog.truncate(false);
            }

            // Note: The delete step can get skipped if process exits at this
            // point. File is deleted again when database is re-opened.
            mRedoLog.deleteOldFile(redoLogId);

            // Deleted pages are now available for new allocations.
            mAllocator.fill();
        }
    }

    /**
     * Method is invoked with exclusive commit lock and shared root node latch
     * held. Both are released by this method.
     */
    private byte[] flush(final long redoLogId, final long masterUndoLogId) throws IOException {
        final long txnId;
        synchronized (mTxnIdLock) {
            txnId = mTxnId;
        }
        final Node root = mRegistry.mRoot;
        final long rootId = root.mId;
        final int stateToFlush = mCommitState;
        mCommitState = (byte) (stateToFlush ^ 1);
        root.releaseShared();
        mPageDb.exclusiveCommitLock().unlock();

        mAllocator.beginDirtyIteration();
        Node node;
        while ((node = mAllocator.removeNextDirtyNode(stateToFlush)) != null) {
            node.mCachedState = CACHED_CLEAN;
            node.downgrade();
            try {
                node.write(this);
            } finally {
                node.releaseShared();
            }
        }

        byte[] header = new byte[HEADER_SIZE];
        writeInt(header, I_ENCODING_VERSION, ENCODING_VERSION);
        writeLong(header, I_ROOT_PAGE_ID, rootId);
        writeLong(header, I_MASTER_UNDO_LOG_PAGE_ID, masterUndoLogId);
        writeLong(header, I_TRANSACTION_ID, txnId);
        // Add one to redoLogId, indicating the active log id.
        writeLong(header, I_REDO_LOG_ID, redoLogId + 1);

        return header;
    }
}
