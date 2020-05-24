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

package org.cojen.tupl.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import java.math.BigInteger;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.ConcurrentSkipListMap;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.ReentrantLock;

import java.util.function.LongConsumer;

import static java.lang.System.arraycopy;

import static java.util.Arrays.fill;

import org.cojen.tupl.CacheExhaustedException;
import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.CompactionObserver;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Crypto;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DatabaseFullException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;
import org.cojen.tupl.Index;
import org.cojen.tupl.LargeKeyException;
import org.cojen.tupl.LargeValueException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;
import org.cojen.tupl.VerificationObserver;
import org.cojen.tupl.View;

import org.cojen.tupl.ev.SafeEventListener;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.Handler;
import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

import static org.cojen.tupl.core.Node.*;
import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Standard database implementation. The name "LocalDatabase" is used to imply that the
 * database is local to the current machine and not remotely accessed, although no remote
 * database layer exists. This class could just as well have been named "DatabaseImpl".
 *
 * @author Brian S O'Neill
 */
public final class LocalDatabase extends CoreDatabase {
    private static final int DEFAULT_CACHE_NODES = 1000;
    // +2 for registry and key map root nodes, +1 for one user index, and +2 for at least one
    // usage list to function correctly.
    private static final int MIN_CACHE_NODES = 5;

    private static final long PRIMER_MAGIC_NUMBER = 4943712973215968399L;

    private static final String LOCK_FILE_SUFFIX = ".lock";
    static final String PRIMER_FILE_SUFFIX = ".primer";
    static final String REDO_FILE_SUFFIX = ".redo.";

    private static int nodeCountFromBytes(long bytes, int pageSize) {
        if (bytes <= 0) {
            return 0;
        }
        pageSize += NODE_OVERHEAD;
        bytes += pageSize - 1;
        if (bytes <= 0) {
            // Overflow.
            return Integer.MAX_VALUE;
        }
        long count = bytes / pageSize;
        return count <= Integer.MAX_VALUE ? (int) count : Integer.MAX_VALUE;
    }

    private static long byteCountFromNodes(int nodes, int pageSize) {
        return nodes * (long) (pageSize + NODE_OVERHEAD);
    }

    private static final int ENCODING_VERSION = 20130112;

    private static final int I_ENCODING_VERSION        = 0;
    private static final int I_ROOT_PAGE_ID            = I_ENCODING_VERSION + 4;
    private static final int I_MASTER_UNDO_LOG_PAGE_ID = I_ROOT_PAGE_ID + 8;
    private static final int I_TRANSACTION_ID          = I_MASTER_UNDO_LOG_PAGE_ID + 8;
    private static final int I_CHECKPOINT_NUMBER       = I_TRANSACTION_ID + 8;
    private static final int I_REDO_TXN_ID             = I_CHECKPOINT_NUMBER + 8;
    private static final int I_REDO_POSITION           = I_REDO_TXN_ID + 8;
    private static final int I_REPL_ENCODING           = I_REDO_POSITION + 8;
    private static final int HEADER_SIZE               = I_REPL_ENCODING + 8;

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int MINIMUM_PAGE_SIZE = 512;
    private static final int MAXIMUM_PAGE_SIZE = 65536;

    private static final int OPEN_REGULAR = 0, OPEN_DESTROY = 1, OPEN_TEMP = 2;

    final EventListener mEventListener;

    private final File mBaseFile;
    private final boolean mReadOnly;
    private final LockedFile mLockFile;

    final DurabilityMode mDurabilityMode;
    final long mDefaultLockTimeoutNanos;
    final LockManager mLockManager;
    private final ThreadLocal<SoftReference<LocalTransaction>> mLocalTransaction;
    final RedoWriter mRedoWriter;
    final PageDb mPageDb;
    final int mPageSize;

    private final Object mArena;
    private final NodeGroup[] mNodeGroups;

    private final CommitLock mCommitLock;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    // State to apply to nodes which have just been read. Is CACHED_DIRTY_0 for empty databases
    // which have never checkpointed, but is CACHED_CLEAN otherwise.
    private volatile byte mInitialReadState = CACHED_CLEAN;

    // Set during checkpoint after commit state has switched. If checkpoint aborts, next
    // checkpoint will resume with this commit header and master undo log.
    /*P*/ // [
    private byte[] mCommitHeader;
    /*P*/ // |
    /*P*/ // private long mCommitHeader = p_null();
    /*P*/ // private static final VarHandle cCommitHeaderHandle;
    /*P*/ // ]
    private UndoLog mCommitMasterUndoLog;

    // Typically opposite of mCommitState, or negative if checkpoint is not in
    // progress. Indicates which nodes are being flushed by the checkpoint.
    private volatile int mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;

    private static final int CHECKPOINT_FLUSH_PREPARE = -2, CHECKPOINT_NOT_FLUSHING = -1;

    // The root tree, which maps tree ids to other tree root node ids.
    private final BTree mRegistry;

    // RK == Registry Key prefixes used with mRegistryKeyMap.
    static final byte RK_INDEX_NAME   =  0; // name to id mapping for user trees
    static final byte RK_INDEX_ID     =  1; // id to name mapping for user trees
    static final byte RK_TREE_ID_MASK =  2; // full key for random tree id mask (deprecated)
    static final byte RK_NEXT_TREE_ID =  3; // full key for tree id sequence
    static final byte RK_TRASH_ID     =  4; // id to name mapping of trash
    static final byte RK_NEXT_TEMP_ID =  5; // full key for temporary tree id sequence
    static final byte RK_CUSTOM_NAME  =  6; // name to id mapping for custom handlers
    static final byte RK_CUSTOM_ID    =  7; // id to name mapping for custom handlers
    static final byte RK_PREPARE_NAME =  8; // name to id mapping for prepare handlers
    static final byte RK_PREPARE_ID   =  9; // id to name mapping for prepare handlers

    // Various mappings, defined by RK_ fields.
    private final BTree mRegistryKeyMap;

    private final Latch mOpenTreesLatch;
    // Maps tree names to open trees.
    // Must be a concurrent map because we rely on concurrent iteration.
    private final Map<byte[], TreeRef> mOpenTrees;
    private final LHashTable.Obj<TreeRef> mOpenTreesById;
    private final ReferenceQueue<Object> mOpenTreesRefQueue;

    // Map of all loaded nodes.
    private Node[] mNodeMapTable;
    private static final Node NM_LOCK = new Node();
    private static final VarHandle cNodeMapElementHandle;

    final int mMaxKeySize;
    final int mMaxEntrySize;
    final int mMaxFragmentedEntrySize;

    // Fragmented values which are transactionally deleted go here.
    private BTree mFragmentedTrash;

    // Pre-calculated maximum capacities for inode levels.
    private final long[] mFragmentInodeLevelCaps;

    // Stripe the transaction contexts, for improved concurrency.
    private final TransactionContext[] mTxnContexts;

    // Checkpoint lock is fair, to ensure that user checkpoint requests are not stalled for too
    // long by checkpoint thread.
    private final ReentrantLock mCheckpointLock = new ReentrantLock(true);

    private long mLastCheckpointNanos;

    private final Checkpointer mCheckpointer;

    final TempFileManager mTempFileManager;

    /*P*/ // [|
    /*P*/ // final boolean mFullyMapped;
    /*P*/ // ]

    // Maps registered cursor ids to index ids.
    private BTree mCursorRegistry;

    // Registered custom handlers.
    private final Map<String, CustomHandler> mCustomHandlers;
    private final LHashTable.Obj<CustomHandler> mCustomHandlersById;

    // Registered prepare handlers.
    private final Map<String, PrepareHandler> mPrepareHandlers;
    private final LHashTable.Obj<PrepareHandler> mPrepareHandlersById;

    // Maps transaction id to the handler id and optional message.
    private BTree mPreparedTxns;

    private volatile int mClosed;
    private volatile Throwable mClosedCause;

    private static final VarHandle cClosedHandle;

    static {
        try {
            cClosedHandle =
                MethodHandles.lookup().findVarHandle
                (LocalDatabase.class, "mClosed", int.class);

            /*P*/ // [|
            /*P*/ // cCommitHeaderHandle =
            /*P*/ //     MethodHandles.lookup().findVarHandle
            /*P*/ //     (LocalDatabase.class, "mCommitHeader", long.class);
            /*P*/ // ]

            cNodeMapElementHandle = MethodHandles.arrayElementVarHandle(Node[].class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * Open a database, creating it if necessary.
     */
    static LocalDatabase open(Launcher launcher) throws IOException {
        var db = new LocalDatabase(launcher, OPEN_REGULAR);
        try {
            db.finishInit(launcher);
            return db;
        } catch (Throwable e) {
            closeQuietly(db);
            throw e;
        }
    }

    /**
     * Delete the contents of an existing database, and replace it with an
     * empty one. When using a raw block device for the data file, this method
     * must be used to format it.
     */
    static LocalDatabase destroy(Launcher launcher) throws IOException {
        if (launcher.mReadOnly) {
            throw new IllegalArgumentException("Cannot destroy read-only database");
        }
        var db = new LocalDatabase(launcher, OPEN_DESTROY);
        try {
            db.finishInit(launcher);
            return db;
        } catch (Throwable e) {
            closeQuietly(db);
            throw e;
        }
    }

    /**
     * @param launcher base file is set as a side-effect
     */
    static BTree openTemp(TempFileManager tfm, Launcher launcher) throws IOException {
        File file = tfm.createTempFile();
        launcher.baseFile(file);
        launcher.dataFiles(file);
        launcher.createFilePath(false);
        launcher.durabilityMode(DurabilityMode.NO_FLUSH);
        var db = new LocalDatabase(launcher, OPEN_TEMP);
        tfm.register(file, db);
        db.mCheckpointer.start(false);
        return db.mRegistry;
    }

    /**
     * @param launcher unshared launcher
     */
    private LocalDatabase(Launcher launcher, int openMode) throws IOException {
        launcher.mEventListener = mEventListener = 
            SafeEventListener.makeSafe(launcher.mEventListener);

        mCustomHandlers = Launcher.mapClone(launcher.mCustomHandlers);
        mCustomHandlersById = Launcher.newByIdMap(mCustomHandlers);

        mPrepareHandlers = Launcher.mapClone(launcher.mPrepareHandlers);
        mPrepareHandlersById = Launcher.newByIdMap(mPrepareHandlers);

        mBaseFile = launcher.mBaseFile;
        mReadOnly = launcher.mReadOnly;
        final File[] dataFiles = launcher.dataFiles();

        int pageSize = launcher.mPageSize;
        boolean explicitPageSize = true;
        if (pageSize <= 0) {
            launcher.pageSize(pageSize = DEFAULT_PAGE_SIZE);
            explicitPageSize = false;
        } else if (pageSize < MINIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too small: " + pageSize + " < " + MINIMUM_PAGE_SIZE);
        } else if (pageSize > MAXIMUM_PAGE_SIZE) {
            throw new IllegalArgumentException
                ("Page size is too large: " + pageSize + " > " + MAXIMUM_PAGE_SIZE);
        } else if ((pageSize & 1) != 0) {
            throw new IllegalArgumentException("Page size must be even: " + pageSize);
        }

        int minCache, maxCache;
        cacheSize: {
            long minCacheBytes = Math.max(0, launcher.mMinCacheBytes);
            long maxCacheBytes = Math.max(0, launcher.mMaxCacheBytes);

            if (maxCacheBytes == 0) {
                maxCacheBytes = minCacheBytes;
                if (maxCacheBytes == 0) {
                    minCache = maxCache = DEFAULT_CACHE_NODES;
                    break cacheSize;
                }
            }

            if (minCacheBytes > maxCacheBytes) {
                throw new IllegalArgumentException
                    ("Minimum cache size exceeds maximum: " +
                     minCacheBytes + " > " + maxCacheBytes);
            }

            minCache = nodeCountFromBytes(minCacheBytes, pageSize);
            maxCache = nodeCountFromBytes(maxCacheBytes, pageSize);

            minCache = Math.max(MIN_CACHE_NODES, minCache);
            maxCache = Math.max(MIN_CACHE_NODES, maxCache);
        }

        // Update launcher such that info file is correct.
        launcher.mMinCacheBytes = byteCountFromNodes(minCache, pageSize);
        launcher.mMaxCacheBytes = byteCountFromNodes(maxCache, pageSize);

        mDurabilityMode = launcher.mDurabilityMode;
        mDefaultLockTimeoutNanos = launcher.mLockTimeoutNanos;
        mLockManager = new LockManager(this, launcher.mLockUpgradeRule, mDefaultLockTimeoutNanos);
        mLocalTransaction = new ThreadLocal<>();

        // Initialize NodeMapTable, the primary cache of Nodes.
        {
            int capacity = Utils.roundUpPower2(maxCache);
            if (capacity < 0) {
                capacity = 0x40000000;
            }
            mNodeMapTable = new Node[capacity];
        }

        if (mBaseFile != null && !mReadOnly && launcher.mMkdirs) {
            File baseDir = mBaseFile.getParentFile();
            final boolean baseDirectoriesCreated = baseDir.mkdirs();

            if (!baseDirectoriesCreated && !baseDir.exists()) {
                throw new FileNotFoundException("Could not create directory: " + baseDir);
            }

            if (dataFiles != null) {
                for (File f : dataFiles) {
                    File dataDir = f.getParentFile();
                    final boolean dataDirectoriesCreated = dataDir.mkdirs();

                    if (!dataDirectoriesCreated && !dataDir.exists()) {
                        throw new FileNotFoundException("Could not create directory: " + dataDir);
                    }
                }
            }
        }

        final int procCount = Runtime.getRuntime().availableProcessors();

        LockedFile attemptCreate = null;

        try {
            // Create lock file, preventing database from being opened multiple times.
            if (mBaseFile == null || openMode == OPEN_TEMP) {
                mLockFile = null;
            } else {
                var lockFile = new File(lockFilePath());

                boolean didExist = lockFile.exists();

                mLockFile = new LockedFile(lockFile, mReadOnly);

                if (!didExist) {
                    attemptCreate = mLockFile;
                }
            }

            if (openMode == OPEN_DESTROY) {
                deleteRedoLogFiles();
            }

            final long cacheInitStart = System.nanoTime();

            /*P*/ // [|
            /*P*/ // boolean fullyMapped = false;
            /*P*/ // ]

            EventListener debugListener = null;
            if (launcher.mDebugOpen != null) {
                debugListener = mEventListener;
            }

            if (dataFiles == null) {
                PageArray dataPageArray = launcher.mDataPageArray;
                if (dataPageArray == null) {
                    mPageDb = new NonPageDb(pageSize);
                } else {
                    dataPageArray = dataPageArray.open();
                    Crypto crypto = launcher.mCrypto;
                    mPageDb = DurablePageDb.open
                        (debugListener, dataPageArray, crypto, openMode == OPEN_DESTROY);
                    /*P*/ // [|
                    /*P*/ // fullyMapped = crypto == null && dataPageArray.isFullyMapped();
                    /*P*/ // ]
                }
            } else {
                EnumSet<OpenOption> options = launcher.createOpenOptions();

                PageDb pageDb;
                try {
                    pageDb = DurablePageDb.open
                        (debugListener, explicitPageSize, pageSize,
                         dataFiles, options,
                         launcher.mCrypto, openMode == OPEN_DESTROY);
                } catch (FileNotFoundException e) {
                    if (!mReadOnly) {
                        throw e;
                    }
                    pageDb = new NonPageDb(pageSize);
                }

                mPageDb = pageDb;
            }

            /*P*/ // [|
            /*P*/ // mFullyMapped = fullyMapped;
            /*P*/ // ]

            // Actual page size might differ from configured size.
            launcher.pageSize(pageSize = mPageSize = mPageDb.pageSize());

            /*P*/ // [
            launcher.mDirectPageAccess = false;
            /*P*/ // |
            /*P*/ // launcher.mDirectPageAccess = true;
            /*P*/ // ]

            mCommitLock = mPageDb.commitLock();

            // Pre-allocate nodes. They are automatically added to the node group usage lists,
            // and so nothing special needs to be done to allow them to get used. Since the
            // initial state is clean, evicting these nodes does nothing.

            if (mEventListener != null) {
                mEventListener.notify(EventType.CACHE_INIT_BEGIN,
                                      "Initializing %1$d cache nodes", minCache);
            }

            NodeGroup[] groups;
            try {
                // Try to allocate the minimum cache size into an arena, which has lower memory
                // overhead, is page aligned, and takes less time to zero-fill.
                arenaAlloc: {
                    // If database is fully mapped, then no cache pages are allocated at all.
                    // Nodes point directly to a mapped region of memory.
                    /*P*/ // [|
                    /*P*/ // if (mFullyMapped) {
                    /*P*/ //     mArena = null;
                    /*P*/ //     break arenaAlloc;
                    /*P*/ // }
                    /*P*/ // ]

                    try {
                        mArena = p_arenaAlloc(pageSize, minCache); 
                    } catch (IOException e) {
                        var oom = new OutOfMemoryError();
                        oom.initCause(e);
                        throw oom;
                    }
                }

                long usedRate;
                if (mPageDb.isDurable()) {
                    // Magic constant was determined empirically against the G1 collector. A
                    // higher constant increases memory thrashing.
                    usedRate = Utils.roundUpPower2((long) Math.ceil(maxCache / 32768.0)) - 1;
                } else {
                    // Nothing gets evicted, so no need to ever adjust usage order.
                    usedRate = -1;
                }

                int stripes = roundUpPower2(procCount * 16);

                int stripeSize;
                while (true) {
                    stripeSize = maxCache / stripes;
                    if (stripes <= 1 || stripeSize >= 100) {
                        break;
                    }
                    stripes >>= 1;
                }

                int rem = maxCache % stripes;

                groups = new NodeGroup[stripes];

                for (int i=0; i<stripes; i++) {
                    int size = stripeSize;
                    if (rem > 0) {
                        size++;
                        rem--;
                    }
                    groups[i] = new NodeGroup(this, usedRate, size);
                }

                stripeSize = minCache / stripes;
                rem = minCache % stripes;

                for (NodeGroup group : groups) {
                    int size = stripeSize;
                    if (rem > 0) {
                        size++;
                        rem--;
                    }
                    group.initialize(mArena, size);
                }
            } catch (OutOfMemoryError e) {
                groups = null;
                var oom = new OutOfMemoryError
                    ("Unable to allocate the minimum required number of cache nodes: " +
                     minCache + " (" + (minCache * (long) (pageSize + NODE_OVERHEAD)) + " bytes)");
                oom.initCause(e.getCause());
                throw oom;
            }

            mNodeGroups = groups;

            if (mEventListener != null) {
                double duration = (System.nanoTime() - cacheInitStart) / 1_000_000_000.0;
                mEventListener.notify(EventType.CACHE_INIT_COMPLETE,
                                      "Cache initialization completed in %1$1.3f seconds",
                                      duration, TimeUnit.SECONDS);
            }

            mTxnContexts = new TransactionContext[procCount * 4];
            for (int i=0; i<mTxnContexts.length; i++) {
                mTxnContexts[i] = new TransactionContext(mTxnContexts.length, 4096);
            };

            mCommitLock.acquireExclusive();
            try {
                mCommitState = CACHED_DIRTY_0;
            } finally {
                mCommitLock.releaseExclusive();
            }

            var header = new byte[HEADER_SIZE];
            mPageDb.readExtraCommitData(header);

            // Also verifies the database and replication encodings.
            Node rootNode = loadRegistryRoot(launcher, header);

            // Cannot call newBTreeInstance because mRedoWriter isn't set yet.
            if (launcher.mRepl != null) {
                mRegistry = new BTree.Repl(this, Tree.REGISTRY_ID, null, rootNode);
            } else {
                mRegistry = new BTree(this, Tree.REGISTRY_ID, null, rootNode);
            }

            mOpenTreesLatch = new Latch();
            if (openMode == OPEN_TEMP) {
                mOpenTrees = Collections.emptyMap();
                mOpenTreesById = new LHashTable.Obj<>(0);
                mOpenTreesRefQueue = null;
            } else {
                mOpenTrees = new ConcurrentSkipListMap<>(KeyComparator.THE);
                mOpenTreesById = new LHashTable.Obj<>(16);
                mOpenTreesRefQueue = new ReferenceQueue<>();
            }

            long txnId = decodeLongLE(header, I_TRANSACTION_ID);
            if (txnId < 0) {
                throw new CorruptDatabaseException("Invalid transaction id: " + txnId);
            }

            long redoNum = decodeLongLE(header, I_CHECKPOINT_NUMBER);
            long redoPos = decodeLongLE(header, I_REDO_POSITION);
            long redoTxnId = decodeLongLE(header, I_REDO_TXN_ID);

            if (debugListener != null) {
                debugListener.notify(EventType.DEBUG, "MASTER_UNDO_LOG_PAGE_ID: %1$d",
                                     decodeLongLE(header, I_MASTER_UNDO_LOG_PAGE_ID));
                debugListener.notify(EventType.DEBUG, "TRANSACTION_ID: %1$d", txnId);
                debugListener.notify(EventType.DEBUG, "CHECKPOINT_NUMBER: %1$d", redoNum);
                debugListener.notify(EventType.DEBUG, "REDO_TXN_ID: %1$d", redoTxnId);
                debugListener.notify(EventType.DEBUG, "REDO_POSITION: %1$d", redoPos);
            }

            if (openMode == OPEN_TEMP) {
                mRegistryKeyMap = null;
            } else {
                mRegistryKeyMap = openInternalTree(Tree.REGISTRY_KEY_MAP_ID, IX_CREATE, launcher);
                if (debugListener != null) {
                    Cursor c = indexRegistryById().newCursor(Transaction.BOGUS);
                    try {
                        for (c.first(); c.key() != null; c.next()) {
                            long indexId = decodeLongBE(c.key(), 0);
                            var nameStr = new String(c.value(), StandardCharsets.UTF_8);
                            debugListener.notify(EventType.DEBUG, "Index: id=%1$d, name=%2$s",
                                                 indexId, nameStr);
                        }
                    } finally {
                        c.reset();
                    }
                }
            }

            BTree cursorRegistry = null;
            if (openMode != OPEN_TEMP) {
                cursorRegistry = openInternalTree(Tree.CURSOR_REGISTRY_ID, IX_FIND, launcher);
            }

            // Limit maximum non-fragmented entry size to 0.75 of usable node size.
            mMaxEntrySize = ((pageSize - Node.TN_HEADER_SIZE) * 3) >> 2;

            // Limit maximum fragmented entry size to guarantee that 2 entries fit. Each also
            // requires 2 bytes for pointer and up to 3 bytes for value length field.
            mMaxFragmentedEntrySize = (pageSize - Node.TN_HEADER_SIZE - (2 + 3 + 2 + 3)) >> 1;

            // Limit the maximum key size to allow enough room for a fragmented value. It might
            // require up to 11 bytes for fragment encoding (when length is >= 65536), and
            // additional bytes are required for the value header inside the tree node.
            mMaxKeySize = Math.min(16383, mMaxFragmentedEntrySize - (2 + 11));

            mFragmentInodeLevelCaps = calculateInodeLevelCaps(mPageSize);

            // Enable caching of PageQueue nodes before recovery begins, because it will be
            // deleting pages, and caching helps performance.
            mPageDb.pageCache(this);

            if (mBaseFile == null || openMode == OPEN_TEMP || debugListener != null) {
                mTempFileManager = null;
            } else {
                mTempFileManager = new TempFileManager(mBaseFile);
            }

            long recoveryStart = 0;
            if (mBaseFile == null) {
                mRedoWriter = null;
                mCheckpointer = null;
            } else if (openMode == OPEN_TEMP) {
                mRedoWriter = null;
                mCheckpointer = new Checkpointer(this, launcher, mNodeGroups.length);
            } else {
                if (debugListener != null) {
                    mCheckpointer = null;
                } else {
                    mCheckpointer = new Checkpointer(this, launcher, mNodeGroups.length);
                }

                // Perform recovery by examining redo and undo logs.

                if (mEventListener != null) {
                    mEventListener.notify(EventType.RECOVERY_BEGIN, "Database recovery begin");
                    recoveryStart = System.nanoTime();
                }

                var txns = new LHashTable.Obj<LocalTransaction>(16);
                {
                    long masterNodeId = decodeLongLE(header, I_MASTER_UNDO_LOG_PAGE_ID);
                    if (masterNodeId != 0) {
                        if (mEventListener != null) {
                            mEventListener.notify
                                (EventType.RECOVERY_LOAD_UNDO_LOGS, "Loading undo logs");
                        }

                        UndoLog master = UndoLog.recoverMasterUndoLog(this, masterNodeId);

                        boolean trace = debugListener != null &&
                            Boolean.TRUE.equals(launcher.mDebugOpen.get("traceUndo"));

                        master.recoverTransactions(debugListener, trace, txns);
                    }
                }

                var cursors = new LHashTable.Obj<BTreeCursor>(4);
                if (cursorRegistry != null) {
                    Cursor c = cursorRegistry.newCursor(Transaction.BOGUS);
                    for (c.first(); c.key() != null; c.next()) {
                        long cursorId = decodeLongBE(c.key(), 0);
                        byte[] regValue = c.value();
                        long indexId = decodeLongBE(regValue, 0);
                        var tree = (BTree) anyIndexById(indexId);

                        var cursor = new BTreeCursor(tree, Transaction.BOGUS);
                        cursor.mKeyOnly = true;

                        if (regValue.length >= 9) {
                            // Cursor key was registered too.
                            var key = new byte[regValue.length - 9];
                            System.arraycopy(regValue, 9, key, 0, key.length);
                            cursor.find(key);
                        }

                        // Assign after any find operation, because it will reset the cursor id.
                        cursor.mCursorId = cursorId;

                        cursors.insert(cursorId).value = cursor;
                    }

                    cursorRegistry.forceClose();
                }

                // Although the handlers shouldn't access the database yet, be safe and call
                // this method at the point that the database is mostly functional. The handler
                // methods will be called soon to perform recovery.
                initHandlers(this, mCustomHandlers, mPrepareHandlers);

                // Must tag the trashed trees before starting replication and recovery.
                // Otherwise, trees recently deleted might get double deleted.
                tagTrashedTrees();

                StreamReplicator repl = launcher.mRepl;
                if (repl != null) {
                    if (mEventListener != null) {
                        mEventListener.notify(EventType.REPLICATION_DEBUG,
                                              "Starting at: %1$d", redoPos);
                    }

                    repl.start();

                    if (mReadOnly) {
                        mRedoWriter = null;

                        if (debugListener != null &&
                            Boolean.TRUE.equals(launcher.mDebugOpen.get("traceRedo")))
                        {
                            var printer = new RedoEventPrinter(debugListener, EventType.DEBUG);
                            new ReplDecoder(repl, redoPos, redoTxnId, new Latch()).run(printer);
                        }
                    } else {
                        var engine = new ReplEngine
                            (repl, launcher.mMaxReplicaThreads, this, txns, cursors);
                        mRedoWriter = engine.initWriter(redoNum);

                        // Cannot start recovery until constructor is finished and final field
                        // values are visible to other threads. Pass the state to the caller
                        // through the launcher object.
                        launcher.mReplRecoveryStartNanos = recoveryStart;
                        launcher.mReplInitialPostion = redoPos;
                        launcher.mReplInitialTxnId = redoTxnId;
                    }
                } else {
                    // Apply cache primer before applying redo logs.
                    applyCachePrimer(launcher);

                    final long logId = redoNum;

                    if (mReadOnly) {
                        mRedoWriter = null;

                        if (debugListener != null &&
                            Boolean.TRUE.equals(launcher.mDebugOpen.get("traceRedo")))
                        {
                            var printer = new RedoEventPrinter(debugListener, EventType.DEBUG);

                            var replayLog = new RedoLog(launcher, logId, redoPos);

                            replayLog.replay
                                (printer, debugListener, EventType.RECOVERY_APPLY_REDO_LOG,
                                 "Applying redo log: %1$d");
                        }
                    } else {
                        // Make sure old redo logs are deleted. Process might have exited
                        // before last checkpoint could delete them.
                        deleteNumberedFiles(mBaseFile, REDO_FILE_SUFFIX, 0, logId - 1);

                        boolean doCheckpoint = txns.size() != 0;

                        var applier = new RedoLogApplier
                            (launcher.mMaxReplicaThreads, this, txns, cursors);
                        var replayLog = new RedoLog(launcher, logId, redoPos);

                        // As a side-effect, log id is set one higher than last file scanned.
                        TreeMap<Long, File> redoFiles = replayLog.replay
                            (applier, mEventListener, EventType.RECOVERY_APPLY_REDO_LOG,
                             "Applying redo log: %1$d");

                        doCheckpoint |= !redoFiles.isEmpty();

                        // Finish recovery and collect unfinished prepared transactions.
                        launcher.mUnfinished = applier.finish();

                        // Check if any exceptions were caught by recovery worker threads.
                        checkClosedCause();

                        // Avoid re-using transaction ids used by recovery.
                        txnId = applier.highestTxnId(txnId);

                        // New redo logs begin with identifiers one higher than last scanned.
                        var log = new RedoLog(launcher, replayLog, mTxnContexts[0]);
                        mRedoWriter = log;

                        if (doCheckpoint) {
                            // Do this early for checkpoint to store correct transaction id.
                            resetTransactionContexts(txnId);
                            txnId = -1;

                            try {
                                forceCheckpoint();
                            } catch (Throwable e) {
                                // Delete the newly created redo log files.
                                log.initialCheckpointFailed(e);
                                throw e;
                            }

                            // Only cleanup after successful checkpoint.
                            deleteReverseOrder(redoFiles);
                        }
                    }

                    recoveryComplete(recoveryStart);
                }
            }

            if (txnId >= 0) {
                resetTransactionContexts(txnId);
            }
        } catch (Throwable e) {
            // Close, but don't double report the exception since construction never finished.
            closeQuietly(this);

            // Clean up the mess by deleting the lock file if it was just created.
            deleteLockFile(attemptCreate, null);

            throw e;
        }
    }

    private String lockFilePath() {
        return mBaseFile.getPath() + LOCK_FILE_SUFFIX;
    }

    private IOException deleteLockFile(LockedFile file, IOException ex) {
        if (file != null) {
            ex = file.delete(lockFilePath(), ex);
        }
        return ex;
    }

    /**
     * Post construction, allow additional threads access to the database.
     */
    @SuppressWarnings("unchecked")
    private void finishInit(Launcher launcher) throws IOException {
        if (mCheckpointer == null) {
            // Nothing is durable and nothing to ever clean up.
            return;
        }

        // Register objects to automatically shutdown.
        mCheckpointer.register(new RedoClose(this));
        mCheckpointer.register(mTempFileManager);

        if (mRedoWriter instanceof ReplWriter) {
            // Need to do this after mRedoWriter is assigned, ensuring that trees are opened as
            // BTree.Repl instances.
            applyCachePrimer(launcher);
        }

        if (launcher.mCachePriming && mPageDb.isDurable() && !mReadOnly) {
            mCheckpointer.register(new ShutdownPrimer(this));
        }

        BTree trashed = openNextTrashedTree(null);

        if (trashed != null) {
            Runner.start("IndexDeletion", new Deletion(trashed, true, mEventListener));
        }

        mCheckpointer.start(false);

        emptyLingeringTrash(null); // only for non-replicated transactions

        if (!(mRedoWriter instanceof ReplController)) {
            LHashTable.Obj<LocalTransaction> unfinished = launcher.mUnfinished;
            if (unfinished != null) {
                Runner.start(() -> invokeRecoveryHandler(unfinished, mRedoWriter));
                launcher.mUnfinished = null;
            }
        } else {
            // Start replication and recovery.
            var controller = (ReplController) mRedoWriter;

            if (mEventListener != null) {
                mEventListener.notify(EventType.RECOVERY_PROGRESS, "Starting replication recovery");
            }

            try {
                controller.ready(launcher.mReplInitialPostion, launcher.mReplInitialTxnId);
            } catch (Throwable e) {
                closeQuietly(this, e);
                throw e;
            }

            recoveryComplete(launcher.mReplRecoveryStartNanos);
        }
    }

    /**
     * Called by ReplManager.
     */
    @Override
    long writeControlMessage(byte[] message) throws IOException {
        // Commit lock must be held to prevent a checkpoint from starting. If the control
        // message fails to be applied, panic the database. If the database is kept open after
        // a failure and then a checkpoint completes, the control message would be dropped.
        // Normal transactional operations aren't so sensitive, because they have an undo log.
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            RedoWriter redo = txnRedoWriter();
            TransactionContext context = anyTransactionContext();
            long commitPos = context.redoControl(redo, message);

            // Waiting for confirmation with the shared lock held isn't ideal, but control
            // messages aren't that frequent.
            redo.txnCommitSync(commitPos);

            try {
                ((ReplController) mRedoWriter).mRepl.controlMessageReceived(commitPos, message);
            } catch (Throwable e) {
                // Panic.
                closeQuietly(this, e);
                throw e;
            }

            return commitPos;
        } finally {
            shared.release();
        }
    }

    /**
     * @param txns must not be null
     * @param redo RedoWriter assigned to each transaction; pass null to perform partial
     * rollback but don't invoke the handler
     */
    void invokeRecoveryHandler(LHashTable.Obj<LocalTransaction> txns, RedoWriter redo) {
        // Note: Even if the recovery handler isn't called, looking it up performs validation.

        txns.traverse(entry -> {
            LocalTransaction txn = entry.value;

            try {
                UndoLog.RTP rtp = txn.rollbackForRecovery
                    (redo, mDurabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);

                int handlerId = rtp.handlerId;
                byte[] message = rtp.message;

                PrepareHandler recovery = findPrepareRecoveryHandler(handlerId);

                if (redo != null) {
                    try {
                        if (rtp.commit) {
                            recovery.prepareCommit(txn, message);
                        } else {
                            recovery.prepare(txn, message);
                        }
                    } catch (UnmodifiableReplicaException e) {
                        // Ensure that it can be handed off again.
                        txn.reset(e);
                    }
                }
            } catch (Throwable e) {
                if (!isClosed()) {
                    e = new CorruptDatabaseException("Malformed prepared transaction: " + txn, e);

                    EventListener listener = mEventListener;
                    if (listener == null) {
                        uncaught(e);
                    } else {
                        listener.notify
                            (EventType.RECOVERY_HANDLER_UNCAUGHT,
                             "Uncaught exception when recovering a prepared transaction: %1$s", e);
                    }
                }
            }

            return true;
        });
    }

    private void applyCachePrimer(Launcher launcher) {
        if (mPageDb.isDurable()) {
            File primer = primerFile();
            try {
                if (launcher.mCachePriming && primer.exists()) {
                    if (mEventListener != null) {
                        mEventListener.notify(EventType.RECOVERY_CACHE_PRIMING,
                                              "Cache priming");
                    }
                    FileInputStream fin;
                    try {
                        fin = new FileInputStream(primer);
                        try (var bin = new BufferedInputStream(fin)) {
                            applyCachePrimer(bin);
                        } catch (IOException e) {
                            fin.close();
                        }
                    } catch (IOException e) {
                    }
                }
            } finally {
                if (!mReadOnly) {
                    primer.delete();
                }
            }
        }
    }

    static class ShutdownPrimer extends ShutdownHook.Weak<LocalDatabase> {
        ShutdownPrimer(LocalDatabase db) {
            super(db);
        }

        @Override
        void doShutdown(LocalDatabase db) {
            if (db.mReadOnly) {
                return;
            }

            File primer = db.primerFile();

            FileOutputStream fout;
            try {
                fout = new FileOutputStream(primer);
                try {
                    try (var bout = new BufferedOutputStream(fout)) {
                        db.createCachePrimer(bout);
                    }
                } catch (IOException e) {
                    fout.close();
                    primer.delete();
                }
            } catch (IOException e) {
            }
        }
    }

    File primerFile() {
        return new File(mBaseFile.getPath() + PRIMER_FILE_SUFFIX);
    }

    private void recoveryComplete(long recoveryStart) {
        if (mEventListener != null) {
            double duration = (System.nanoTime() - recoveryStart) / 1_000_000_000.0;
            mEventListener.notify(EventType.RECOVERY_COMPLETE,
                                  "Recovery completed in %1$1.3f seconds",
                                  duration, TimeUnit.SECONDS);
        }
    }

    private void deleteRedoLogFiles() throws IOException {
        if (mBaseFile != null && !mReadOnly) {
            deleteNumberedFiles(mBaseFile, REDO_FILE_SUFFIX);
        }
    }

    private boolean hasRedoLogFiles() throws IOException {
        return mBaseFile != null
            && !findNumberedFiles(mBaseFile, REDO_FILE_SUFFIX, 0, Long.MAX_VALUE).isEmpty();
    }

    @Override
    public Index findIndex(byte[] name) throws IOException {
        return openTree(name, IX_FIND);
    }

    @Override
    public Index openIndex(byte[] name) throws IOException {
        return openTree(name, IX_CREATE);
    }

    @Override
    public Index indexById(long id) throws IOException {
        return indexById(null, id);
    }

    private Index indexById(Transaction txn, long id) throws IOException {
        if (Tree.isInternal(id)) {
            throw new IllegalArgumentException("Invalid id: " + id);
        }

        Index index = lookupIndexById(id);

        if (index != null) {
            return index;
        }

        var idKey = new byte[9];
        idKey[0] = RK_INDEX_ID;
        encodeLongBE(idKey, 1, id);

        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            byte[] name;

            if (txn != null) {
                name = mRegistryKeyMap.load(txn, idKey);
            } else {
                // Lookup name with exclusive lock, to prevent races with concurrent index
                // creation. If a replicated operation which requires the newly created index
                // merely acquired a shared lock, then it might not find the index at all.

                Locker locker = mLockManager.localLocker();
                while (true) {
                    if (locker.doTryLockExclusive(mRegistryKeyMap.id(), idKey, 0).isHeld()) {
                        break;
                    }
                    // Release locks and retry, avoiding possible deadlock if the checkpointer
                    // has suspended replica decoding in the middle of tree creation.
                    shared.release();
                    Thread.yield();
                    mCommitLock.acquireShared(shared);
                }

                try {
                    name = mRegistryKeyMap.load(Transaction.BOGUS, idKey);
                } finally {
                    locker.doUnlock();
                }
            }

            if (name == null) {
                checkClosed();
                return null;
            }

            var treeIdBytes = new byte[8];
            encodeLongBE(treeIdBytes, 0, id);            

            index = openTree(txn, treeIdBytes, name, IX_FIND);
        } catch (Throwable e) {
            rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        } finally {
            shared.release();
        }

        if (index == null) {
            // Registry needs to be repaired to fix this.
            throw new DatabaseException("Unable to find index in registry");
        }

        return index;
    }

    /**
     * @return null if index is not open
     */
    private Tree lookupIndexById(long id) {
        mOpenTreesLatch.acquireShared();
        try {
            LHashTable.ObjEntry<TreeRef> entry = mOpenTreesById.get(id);
            return entry == null ? null : entry.value.get();
        } finally {
            mOpenTreesLatch.releaseShared();
        }
    }

    /**
     * Allows access to internal indexes which can use the redo log.
     */
    Index anyIndexById(long id) throws IOException {
        return anyIndexById(null, id);
    }

    /**
     * Allows access to internal indexes which can use the redo log.
     */
    Index anyIndexById(Transaction txn, long id) throws IOException {
        return Tree.isInternal(id) ? internalIndex(id) : indexById(txn, id);
    }

    /**
     * @param id must be an internal index
     */
    private Index internalIndex(long id) throws IOException {
        if (id == Tree.REGISTRY_KEY_MAP_ID) {
            return mRegistryKeyMap;
        } else if (id == Tree.FRAGMENTED_TRASH_ID) {
            return fragmentedTrash();
        } else if (id == Tree.PREPARED_TXNS_ID) {
            return preparedTxns();
        } else {
            throw new CorruptDatabaseException("Internal index referenced by redo log: " + id);
        }
    }

    @Override
    public void renameIndex(Index index, byte[] newName) throws IOException {
        renameIndex(index, newName.clone(), 0);
    }

    /**
     * @param newName not cloned
     * @param redoTxnId non-zero if rename is performed by recovery
     */
    void renameIndex(final Index index, final byte[] newName, final long redoTxnId)
        throws IOException
    {
        // Design note: Rename is a Database method instead of an Index method because it
        // offers an extra degree of safety. It's too easy to call rename and pass a byte[] by
        // an accident when something like remove was desired instead. Requiring access to the
        // Database instance makes this operation a bit more of a hassle to use, which is
        // desirable. Rename is not expected to be a common operation.

        accessTree(index).rename(newName, redoTxnId);
    }

    /**
     * @param newName not cloned
     * @param redoTxnId non-zero if rename is performed by recovery
     */
    void renameBTree(final BTree tree, final byte[] newName, final long redoTxnId)
        throws IOException
    {
        final byte[] idKey, trashIdKey;
        final byte[] oldName, oldNameKey;
        final byte[] newNameKey;

        final LocalTransaction txn;

        final Node root = tree.mRoot;
        root.acquireExclusive();
        try {
            if (root.mPage == p_closedTreePage()) {
                throw new ClosedIndexException();
            }

            if (Tree.isInternal(tree.mId)) {
                throw new IllegalStateException("Cannot rename an internal index");
            }

            oldName = tree.mName;

            if (oldName == null) {
                throw new IllegalStateException("Cannot rename a temporary index");
            }

            if (Arrays.equals(oldName, newName)) {
                return;
            }

            idKey = newKey(RK_INDEX_ID, tree.mIdBytes);
            trashIdKey = newKey(RK_TRASH_ID, tree.mIdBytes);
            oldNameKey = newKey(RK_INDEX_NAME, oldName);
            newNameKey = newKey(RK_INDEX_NAME, newName);

            txn = newNoRedoTransaction(redoTxnId);
            try {
                txn.lockTimeout(-1, null);
                txn.doLockExclusive(mRegistryKeyMap.mId, idKey);
                txn.doLockExclusive(mRegistryKeyMap.mId, trashIdKey);
                // Lock in a consistent order, avoiding deadlocks.
                if (Arrays.compareUnsigned(oldNameKey, newNameKey) <= 0) {
                    txn.doLockExclusive(mRegistryKeyMap.mId, oldNameKey);
                    txn.doLockExclusive(mRegistryKeyMap.mId, newNameKey);
                } else {
                    txn.doLockExclusive(mRegistryKeyMap.mId, newNameKey);
                    txn.doLockExclusive(mRegistryKeyMap.mId, oldNameKey);
                }
            } catch (Throwable e) {
                txn.reset();
                throw e;
            }
        } finally {
            // Can release now that registry entries are locked. Those locks will prevent
            // concurrent renames of the same index.
            root.releaseExclusive();
        }

        try {
            Cursor c = mRegistryKeyMap.newCursor(txn);
            try {
                c.autoload(false);

                c.find(trashIdKey);
                if (c.value() != null) {
                    throw new IllegalStateException("Index is deleted");
                }

                c.find(newNameKey);
                if (c.value() != null) {
                    throw new IllegalStateException("New name is used by another index");
                }

                c.store(tree.mIdBytes);
            } finally {
                c.reset();
            }

            LongConsumer finishTask = pos -> {
                try {
                    if (pos >= 0) {
                        txn.durabilityMode(DurabilityMode.NO_REDO);
                        mRegistryKeyMap.delete(txn, oldNameKey);
                        mRegistryKeyMap.store(txn, idKey, newName);

                        mOpenTreesLatch.acquireExclusive();
                        try {
                            txn.commit();

                            tree.mName = newName;
                            mOpenTrees.put(newName, mOpenTrees.remove(oldName));
                        } finally {
                            mOpenTreesLatch.releaseExclusive();
                        }
                    }
                } catch (Throwable e) {
                    rethrow(e);
                } finally {
                    txn.reset();
                }
            };

            if (redoTxnId == 0 && txn.mRedo != null) {
                txn.durabilityMode(alwaysRedo(mDurabilityMode));

                long commitPos;
                CommitLock.Shared shared = mCommitLock.acquireShared();
                try {
                    txn.check();
                    commitPos = txn.mContext.redoRenameIndexCommitFinal
                        (txn.mRedo, txn.txnId(), tree.mId, newName, txn.durabilityMode());
                } finally {
                    shared.release();
                }

                if (commitPos != 0) {
                    // Must wait for durability confirmation before performing actions below
                    // which cannot be easily rolled back. No global latches or locks are held
                    // while waiting.
                    try {
                        txn.mRedo.txnCommitSync(commitPos);
                    } catch (ConfirmationInterruptedException e) {
                        // Wait for confirmation in the background.
                        ((ReplWriter) txn.mRedo).mReplWriter.uponCommit(commitPos, finishTask);
                        throw e;
                    }
                }
            }

            finishTask.accept(0);
        } catch (Throwable e) {
            if (!(e instanceof ConfirmationInterruptedException)) {
                txn.reset();
            }
            if (e instanceof IllegalStateException) {
                throw e;
            }
            rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        }
    }

    private Tree accessTree(Index index) {
        try {
            Tree tree;
            if ((tree = ((Tree) index)).isMemberOf(this)) {
                return tree;
            }
        } catch (ClassCastException e) {
            // Cast and catch an exception instead of calling instanceof to cause a
            // NullPointerException to be thrown if index is null.
        }
        throw new IllegalStateException("Index belongs to a different database");
    }

    @Override
    public Runnable deleteIndex(Index index) throws IOException {
        // Design note: This is a Database method instead of an Index method because it offers
        // an extra degree of safety. See notes in renameIndex.
        return accessTree(index).drop(false);
    }

    /**
     * Returns a deletion task for a tree which just moved to the trash.
     */
    Runnable replicaDeleteTree(long treeId) throws IOException {
        var treeIdBytes = new byte[8];
        encodeLongBE(treeIdBytes, 0, treeId);

        BTree trashed = openTrashedTree(treeIdBytes, false);

        return new Deletion(trashed, false, null);
    }

    /**
     * Called by BTree.drop with root node latch held exclusively.
     *
     * @param shared commit lock held shared; always released by this method
     */
    Runnable deleteTree(BTree tree, CommitLock.Shared shared) throws IOException {
        try {
            if (!(tree instanceof BTree.Temp) && !moveToTrash(tree.mId, tree.mIdBytes)) {
                // Handle concurrent delete attempt.
                throw new ClosedIndexException();
            }
        } finally {
            // Always release before calling close, which might require an exclusive lock.
            shared.release();
        }

        Node root = tree.close(true, true);
        if (root == null) {
            // Handle concurrent close attempt.
            throw new ClosedIndexException();
        }

        BTree trashed = newBTreeInstance(tree.mId, tree.mIdBytes, tree.mName, root);

        return new Deletion(trashed, false, null);
    }

    /**
     * Quickly delete an empty temporary tree, which has no active threads and cursors.
     */
    void quickDeleteTemporaryTree(BTree tree) throws IOException {
        mOpenTreesLatch.acquireExclusive();
        try {
            TreeRef ref = mOpenTreesById.removeValue(tree.mId);
            if (ref == null || ref.get() != tree) {
                // BTree is likely being closed by a concurrent database close.
                return;
            }
            ref.clear();
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }

        Node root = tree.mRoot;
        byte[] trashIdKey = newKey(RK_TRASH_ID, tree.mIdBytes);

        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            root.acquireExclusive();

            if (!root.hasKeys() && root.mPage != p_closedTreePage()) {
                // Delete and remove from trash.
                prepareToDelete(root);
                deleteNode(root);
                mRegistryKeyMap.delete(Transaction.BOGUS, trashIdKey);
                mRegistry.delete(Transaction.BOGUS, tree.mIdBytes);
                return;
            }

            root.releaseExclusive();
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        } finally {
            shared.release();
        }

        // BTree isn't truly empty -- it might be composed of many empty leaf nodes.
        tree.deleteAll();
        removeFromTrash(tree, root);
    }

    /**
     * Returns the view of trashed trees.
     */
    private View trash() {
        return mRegistryKeyMap.viewPrefix(new byte[] {RK_TRASH_ID}, 1);
    }

    private void tagTrashedTrees() throws IOException {
        // Tag all the entries that should be deleted automatically. Entries created later will
        // have a different prefix, and so they'll be ignored.
        try (Cursor c = trash().newCursor(Transaction.BOGUS)) {
            for (c.first(); c.key() != null; c.next()) {
                byte[] name = c.value();
                if (name.length != 0) {
                    name[0] |= 0x80;
                    c.store(name);
                }
            }
        }
    }

    /**
     * @param lastIdBytes null to start with first
     * @return null if none available
     */
    private BTree openNextTrashedTree(byte[] lastIdBytes) throws IOException {
        return openTrashedTree(lastIdBytes, true);
    }

    /**
     * @param idBytes null to start with first
     * @param next true to find tree with next higher id
     * @return null if not found
     */
    private BTree openTrashedTree(byte[] idBytes, boolean next) throws IOException {
        byte[] treeIdBytes, name, rootIdBytes;

        try (Cursor c = trash().newCursor(Transaction.BOGUS)) {
            if (idBytes == null) {
                c.first();
            } else if (next) {
                c.findGt(idBytes);
            } else {
                c.find(idBytes);
            }

            while (true) {
                treeIdBytes = c.key();

                if (treeIdBytes == null) {
                    return null;
                }

                rootIdBytes = mRegistry.load(Transaction.BOGUS, treeIdBytes);

                if (rootIdBytes == null) {
                    // Clear out bogus entry in the trash.
                    c.store(null);
                } else {
                    name = c.value();
                    if (name[0] < 0 || (idBytes != null && next == false)) {
                        // Found a tagged entry, or found the requested entry.
                        break;
                    }
                }

                if (next) {
                    c.next();
                } else {
                    return null;
                }
            }
        }

        long rootId = rootIdBytes.length == 0 ? 0 : decodeLongLE(rootIdBytes, 0);

        if ((name[0] & 0x7f) == 0) {
            name = null;
        } else {
            // Trim off the tag byte.
            var actual = new byte[name.length - 1];
            System.arraycopy(name, 1, actual, 0, actual.length);
            name = actual;
        }

        long treeId = decodeLongBE(treeIdBytes, 0);

        return newBTreeInstance(treeId, treeIdBytes, name, loadTreeRoot(treeId, rootId));
    }

    private class Deletion implements Runnable {
        private BTree mTrashed;
        private final boolean mResumed;
        private final EventListener mListener;

        Deletion(BTree trashed, boolean resumed, EventListener listener) {
            mTrashed = trashed;
            mResumed = resumed;
            mListener = listener;
        }

        @Override
        public synchronized void run() {
            while (mTrashed != null) {
                delete();
            }
        }

        private void delete() {
            if (mListener != null) {
                mListener.notify(EventType.DELETION_BEGIN,
                                 "Index deletion " + (mResumed ? "resumed" : "begin") +
                                 ": %1$d, name: %2$s",
                                 mTrashed.id(), mTrashed.nameString());
            }

            final byte[] idBytes = mTrashed.mIdBytes;

            try {
                long start = System.nanoTime();

                if (mTrashed.deleteAll()) {
                    Node root = mTrashed.close(true, false);
                    removeFromTrash(mTrashed, root);
                } else {
                    // Database is closed.
                    return;
                }

                if (mListener != null) {
                    double duration = (System.nanoTime() - start) / 1_000_000_000.0;
                    mListener.notify(EventType.DELETION_COMPLETE,
                                     "Index deletion complete: %1$d, name: %2$s, " +
                                     "duration: %3$1.3f seconds",
                                     mTrashed.id(), mTrashed.nameString(), duration);
                }
            } catch (IOException e) {
                if (!isClosed() && mListener != null) {
                    mListener.notify
                        (EventType.DELETION_FAILED,
                         "Index deletion failed: %1$d, name: %2$s, exception: %3$s",
                         mTrashed.id(), mTrashed.nameString(), rootCause(e));
                }
                closeQuietly(mTrashed);
                return;
            } finally {
                mTrashed = null;
            }

            if (mResumed) {
                try {
                    mTrashed = openNextTrashedTree(idBytes);
                } catch (IOException e) {
                    if (!isClosed() && mListener != null) {
                        mListener.notify
                            (EventType.DELETION_FAILED,
                             "Unable to resume deletion: %1$s", rootCause(e));
                    }
                    return;
                }
            }
        }
    }

    @Override
    public BTree newTemporaryIndex() throws IOException {
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            return newTemporaryTree(false);
        } finally {
            shared.release();
        }
    }

    /**
     * Caller must hold commit lock. Pass true to preallocate a dirty root node for the tree,
     * which will be held exclusive. Caller is then responsible for initializing it
     */
    BTree newTemporaryTree(boolean preallocate) throws IOException {
        checkClosed();

        // Cleanup before opening more trees.
        cleanupUnreferencedTrees();

        long treeId;
        var treeIdBytes = new byte[8];

        long rootId;
        byte[] rootIdBytes;

        if (preallocate) {
            rootId = mPageDb.allocPage();
            rootIdBytes = new byte[8];
            encodeLongLE(rootIdBytes, 0, rootId);
        } else {
            rootId = 0;
            rootIdBytes = EMPTY_BYTES;
        }

        try {
            do {
                treeId = nextTreeId(RK_NEXT_TEMP_ID);
                encodeLongBE(treeIdBytes, 0, treeId);
            } while (!mRegistry.insert(Transaction.BOGUS, treeIdBytes, rootIdBytes));

            // Register temporary index as trash, unreplicated.
            Transaction createTxn = newNoRedoTransaction();
            try {
                createTxn.lockTimeout(-1, null);
                byte[] trashIdKey = newKey(RK_TRASH_ID, treeIdBytes);
                if (!mRegistryKeyMap.insert(createTxn, trashIdKey, new byte[1])) {
                    throw new DatabaseException("Unable to register temporary index");
                }
                createTxn.commit();
            } finally {
                createTxn.reset();
            }

            Node root;
            if (rootId != 0) {
                root = allocLatchedNode(NodeGroup.MODE_UNEVICTABLE);
                root.id(rootId);
                try {
                    /*P*/ // [|
                    /*P*/ // if (mFullyMapped) {
                    /*P*/ //     root.mPage = mPageDb.dirtyPage(rootId);
                    /*P*/ // }
                    /*P*/ // ]
                    root.mGroup.addDirty(root, mCommitState);
                } catch (Throwable e) {
                    root.releaseExclusive();
                    throw e;
                }
            } else {
                root = loadTreeRoot(treeId, 0);
            }

            try {
                var tree = new BTree.Temp(this, treeId, treeIdBytes, root);
                var treeRef = new TreeRef(tree, tree, mOpenTreesRefQueue);

                mOpenTreesLatch.acquireExclusive();
                try {
                    mOpenTreesById.insert(treeId).value = treeRef;
                } finally {
                    mOpenTreesLatch.releaseExclusive();
                }

                return tree;
            } catch (Throwable e) {
                if (rootId != 0) {
                    root.releaseExclusive();
                }
                throw e;
            }
        } catch (Throwable e) {
            try {
                mRegistry.delete(Transaction.BOGUS, treeIdBytes);
            } catch (Throwable e2) {
                // Panic.
                throw closeOnFailure(this, e);
            }
            if (rootId != 0) {
                try {
                    mPageDb.recyclePage(rootId);
                } catch (Throwable e2) {
                    Utils.suppress(e, e2);
                }
            }
            throw e;
        }
    }

    @Override
    public View indexRegistryByName() throws IOException {
        return mRegistryKeyMap.viewPrefix(new byte[] {RK_INDEX_NAME}, 1).viewUnmodifiable();
    }

    @Override
    public View indexRegistryById() throws IOException {
        return mRegistryKeyMap.viewPrefix(new byte[] {RK_INDEX_ID}, 1).viewUnmodifiable();
    }

    @Override
    public Transaction newTransaction() {
        return doNewTransaction(mDurabilityMode);
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return doNewTransaction(durabilityMode == null ? mDurabilityMode : durabilityMode);
    }

    private LocalTransaction doNewTransaction(DurabilityMode durabilityMode) {
        RedoWriter redo = txnRedoWriter();
        return new LocalTransaction
            (this, redo, durabilityMode, LockMode.UPGRADABLE_READ, mDefaultLockTimeoutNanos);
    }

    LocalTransaction newAlwaysRedoTransaction() {
        return doNewTransaction(alwaysRedo(mDurabilityMode));
    }

    /**
     * Convenience method which returns a transaction intended for locking and undo. Caller can
     * make modifications, but they won't go to the redo log.
     */
    private LocalTransaction newNoRedoTransaction() {
        return doNewTransaction(DurabilityMode.NO_REDO);
    }

    /**
     * Convenience method which returns a transaction intended for locking and undo. Caller can
     * make modifications, but they won't go to the redo log.
     *
     * @param redoTxnId non-zero if operation is performed by recovery
     */
    private LocalTransaction newNoRedoTransaction(long redoTxnId) {
        return redoTxnId == 0 ? newNoRedoTransaction() :
            new LocalTransaction(this, redoTxnId, LockMode.UPGRADABLE_READ,
                                 mDefaultLockTimeoutNanos);
    }

    /**
     * Returns a transaction which should be briefly used and reset.
     */
    LocalTransaction threadLocalTransaction(DurabilityMode durabilityMode) {
        SoftReference<LocalTransaction> txnRef = mLocalTransaction.get();
        LocalTransaction txn;
        if (txnRef == null || (txn = txnRef.get()) == null) {
            txn = doNewTransaction(durabilityMode);
            mLocalTransaction.set(new SoftReference<>(txn));
        } else {
            txn.mRedo = txnRedoWriter();
            txn.mDurabilityMode = durabilityMode;
            txn.mLockMode = LockMode.UPGRADABLE_READ;
            txn.mLockTimeoutNanos = mDefaultLockTimeoutNanos;
        }
        return txn;
    }

    void removeThreadLocalTransaction() {
        mLocalTransaction.remove();
    }

    /**
     * Returns a RedoWriter suitable for transactions to write into.
     */
    RedoWriter txnRedoWriter() {
        RedoWriter redo = mRedoWriter;
        if (redo != null) {
            redo = redo.txnRedoWriter();
        }
        return redo;
    }

    private void resetTransactionContexts(long txnId) {
        for (TransactionContext txnContext : mTxnContexts) {
            txnContext.resetTransactionId(txnId++);
        }
    }

    /**
     * Used by auto-commit operations that don't have an explicit transaction.
     */
    TransactionContext anyTransactionContext() {
        return selectTransactionContext(ThreadLocalRandom.current().nextInt());
    }

    /**
     * Called by transaction constructor after hash code has been assigned.
     */
    TransactionContext selectTransactionContext(LocalTransaction txn) {
        return selectTransactionContext(txn.hashCode());
    }

    private TransactionContext selectTransactionContext(int num) {
        return mTxnContexts[(num & 0x7fffffff) % mTxnContexts.length];
    }

    /**
     * Calls discardRedoWriter on all TransactionContexts.
     */
    void discardRedoWriter(RedoWriter expect) {
        for (TransactionContext context : mTxnContexts) {
            context.discardRedoWriter(expect);
        }
    }

    /**
     * Copies a reference to all active UndoLogs into the given collection.
     */
    void gatherUndoLogs(Collection<? super UndoLog> to) {
        for (TransactionContext context : mTxnContexts) {
            context.gatherUndoLogs(to);
        }
    }

    @Override
    public CustomHandler customWriter(String name) throws IOException {
        return (CustomHandler) findOrCreateWriter(name, RK_CUSTOM_NAME, mCustomHandlers);
    }

    /**
     * @return the recovery handler instance
     * @throws CorruptDatabaseException if name isn't found
     * @throws IllegalStateException if not installed
     */
    CustomHandler findCustomRecoveryHandler(int handlerId) throws IOException {
        return findRecoveryHandler(handlerId, RK_CUSTOM_ID,
                                   mCustomHandlers, mCustomHandlersById);
    }

    @Override
    public PrepareHandler prepareWriter(String name) throws IOException {
        return (PrepareHandler) findOrCreateWriter(name, RK_PREPARE_NAME, mPrepareHandlers);
    }

    /**
     * @return the recovery handler instance
     * @throws CorruptDatabaseException if name isn't found
     * @throws IllegalStateException if not installed
     */
    PrepareHandler findPrepareRecoveryHandler(int handlerId) throws IOException {
        return findRecoveryHandler(handlerId, RK_PREPARE_ID,
                                   mPrepareHandlers, mPrepareHandlersById);
    }

    /**
     * @param rkIdPrefix RK_CUSTOM_ID or RK_PREPARE_ID
     * @return null if not found
     */
    String findHandlerName(int handlerId, byte rkIdPrefix) throws IOException {
        byte[] idKey = newKey(rkIdPrefix, handlerId);
        byte[] nameBytes;
        try {
            nameBytes = mRegistryKeyMap.load(null, idKey);
        } catch (LockTimeoutException e) {
            // Try again with infinite timeout.
            nameBytes = null;
        }

        if (nameBytes == null) {
            // Possible race condition with creation of the handler entry by another
            // transaction during recovery. Try again with an upgradable lock, which will wait
            // for the entry lock.
            Transaction txn = newNoRedoTransaction();
            try {
                txn.lockTimeout(-1, null);
                nameBytes = mRegistryKeyMap.load(txn, idKey);
            } finally {
                txn.reset();
            }
        }

        return nameBytes == null ? null : new String(nameBytes, StandardCharsets.UTF_8);
    }

    /**
     * @param rkNamePrefix RK_CUSTOM_NAME or RK_PREPARE_NAME
     */
    @SuppressWarnings("unchecked")
    private <H extends Handler> HandlerWriter findOrCreateWriter(String name, byte rkNamePrefix,
                                                                 Map<String, H> handlers)
        throws IOException
    {
        if (handlers == null) {
            throw new IllegalStateException("Recovery handler not installed: " + name);
        }

        Handler handler;
        synchronized (handlers) {
            // Can probably cheat and not synchronize access, but the behavior is undefined.
            handler = handlers.get(name);
        }

        if (handler instanceof HandlerWriter) {
            return (HandlerWriter) handler;
        }

        int handlerId = findOrCreateHandlerId(name, rkNamePrefix, handlers);

        switch (rkNamePrefix) {
        case RK_CUSTOM_NAME:
            handler = new CustomWriter(this, handlerId, (CustomHandler) handler);
            break;
        case RK_PREPARE_NAME:
            handler = new PrepareWriter(this, handlerId, (PrepareHandler) handler);
            break;
        default:
            throw new AssertionError();
        }

        synchronized (handlers) {
            Handler existing = handlers.get(name);
            if (existing instanceof HandlerWriter) {
                return (HandlerWriter) existing;
            }
            handlers.put(name, (H) handler);
        }

        return (HandlerWriter) handler;
    }

    /**
     * @param rkNamePrefix RK_CUSTOM_NAME or RK_PREPARE_NAME
     * @return handlerId
     */
    private int findOrCreateHandlerId(String name, byte rkNamePrefix,
                                      Map<String, ? extends Handler> handlers)
        throws IOException
    {
        final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        final byte[] nameKey = newKey(rkNamePrefix, nameBytes);

        byte[] idBytes = mRegistryKeyMap.load(null, nameKey);

        if (idBytes == null) define: {
            // Throws IllegalStateException if not found.
            findRecoveryHandler(name, handlers);

            // Assume first use, so define a new handler id.

            final LocalTransaction txn = newAlwaysRedoTransaction();
            try (Cursor nameCursor = mRegistryKeyMap.newCursor(txn)) {
                // Must wait for the transaction to fully commit, or else the vended handler id
                // is invalid if the transaction is pending and rolls back.
                txn.durabilityMode(DurabilityMode.SYNC);

                nameCursor.find(nameKey);
                idBytes = nameCursor.value();

                if (idBytes != null) {
                    // Found it on the second try.
                    break define;
                }

                // Assumes that corresponding _ID prefix is one more than _NAME prefix.
                final byte rkIdPrefix = (byte) (rkNamePrefix + 1);

                final View byId = mRegistryKeyMap.viewPrefix(new byte[] {rkIdPrefix}, 1);

                try (Cursor idCursor = byId.newCursor(txn)) {
                    idCursor.autoload(false);
                    while (true) {
                        idCursor.last();
                        int lastId = idCursor.key() == null ? 0 : decodeIntBE(idCursor.key(), 0);
                        idBytes = new byte[4];
                        encodeIntBE(idBytes, 0, lastId + 1);
                        idCursor.findNearby(idBytes);
                        if (idCursor.value() == null) {
                            idCursor.store(nameBytes);
                            break;
                        }
                    }
                }

                nameCursor.commit(idBytes);
            } finally {
                txn.reset();
            }
        }

        return decodeIntBE(idBytes, 0);
    }

    /**
     * @param rkIdPrefix RK_CUSTOM_ID or RK_PREPARE_ID
     * @return the recovery handler instance
     * @throws CorruptDatabaseException if name isn't found
     * @throws IllegalStateException if not installed
     */
    private <H extends Handler> H findRecoveryHandler(int handlerId, byte rkIdPrefix,
                                                      Map<String, H> handlers,
                                                      LHashTable.Obj<H> handlersById)
        throws IOException
    {
        long scrambledId = scramble(handlerId);

        if (handlersById != null) {
            H handler;
            synchronized (handlersById) {
                handler = handlersById.getValue(scrambledId);
            }
            if (handler != null) {
                return handler;
            }
        }

        String name = findHandlerName(handlerId, rkIdPrefix);

        if (name == null) {
            String type;
            switch (rkIdPrefix) {
            case RK_CUSTOM_ID: type = "custom"; break;
            case RK_PREPARE_ID: type = "prepare"; break;
            default: type = String.valueOf(rkIdPrefix); break;
            }

            throw new CorruptDatabaseException
                ("Unable to find " + type + " handler name for id " + handlerId);
        }

        H handler = findRecoveryHandler(name, handlers);

        synchronized (handlersById) {
            handlersById.insert(scrambledId).value = handler;
        }

        return handler;
    }

    /**
     * @return the recovery handler instance
     * @throws IllegalStateException if not installed
     */
    @SuppressWarnings("unchecked")
    private static <H extends Handler> H findRecoveryHandler(String name, Map<String, H> handlers) {
        if (handlers != null) {
            H handler;
            synchronized (handlers) {
                // Can probably cheat and not synchronize access, but the behavior is undefined.
                handler = handlers.get(name);
            }
            if (handler instanceof HandlerWriter) {
                return (H) ((HandlerWriter) handler).mRecoveryHandler;
            } else if (handler != null) {
                return handler;
            }
        }

        throw new IllegalStateException("Recovery handler not installed: " + name);
    }

    @Override
    public long preallocate(long bytes) throws IOException {
        if (!isClosed() && mPageDb.isDurable()) {
            int pageSize = mPageSize;
            long pageCount = (bytes + pageSize - 1) / pageSize;
            if (pageCount > 0) {
                pageCount = mPageDb.allocatePages(pageCount);
                if (pageCount > 0) {
                    try {
                        forceCheckpoint();
                    } catch (Throwable e) {
                        rethrowIfRecoverable(e);
                        closeQuietly(this, e);
                        throw e;
                    }
                }
                return pageCount * pageSize;
            }
        }
        return 0;
    }

    @Override
    public Sorter newSorter() {
        return new ParallelSorter(this, Runner.current());
    }

    @Override
    public void capacityLimit(long bytes) {
        mPageDb.pageLimit(bytes < 0 ? -1 : (bytes / mPageSize));
    }

    @Override
    public long capacityLimit() {
        long pageLimit = mPageDb.pageLimit();
        return pageLimit < 0 ? -1 : (pageLimit * mPageSize);
    }

    @Override
    public void capacityLimitOverride(long bytes) {
        mPageDb.pageLimitOverride(bytes < 0 ? -1 : (bytes / mPageSize));
    }

    @Override
    public Snapshot beginSnapshot() throws IOException {
        if (!(mPageDb.isDurable())) {
            throw new UnsupportedOperationException("Snapshot only allowed for durable databases");
        }
        checkClosed();
        var pageDb = (DurablePageDb) mPageDb;
        return pageDb.beginSnapshot(this);
    }

    /**
     * Restore from a {@link #beginSnapshot snapshot}, into the data files defined by the given
     * configuration. All existing data and redo log files at the snapshot destination are
     * deleted before the restore begins.
     *
     * @param in snapshot source; does not require extra buffering; auto-closed
     */
    static Database restoreFromSnapshot(Launcher launcher, InputStream in) throws IOException {
        if (launcher.mReadOnly) {
            throw new IllegalArgumentException("Cannot restore into a read-only database");
        }

        PageDb restored;

        File[] dataFiles = launcher.dataFiles();
        if (dataFiles == null) {
            PageArray dataPageArray = launcher.mDataPageArray;

            if (dataPageArray == null) {
                throw new UnsupportedOperationException
                    ("Restore only allowed for durable databases");
            }

            dataPageArray = dataPageArray.open();
            dataPageArray.truncatePageCount(0);

            // Delete old redo log files.
            deleteNumberedFiles(launcher.mBaseFile, REDO_FILE_SUFFIX);

            restored = DurablePageDb.restoreFromSnapshot(dataPageArray, launcher.mCrypto, in);

            // Delete the object, but keep the page array open.
            restored.delete();
        } else {
            for (File f : dataFiles) {
                // Delete old data file.
                Utils.delete(f);
                if (launcher.mMkdirs) {
                    f.getParentFile().mkdirs();
                }
            }

            EnumSet<OpenOption> options = launcher.createOpenOptions();

            // Delete old redo log files.
            deleteNumberedFiles(launcher.mBaseFile, REDO_FILE_SUFFIX);

            int pageSize = launcher.mPageSize;
            if (pageSize <= 0) {
                pageSize = DEFAULT_PAGE_SIZE;
            }

            restored = DurablePageDb.restoreFromSnapshot
                (pageSize, dataFiles, options, launcher.mCrypto, in);

            try {
                restored.close();
            } finally {
                restored.delete();
            }
        }

        return launcher.open(false, null);
    }

    @Override
    public void createCachePrimer(OutputStream out) throws IOException {
        if (!(mPageDb.isDurable())) {
            throw new UnsupportedOperationException
                ("Cache priming only allowed for durable databases");
        }

        out = ((DurablePageDb) mPageDb).encrypt(out);

        var dout = new DataOutputStream(out);

        dout.writeLong(PRIMER_MAGIC_NUMBER);

        for (TreeRef treeRef : mOpenTrees.values()) {
            Tree tree = treeRef.get();
            if (tree != null && !Tree.isInternal(tree.id())) {
                tree.writeCachePrimer(dout);
            }
        }

        // Terminator.
        dout.writeInt(-1);
    }

    @Override
    public void applyCachePrimer(final InputStream fin) throws IOException {
        try (fin) {
            if (!(mPageDb.isDurable())) {
                throw new UnsupportedOperationException
                    ("Cache priming only allowed for durable databases");
            }

            InputStream in = ((DurablePageDb) mPageDb).decrypt(fin);

            DataInput din;
            if (in instanceof DataInput) {
                din = (DataInput) in;
            } else {
                din = new DataInputStream(in);
            }

            long magic = din.readLong();
            if (magic != PRIMER_MAGIC_NUMBER) {
                throw new DatabaseException("Wrong cache primer magic number: " + magic);
            }

            while (true) {
                int len = din.readInt();
                if (len < 0) {
                    break;
                }
                var name = new byte[len];
                din.readFully(name);
                Tree tree = openTree(name, IX_FIND);
                if (tree != null) {
                    tree.applyCachePrimer(din);
                } else {
                    BTree.skipCachePrimer(din);
                }
            }
        }
    }

    @Override
    public Stats stats() {
        return stats(true);
    }

    /**
     * @param strict pass false to fail-fast when trying to latch nodes, preventing deadlocks
     */
    private Stats stats(boolean strict) {
        var stats = new Stats();

        stats.pageSize = mPageSize;

        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            long cursorCount = 0;
            int openTreesCount = 0;

            for (TreeRef treeRef : mOpenTrees.values()) {
                Tree tree = treeRef.get();
                if (tree != null) {
                    openTreesCount++;
                    cursorCount += tree.countCursors(strict);
                }
            }

            cursorCount += countCursors(mRegistry, strict) + countCursors(mRegistryKeyMap, strict)
                + countCursors(mFragmentedTrash, strict) + countCursors(mCursorRegistry, strict)
                + countCursors(mPreparedTxns, strict);

            stats.openIndexes = openTreesCount;
            stats.cursorCount = cursorCount;

            PageDb.Stats pstats = mPageDb.stats();
            stats.freePages = pstats.freePages;
            stats.totalPages = pstats.totalPages;

            stats.lockCount = mLockManager.numLocksHeld();

            for (TransactionContext txnContext : mTxnContexts) {
                txnContext.addStats(stats);
            }
        } finally {
            shared.release();
        }

        for (NodeGroup group : mNodeGroups) {
            stats.cachePages += group.nodeCount();
            stats.dirtyPages += group.dirtyCount();
        }

        if (stats.dirtyPages > stats.totalPages) {
            stats.dirtyPages = stats.totalPages;
        }

        return stats;
    }

    /**
     * @param tree can be null
     */
    private static long countCursors(BTree tree, boolean strict) {
        return tree == null ? 0 : tree.countCursors(strict);
    }

    static class RedoClose extends ShutdownHook.Weak<LocalDatabase> {
        RedoClose(LocalDatabase db) {
            super(db);
        }

        @Override
        void doShutdown(LocalDatabase db) {
            db.redoClose(RedoOps.OP_SHUTDOWN, null);
        }
    }

    /**
     * @param op OP_CLOSE or OP_SHUTDOWN
     */
    private void redoClose(byte op, Throwable cause) {
        RedoWriter redo = mRedoWriter;
        if (redo == null) {
            return;
        }

        redo.closeCause(cause);
        redo = redo.txnRedoWriter();
        redo.closeCause(cause);

        try {
            // NO_FLUSH now behaves like NO_SYNC.
            redo.alwaysFlush(true);
        } catch (IOException e) {
            // Ignore.
        }

        try {
            TransactionContext context = anyTransactionContext();
            context.redoTimestamp(redo, op);
            context.flush();

            redo.force(true, TimeUnit.SECONDS.toNanos(5));
        } catch (IOException e) {
            // Ignore.
        }

        // When shutdown hook is invoked, don't close the redo writer. It may interfere with
        // other shutdown hooks, causing unexpected exceptions to be thrown during the whole
        // shutdown sequence.

        if (op == RedoOps.OP_CLOSE) {
            Utils.closeQuietly(redo);
        }
    }

    @Override
    public void flush() throws IOException {
        flush(0); // flush only
    }

    @Override
    public void sync() throws IOException {
        flush(1); // flush and sync
    }

    /**
     * @param level 0: flush only, 1: flush and sync, 2: flush and sync metadata
     */
    private void flush(int level) throws IOException {
        if (!isClosed() && mRedoWriter != null) {
            mRedoWriter.flush();
            if (level > 0) {
                mRedoWriter.force(level > 1, -1); // infinite timeout
            }
        }
    }

    @Override
    public void checkpoint() throws IOException {
        try {
            checkpoint(0, 0);
        } catch (Throwable e) {
            rethrowIfRecoverable(e);
            closeQuietly(this, e);
            throw e;
        }
    }

    @Override
    public void suspendCheckpoints() {
        Checkpointer c = mCheckpointer;
        if (c != null) {
            c.suspend();
        }
    }

    @Override
    public void resumeCheckpoints() {
        Checkpointer c = mCheckpointer;
        if (c != null) {
            c.resume();
        }
    }

    @Override
    public boolean compactFile(CompactionObserver observer, double target) throws IOException {
        if (target < 0 || target > 1) {
            throw new IllegalArgumentException("Illegal compaction target: " + target);
        }

        if (target == 0) {
            // No compaction to do at all, but not aborted.
            return true;
        }

        long targetPageCount;
        mCheckpointLock.lock();
        try {
            PageDb.Stats stats = mPageDb.stats();
            long usedPages = stats.totalPages - stats.freePages;
            targetPageCount = Math.max(usedPages, (long) (usedPages / target));

            // Determine the maximum amount of space required to store the reserve list nodes
            // and ensure the target includes them.
            long reserve;
            {
                // Total pages freed.
                long freed = stats.totalPages - targetPageCount;

                // Scale by the maximum size for encoding page identifiers, assuming no savings
                // from delta encoding.
                freed *= calcUnsignedVarLongLength(stats.totalPages << 1);

                // Divide by the node size, excluding the header (see PageQueue).
                reserve = freed / (mPageSize - (8 + 8));

                // A minimum is required because the regular and free lists need to allocate
                // one extra node at checkpoint. Up to three checkpoints may be issued, so pad
                // by 2 * 3 = 6.
                reserve += 6;
            }

            targetPageCount += reserve;

            if (targetPageCount >= stats.totalPages && targetPageCount >= mPageDb.pageCount()) {
                return true;
            }

            if (!mPageDb.compactionStart(targetPageCount)) {
                return false;
            }
        } finally {
            mCheckpointLock.unlock();
        }

        boolean completed = mPageDb.compactionScanFreeList();

        if (completed) {
            // Issue a checkpoint to ensure all dirty nodes are flushed out. This ensures that
            // nodes can be moved out of the compaction zone by simply marking them dirty. If
            // already dirty, they'll not be in the compaction zone unless compaction aborted.
            checkpoint();

            if (observer == null) {
                observer = new CompactionObserver();
            }

            final long highestNodeId = targetPageCount - 1;
            final CompactionObserver fobserver = observer;

            completed = scanAllIndexes(tree -> {
                return tree.compactTree(tree.observableView(), highestNodeId, fobserver);
            });

            forceCheckpoint();

            if (completed && mPageDb.compactionScanFreeList()) {
                if (!mPageDb.compactionVerify() && mPageDb.compactionScanFreeList()) {
                    forceCheckpoint();
                }
            }
        }

        mCheckpointLock.lock();
        try {
            completed &= mPageDb.compactionEnd();

            // Reclaim reserved pages, but only after a checkpoint has been performed.
            forceCheckpoint();
            mPageDb.compactionReclaim();
            // Checkpoint again in order for reclaimed pages to be immediately available.
            forceCheckpoint();

            if (completed) {
                // And now, attempt to actually shrink the file.
                return mPageDb.truncatePages();
            }
        } finally {
            mCheckpointLock.unlock();
        }

        return false;
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
        var fls = new FreeListScan();
        Runner.start(fls);

        if (observer == null) {
            observer = new VerificationObserver();
        }

        final boolean[] passedRef = {true};
        final VerificationObserver fobserver = observer;

        scanAllIndexes(tree -> {
            Index view = tree.observableView();
            fobserver.failed = false;
            boolean keepGoing = tree.verifyTree(view, fobserver);
            passedRef[0] &= !fobserver.failed;
            if (keepGoing) {
                keepGoing = fobserver.indexComplete(view, !fobserver.failed, null);
            }
            return keepGoing;
        });

        // Throws an exception if it fails.
        fls.waitFor();

        return passedRef[0];
    }

    private class FreeListScan implements Runnable, LongConsumer {
        private Object mFinished;

        @Override
        public void run() {
            // The free list is scanned with a shared commit lock held. Perform the scan
            // without interference from a checkpoint, which would attempt to acquire the
            // exclusive commit lock, causing any other shared lock requests to stall.
            mCheckpointLock.lock();
            Object finished;
            try {
                mPageDb.scanFreeList(this);
                finished = this;
            } catch (Throwable e) {
                finished = e;
            } finally {
                mCheckpointLock.unlock();
            }

            synchronized (this) {
                mFinished = finished;
                notifyAll();
            }
        }

        @Override
        public void accept(long id) {
            // TODO: check for duplicates
        }

        synchronized void waitFor() throws IOException {
            try {
                while (mFinished == null) {
                    wait();
                }
            } catch (InterruptedException e) {
                return;
            }

            if (mFinished instanceof Throwable) {
                rethrow((Throwable) mFinished);
            }
        }
    }

    @FunctionalInterface
    static interface ScanVisitor {
        /**
         * @return false if should stop
         */
        boolean apply(Tree tree) throws IOException;
    }

    /**
     * @return false if stopped
     */
    private boolean scanAllIndexes(ScanVisitor visitor) throws IOException {
        if (!scan(visitor, mRegistry) || !scan(visitor, mRegistryKeyMap)
            || !scan(visitor, openFragmentedTrash(IX_FIND))
            || !scan(visitor, openCursorRegistry(IX_FIND))
            || !scan(visitor, openPreparedTxns(IX_FIND)))
        {
            return false;
        }

        // Note that temporary indexes aren't scanned. Some operations performed on them (the
        // sorter) aren't thread-safe, and so verification and compaction cannot examine them.
        Cursor all = indexRegistryByName().newCursor(null);
        try {
            for (all.first(); all.key() != null; all.next()) {
                long id = decodeLongBE(all.value(), 0);

                Index index = indexById(id);
                if (index instanceof Tree && !visitor.apply((Tree) index)) {
                    return false;
                }
            }
        } finally {
            all.reset();
        }

        return true;
    }

    /**
     * @return false if should stop scanning
     */
    private static boolean scan(ScanVisitor visitor, BTree tree) throws IOException {
        return tree == null || visitor.apply(tree);
    }

    @Override
    public boolean isLeader() {
        return mRedoWriter == null ? true : mRedoWriter.isLeader();
    }

    @Override
    public void uponLeader(Runnable acquired, Runnable lost) {
        if (mRedoWriter == null) {
            if (acquired != null) {
                Runner.start(acquired);
            }
        } else {
            mRedoWriter.uponLeader(acquired, lost);
        }
    }

    @Override
    public boolean failover() throws IOException {
        return mRedoWriter == null ? false : mRedoWriter.txnRedoWriter().failover();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        close(cause, false);
    }

    @Override
    public void shutdown() throws IOException {
        close(null, mPageDb.isDurable());
    }

    private void close(Throwable cause, boolean shutdown) throws IOException {
        if (!cClosedHandle.compareAndSet(this, 0, 1)) {
            return;
        }

        if (cause != null) {
            mClosedCause = cause;
            Throwable rootCause = rootCause(cause);
            if (mEventListener == null) {
                uncaught(rootCause);
            } else {
                mEventListener.notify(EventType.PANIC_UNHANDLED_EXCEPTION,
                                      "Closing database due to unhandled exception: %1$s",
                                      rootCause);
            }
        }

        boolean lockedCheckpointer = false;

        try {
            if (mCheckpointer != null) {
                mCheckpointer.close(cause);
            }

            // Wait for any in-progress checkpoint to complete.

            if (mCheckpointLock.tryLock()) {
                lockedCheckpointer = true;
            } else if (cause == null && !(mRedoWriter instanceof ReplController)) {
                // Only attempt lock if not panicked and not replicated. If panicked, other
                // locks might be held and so acquiring checkpoint lock might deadlock.
                // Replicated databases might stall indefinitely when checkpointing.
                // Checkpointer should eventually exit after other resources are closed.
                mCheckpointLock.lock();
                lockedCheckpointer = true;
            }
        } finally {
            Thread ct = mCheckpointer == null ? null : mCheckpointer.interrupt();

            if (lockedCheckpointer) {
                mCheckpointLock.unlock();

                if (ct != null) {
                    // Wait for checkpointer thread to finish.
                    try {
                        ct.join();
                    } catch (InterruptedException e) {
                        // Ignore.
                    }
                }
            }
        }

        try {
            final CommitLock lock = mCommitLock;

            if (mOpenTrees != null) {
                // Clear out open trees with commit lock held, to prevent any trees from being
                // opened again. Any attempt to open a tree must acquire the commit lock and
                // then check if the database is closed.
                final ArrayList<Tree> trees;

                if (lock == null) {
                    mOpenTreesLatch.acquireExclusive();
                } else while (true) {
                    lock.acquireExclusive();
                    if (mOpenTreesLatch.tryAcquireExclusive()) {
                        break;
                    }
                    // Retry to avoid a potential deadlock.
                    lock.releaseExclusive();
                    Thread.yield();
                }

                try {
                    trees = new ArrayList<>(mOpenTreesById.size());

                    mOpenTreesById.traverse(entry -> {
                        Tree tree = entry.value.get();
                        if (tree != null) {
                            trees.add(tree);
                        }
                        return true;
                    });

                    mOpenTrees.clear();

                    trees.add(mRegistryKeyMap);

                    trees.add(mFragmentedTrash);
                    mFragmentedTrash = null;

                    trees.add(mCursorRegistry);
                    mCursorRegistry = null;

                    trees.add(mPreparedTxns);
                    mPreparedTxns = null;
                } finally {
                    mOpenTreesLatch.releaseExclusive();
                    if (lock != null) {
                        lock.releaseExclusive();
                    }
                }

                for (Tree tree : trees) {
                    if (tree != null) {
                        tree.forceClose();
                    }
                }

                if (shutdown) {
                    mCheckpointLock.lock();
                    try {
                        doCheckpoint(-1, 0, 0); // force even if closed
                    } catch (Throwable e) {
                        shutdown = false;
                    } finally {
                        mCheckpointLock.unlock();
                    }
                }

                if (mRegistry != null) {
                    mRegistry.forceClose();
                }
            }

            if (lock != null) {
                lock.acquireExclusive();
            }
            try {
                if (mNodeGroups != null) {
                    for (NodeGroup group : mNodeGroups) {
                        if (group != null) {
                            group.delete();
                        }
                    }
                }

                if (mTxnContexts != null) {
                    for (TransactionContext txnContext : mTxnContexts) {
                        if (txnContext != null) {
                            txnContext.deleteUndoLogs();
                        }
                    }
                }

                UndoLog masterUndoLog = mCommitMasterUndoLog;
                if (masterUndoLog != null) {
                    masterUndoLog.delete();
                }

                nodeMapDeleteAll();

                redoClose(RedoOps.OP_CLOSE, cause);

                IOException ex = null;
                ex = closeQuietly(ex, mPageDb, cause);
                ex = closeQuietly(ex, mTempFileManager, cause);

                if (shutdown && mBaseFile != null && !mReadOnly) {
                    deleteRedoLogFiles();
                    ex = deleteLockFile(mLockFile, ex);
                } else {
                    ex = closeQuietly(ex, mLockFile, cause);
                }

                if (mLockManager != null) {
                    mLockManager.close();
                }

                if (ex != null) {
                    throw ex;
                }
            } finally {
                if (lock != null) {
                    lock.releaseExclusive();
                }
            }
        } finally {
            if (mCheckpointer != null) {
                mCheckpointer.shutdown();
            }
            if (mPageDb != null) {
                mPageDb.delete();
            }
            deleteCommitHeader();
            p_arenaDelete(mArena);
        }
    }

    private void deleteCommitHeader() {
        /*P*/ // [
        mCommitHeader = null;
        /*P*/ // |
        /*P*/ // p_delete((long) cCommitHeaderHandle.getAndSet(this, p_null()));
        /*P*/ // ]
    }

    @Override
    public boolean isClosed() {
        return mClosed != 0;
    }

    /**
     * If any closed cause exception, wraps it as a DatabaseException and throws it.
     */
    void checkClosed() throws DatabaseException {
        checkClosed(null);
    }

    /**
     * If any closed cause exception, wraps it as a DatabaseException and throws it.
     *
     * @param caught exception which was caught; will be rethrown if matches the closed cause
     */
    void checkClosed(Throwable caught) throws DatabaseException {
        if (isClosed()) {
            if (caught != null && caught == mClosedCause) {
                throw rethrow(caught);
            }
            String message = "Closed";
            Throwable cause = mClosedCause;
            if (cause != null) {
                message += "; " + rootCause(cause);
            }
            throw new DatabaseException(message, cause);
        }
    }

    /**
     * Tries to directly throw the closed cause exception, wrapping it if necessary.
     */
    void checkClosedCause() throws IOException {
        Throwable cause = mClosedCause;
        if (cause != null) {
            try {
                throw cause;
            } catch (IOException | RuntimeException | Error e) {
                throw e;
            } catch (Throwable e) {
                throw new DatabaseException(cause);
            }
        }
    }

    Throwable closedCause() {
        return mClosedCause;
    }

    void treeClosed(BTree tree) {
        mOpenTreesLatch.acquireExclusive();
        try {
            TreeRef ref = mOpenTreesById.getValue(tree.mId);
            if (ref != null) {
                Tree actual = ref.get();
                if (actual != null && actual.isUserOf(tree)) {
                    ref.clear();
                    if (tree.mName != null) {
                        mOpenTrees.remove(tree.mName);
                    }
                    mOpenTreesById.remove(tree.mId);
                }
            }
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }
    }

    /**
     * @return false if already in the trash
     */
    private boolean moveToTrash(long treeId, byte[] treeIdBytes) throws IOException {
        final LocalTransaction txn = newNoRedoTransaction();

        try {
            txn.lockTimeout(-1, null);

            if (!doMoveToTrash(txn, treeId, treeIdBytes)) {
                return false;
            }

            if (txn.mRedo != null) {
                // Note: No additional operations can appear after OP_DELETE_INDEX. When a
                // replica reads this operation it immediately commits the transaction in order
                // for the deletion task to be started immediately. The redo log still contains
                // a commit operation, which is redundant and harmless.

                txn.durabilityMode(alwaysRedo(mDurabilityMode));

                long commitPos;
                CommitLock.Shared shared = mCommitLock.acquireShared();
                try {
                    txn.check();
                    commitPos = txn.mContext.redoDeleteIndexCommitFinal
                        (txn.mRedo, txn.txnId(), treeId, txn.durabilityMode());
                } finally {
                    shared.release();
                }

                if (commitPos != 0) {
                    // Must wait for durability confirmation before performing actions below
                    // which cannot be easily rolled back. No global latches or locks are held
                    // while waiting.
                    txn.mRedo.txnCommitSync(commitPos);
                }
            }

            txn.commit();
        } catch (Throwable e) {
            rethrowIfRecoverable(e);
            throw closeOnFailure(this, e);
        } finally {
            txn.reset();
        }

        return true;
    }

    /**
     * @param txn must not redo
     */
    void redoMoveToTrash(LocalTransaction txn, long treeId) throws IOException {
        var treeIdBytes = new byte[8];
        encodeLongBE(treeIdBytes, 0, treeId);
        doMoveToTrash(txn, treeId, treeIdBytes);
    }

    /**
     * @param txn must not redo
     * @return false if already in the trash
     */
    private boolean doMoveToTrash(LocalTransaction txn, long treeId, byte[] treeIdBytes)
        throws IOException
    {
        final byte[] trashIdKey = newKey(RK_TRASH_ID, treeIdBytes);

        if (mRegistryKeyMap.load(txn, trashIdKey) != null) {
            // Already in the trash.
            return false;
        }

        final byte[] idKey = newKey(RK_INDEX_ID, treeIdBytes);

        byte[] treeName = mRegistryKeyMap.exchange(txn, idKey, null);

        if (treeName == null) {
            // A trash entry with just a zero indicates that the name is null.
            mRegistryKeyMap.store(txn, trashIdKey, new byte[1]);
        } else {
            byte[] nameKey = newKey(RK_INDEX_NAME, treeName);
            mRegistryKeyMap.remove(txn, nameKey, treeIdBytes);
            // Tag the trash entry to indicate that name is non-null. Note that nameKey
            // instance is modified directly.
            nameKey[0] = 1;
            mRegistryKeyMap.store(txn, trashIdKey, nameKey);
        }
        
        return true;
    }

    /**
     * Must be called after all entries in the tree have been deleted and tree is closed.
     */
    void removeFromTrash(BTree tree, Node root) throws IOException {
        byte[] trashIdKey = newKey(RK_TRASH_ID, tree.mIdBytes);

        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            if (root != null) {
                root.acquireExclusive();
                if (root.mPage == p_closedTreePage()) {
                    // Database has been closed.
                    root.releaseExclusive();
                    return;
                }
                deleteNode(root);
            }
            mRegistryKeyMap.delete(Transaction.BOGUS, trashIdKey);
            mRegistry.delete(Transaction.BOGUS, tree.mIdBytes);
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        } finally {
            shared.release();
        }
    }

    /**
     * Removes all references to a temporary tree which was grafted to another one. Caller must
     * hold shared commit lock.
     */
    void removeGraftedTempTree(BTree tree) throws IOException {
        try {
            mOpenTreesLatch.acquireExclusive();
            try {
                TreeRef ref = mOpenTreesById.removeValue(tree.mId);
                if (ref != null && ref.get() == tree) {
                    ref.clear();
                }
            } finally {
                mOpenTreesLatch.releaseExclusive();
            }
            byte[] trashIdKey = newKey(RK_TRASH_ID, tree.mIdBytes);
            mRegistryKeyMap.delete(Transaction.BOGUS, trashIdKey);
            mRegistry.delete(Transaction.BOGUS, tree.mIdBytes);
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        }
    }

    /**
     * Should be called before attempting to register a cursor, in case an exception is thrown.
     */
    BTree cursorRegistry() throws IOException {
        BTree cursorRegistry = mCursorRegistry;
        return cursorRegistry != null ? cursorRegistry : openCursorRegistry(IX_CREATE);
    }

    /**
     * @param ixOption IX_FIND or IX_CREATE
     */
    private BTree openCursorRegistry(long ixOption) throws IOException {
        BTree cursorRegistry;

        mOpenTreesLatch.acquireExclusive();
        try {
            if ((cursorRegistry = mCursorRegistry) == null) {
                mCursorRegistry = cursorRegistry =
                    openInternalTree(Tree.CURSOR_REGISTRY_ID, ixOption);
            }
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }

        return cursorRegistry;
    }

    /**
     * Should be called after the cursor id has been assigned, with the commit lock held.
     */
    void registerCursor(BTree cursorRegistry, BTreeCursor cursor) throws IOException {
        try {
            var cursorIdBytes = new byte[8];
            encodeLongBE(cursorIdBytes, 0, cursor.mCursorId);
            byte[] regValue = cursor.mTree.mIdBytes;
            byte[] key = cursor.key();
            if (key != null) {
                var newReg = new byte[regValue.length + 1 + key.length];
                System.arraycopy(regValue, 0, newReg, 0, regValue.length);
                System.arraycopy(key, 0, newReg, regValue.length + 1, key.length);
                regValue = newReg;
            }
            cursorRegistry.store(Transaction.BOGUS, cursorIdBytes, regValue);
        } catch (Throwable e) {
            try {
                cursor.unregister();
            } catch (Throwable e2) {
                suppress(e, e2);
            }
            throw e;
        }
    }

    void unregisterCursor(long cursorId) {
        try {
            var cursorIdBytes = new byte[8];
            encodeLongBE(cursorIdBytes, 0, cursorId);
            cursorRegistry().store(Transaction.BOGUS, cursorIdBytes, null);
        } catch (Throwable e) {
            // Database is borked, cleanup later.
        }
    }

    BTree preparedTxns() throws IOException {
        BTree preparedTxns = mPreparedTxns;
        return preparedTxns != null ? preparedTxns : openPreparedTxns(IX_CREATE);
    }

    /**
     * Returns null if the index doesn't exist.
     */
    BTree tryPreparedTxns() throws IOException {
        BTree preparedTxns = mPreparedTxns;
        return preparedTxns != null ? preparedTxns : openPreparedTxns(IX_FIND);
    }

    /**
     * @param ixOption IX_FIND or IX_CREATE
     */
    private BTree openPreparedTxns(long ixOption) throws IOException {
        BTree preparedTxns;

        mOpenTreesLatch.acquireExclusive();
        try {
            if ((preparedTxns = mPreparedTxns) == null) {
                mPreparedTxns = preparedTxns =
                    openInternalTree(Tree.PREPARED_TXNS_ID, ixOption);
            }
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }

        return preparedTxns;
    }

    /**
     * @param treeId pass zero if unknown or not applicable
     * @param rootId pass zero to create
     * @return unlatched and unevictable root node
     */
    private Node loadTreeRoot(final long treeId, final long rootId) throws IOException {
        if (rootId == 0) {
            Node rootNode = allocLatchedNode(NodeGroup.MODE_UNEVICTABLE);

            try {
                /*P*/ // [
                rootNode.asEmptyRoot();
                /*P*/ // |
                /*P*/ // if (mFullyMapped) {
                /*P*/ //     rootNode.mPage = p_nonTreePage(); // always an empty leaf node
                /*P*/ //     rootNode.id(0);
                /*P*/ //     rootNode.mCachedState = CACHED_CLEAN;
                /*P*/ // } else {
                /*P*/ //     rootNode.asEmptyRoot();
                /*P*/ // }
                /*P*/ // ]
                return rootNode;
            } finally {
                rootNode.releaseExclusive();
            }
        } else {
            // Check if root node is still around after tree was closed.
            Node rootNode = nodeMapGetAndRemove(rootId);

            if (rootNode != null) {
                try {
                    rootNode.makeUnevictable();
                    return rootNode;
                } finally {
                    rootNode.releaseExclusive();
                }
            }

            rootNode = allocLatchedNode(NodeGroup.MODE_UNEVICTABLE);

            try {
                try {
                    rootNode.read(this, rootId);
                } finally {
                    rootNode.releaseExclusive();
                }
                return rootNode;
            } catch (Throwable e) {
                rootNode.makeEvictableNow();
                throw e;
            }
        }
    }

    /**
     * Loads the root registry node, or creates one if store is new. Root node
     * is not eligible for eviction.
     */
    private Node loadRegistryRoot(Launcher launcher, byte[] header) throws IOException {
        int version = decodeIntLE(header, I_ENCODING_VERSION);

        if (launcher.mDebugOpen != null) {
            mEventListener.notify(EventType.DEBUG, "ENCODING_VERSION: %1$d", version);
        }

        long rootId;
        if (version == 0) {
            rootId = 0;
            // No registry; clearly nothing has been checkpointed.
            mInitialReadState = CACHED_DIRTY_0;
        } else {
            if (version != ENCODING_VERSION) {
                throw new CorruptDatabaseException("Unknown encoding version: " + version);
            }

            rootId = decodeLongLE(header, I_ROOT_PAGE_ID);

            if (launcher.mDebugOpen != null) {
                mEventListener.notify(EventType.DEBUG, "ROOT_PAGE_ID: %1$d", rootId);
            }
        }

        long replEncoding = decodeLongLE(header, I_REPL_ENCODING);
        StreamReplicator repl = launcher.mRepl;

        if ((replEncoding != 0 || repl != null) && launcher.mDebugOpen != null) {
            mEventListener.notify(EventType.DEBUG, "REPL_ENCODING: %1$d", replEncoding);
        }

        if (repl == null) {
            if (replEncoding != 0 && !hasRedoLogFiles()) {
                // Conversion to non-replicated mode is allowed by simply touching redo file 0.
                throw new DatabaseException
                    ("Database must be configured with a replicator, " +
                     "identified by: " + replEncoding);
            }
        } else if (replEncoding == 0) {
            // Check if conversion to replicated mode is allowed. The replication log must have
            // data starting at position 0, and no redo log files can exist.

            String msg = "Database was created initially without a replicator. " +
                "Conversion isn't possible ";

            if (!repl.isReadable(0)) {
                msg += "without complete replication data.";
                throw new DatabaseException(msg);
            }

            if (hasRedoLogFiles()) {
                msg += "when redo log files exist. A clean shutdown is required.";
                throw new DatabaseException(msg);
            }

            // The redo position passed to the ReplManager must be 0, but what's in the header
            // might be higher. Since we have the header data passed to us already, we can
            // modify it without persisting it.
            encodeLongLE(header, I_REDO_POSITION, 0);
        } else if (replEncoding != repl.encoding()) {
            throw new DatabaseException
                ("Database was created initially with a different replicator, " +
                 "identified by: " + replEncoding);
        }

        return loadTreeRoot(0, rootId);
    }

    private static final byte IX_FIND = 0, IX_CREATE = 1;

    private BTree openInternalTree(long treeId, long ixOption) throws IOException {
        return openInternalTree(treeId, ixOption, null);
    }

    private BTree openInternalTree(long treeId, long ixOption, Launcher launcher)
        throws IOException
    {
        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            checkClosed();

            var treeIdBytes = new byte[8];
            encodeLongBE(treeIdBytes, 0, treeId);
            byte[] rootIdBytes = mRegistry.load(Transaction.BOGUS, treeIdBytes);
            long rootId;
            if (rootIdBytes != null) {
                rootId = decodeLongLE(rootIdBytes, 0);
            } else {
                if (ixOption == IX_FIND) {
                    return null;
                }
                rootId = 0;
            }

            Node root = loadTreeRoot(treeId, rootId);

            // Cannot call newBTreeInstance because mRedoWriter isn't set yet.
            if (launcher != null && launcher.mRepl != null) {
                return new BTree.Repl(this, treeId, treeIdBytes, root);
            }

            return newBTreeInstance(treeId, treeIdBytes, null, root);
        } finally {
            shared.release();
        }
    }

    /**
     * @param name required (cannot be null)
     */
    private Tree openTree(byte[] name, long ixOption) throws IOException {
        return openTree(null, null, name, ixOption);
    }

    /**
     * @param findTxn optional
     * @param treeIdBytes optional
     * @param name required (cannot be null)
     */
    private Tree openTree(Transaction findTxn, byte[] treeIdBytes, byte[] name, long ixOption)
        throws IOException
    {
        find: {
            TreeRef treeRef;
            mOpenTreesLatch.acquireShared();
            try {
                treeRef = mOpenTrees.get(name);
                if (treeRef == null) {
                    break find;
                }
                Tree tree = treeRef.get();
                if (tree != null) {
                    return tree;
                }
            } finally {
                mOpenTreesLatch.releaseShared();
            }

            // Ensure that root node of cleared tree reference is available in the node map
            // before potentially replacing it. Weak references are cleared before they are
            // enqueued, and so polling the queue does not guarantee node eviction. Process the
            // tree directly.
            cleanupUnreferencedTree(treeRef);
        }

        CommitLock.Shared shared = mCommitLock.acquireShared();
        try {
            return doOpenTree(findTxn, treeIdBytes, name, ixOption);
        } finally {
            shared.release();
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param findTxn optional
     * @param treeIdBytes optional
     * @param name required (cannot be null)
     */
    private Tree doOpenTree(Transaction findTxn, byte[] treeIdBytes, byte[] name, long ixOption)
        throws IOException
    {
        checkClosed();

        // Cleanup before opening more trees.
        cleanupUnreferencedTrees();

        name = name.clone();
        byte[] nameKey = newKey(RK_INDEX_NAME, name);

        if (treeIdBytes == null) {
            treeIdBytes = mRegistryKeyMap.load(findTxn, nameKey);
        }

        long treeId;
        // Is non-null if tree was created.
        byte[] idKey;

        if (treeIdBytes != null) {
            // Tree already exists.
            idKey = null;
            treeId = decodeLongBE(treeIdBytes, 0);
        } else if (ixOption == IX_FIND) {
            return null;
        } else create: {
            // Transactional find supported only for opens that do not create.
            if (findTxn != null) {
                throw new AssertionError();
            }

            Transaction createTxn = null;
            Locker locker = mLockManager.localLocker();

            mOpenTreesLatch.acquireExclusive();
            try {
                while (true) {
                    if (locker.doTryLockShared(mRegistryKeyMap.id(), nameKey, 0).isHeld()) {
                        break;
                    }
                    // Release locks and retry, avoiding possible deadlock if the checkpointer
                    // has suspended replica decoding in the middle of tree creation.
                    mOpenTreesLatch.releaseExclusive();
                    mCommitLock.releaseShared();
                    Thread.yield();
                    mCommitLock.acquireShared();
                    mOpenTreesLatch.acquireExclusive();
                }

                try {
                    treeIdBytes = mRegistryKeyMap.load(Transaction.BOGUS, nameKey);
                } finally {
                    locker.doUnlock();
                }

                if (treeIdBytes != null) {
                    // Another thread created it.
                    idKey = null;
                    treeId = decodeLongBE(treeIdBytes, 0);
                    break create;
                }

                treeIdBytes = new byte[8];

                // Non-transactional operations are critical, in that any failure is treated as
                // non-recoverable.
                boolean critical = true;
                try {
                    do {
                        critical = false;
                        treeId = nextTreeId(RK_NEXT_TREE_ID);
                        encodeLongBE(treeIdBytes, 0, treeId);
                        critical = true;
                    } while (!mRegistry.insert(Transaction.BOGUS, treeIdBytes, EMPTY_BYTES));

                    critical = false;

                    try {
                        idKey = newKey(RK_INDEX_ID, treeIdBytes);

                        if (mRedoWriter instanceof ReplController) {
                            // Confirmation is required when replicated.
                            createTxn = newTransaction(DurabilityMode.SYNC);
                        } else {
                            createTxn = newAlwaysRedoTransaction();
                        }

                        createTxn.lockTimeout(-1, null);

                        // Insert order is important for the indexById method to work reliably.
                        if (!mRegistryKeyMap.insert(createTxn, idKey, name)) {
                            throw new DatabaseException("Unable to insert index id");
                        }
                        if (!mRegistryKeyMap.insert(createTxn, nameKey, treeIdBytes)) {
                            throw new DatabaseException("Unable to insert index name");
                        }
                    } catch (Throwable e) {
                        critical = true;
                        try {
                            if (createTxn != null) {
                                createTxn.reset();
                            }
                            mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                            critical = false;
                        } catch (Throwable e2) {
                            Utils.suppress(e, e2);
                        }
                        throw e;
                    }
                } catch (Throwable e) {
                    if (!critical) {
                        rethrowIfRecoverable(e);
                    }
                    throw closeOnFailure(this, e);
                }
            } finally {
                // Release to allow opening other indexes while blocked on commit.
                mOpenTreesLatch.releaseExclusive();
            }

            if (createTxn != null) {
                try {
                    createTxn.commit();
                } catch (Throwable e) {
                    try {
                        createTxn.reset();
                        mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                    } catch (Throwable e2) {
                        Utils.suppress(e, e2);
                        throw closeOnFailure(this, e);
                    }
                    rethrowIfRecoverable(e);
                    throw closeOnFailure(this, e);
                }
            }
        }

        // Use a transaction to ensure that only one thread loads the requested tree. Nothing
        // is written into it.
        Transaction txn = threadLocalTransaction(DurabilityMode.NO_REDO);
        try {
            txn.lockTimeout(-1, null);

            if (txn.lockCheck(mRegistry.id(), treeIdBytes) != LockResult.UNOWNED) {
                throw new LockFailureException("Index open listener self deadlock");
            }

            // Pass the transaction to acquire the lock.
            byte[] rootIdBytes = mRegistry.load(txn, treeIdBytes);

            Tree tree = lookupIndexById(treeId);
            if (tree != null) {
                // Another thread got the lock first and loaded the tree.
                return tree;
            }

            long rootId = (rootIdBytes == null || rootIdBytes.length == 0) ? 0
                : decodeLongLE(rootIdBytes, 0);

            Node root = loadTreeRoot(treeId, rootId);

            BTree btree = newBTreeInstance(treeId, treeIdBytes, name, root);
            tree = btree;

            try {
                var treeRef = new TreeRef(tree, btree, mOpenTreesRefQueue);

                mOpenTreesLatch.acquireExclusive();
                try {
                    mOpenTrees.put(name, treeRef);
                    try {
                        mOpenTreesById.insert(treeId).value = treeRef;
                    } catch (Throwable e) {
                        mOpenTrees.remove(name);
                        throw e;
                    }
                } finally {
                    mOpenTreesLatch.releaseExclusive();
                }
            } catch (Throwable e) {
                btree.close();
                throw e;
            }

            return tree;
        } catch (Throwable e) {
            if (idKey != null) {
                // Rollback create of new tree.
                try {
                    mRegistryKeyMap.delete(null, idKey);
                    mRegistryKeyMap.delete(null, nameKey);
                    mRegistry.delete(Transaction.BOGUS, treeIdBytes);
                } catch (Throwable e2) {
                    // Ignore.
                }
            }
            throw e;
        } finally {
            txn.reset();
        }
    }

    private BTree newBTreeInstance(long id, byte[] idBytes, byte[] name, Node root) {
        BTree tree;
        if (mRedoWriter instanceof ReplWriter) {
            // Always need an explcit transaction when using auto-commit, to ensure that
            // rollback is possible.
            tree = new BTree.Repl(this, id, idBytes, root);
        } else {
            tree = new BTree(this, id, idBytes, root);
        }
        tree.mName = name;
        return tree;
    }

    /**
     * @param type RK_NEXT_TREE_ID or RK_NEXT_TEMP_ID
     */
    private long nextTreeId(byte type) throws IOException {
        // By generating identifiers from a 64-bit sequence, it's effectively impossible for
        // them to get re-used after trees are deleted. Use a tree id mask, to make the
        // identifiers less predictable and non-compatible with other database instances.

        long treeIdMask = mPageDb.databaseId();
        if (treeIdMask == 0) {
            // Use the old mask for compatibility.
            byte[] key = {RK_TREE_ID_MASK};
            byte[] treeIdMaskBytes = mRegistryKeyMap.load(Transaction.BOGUS, key);
            treeIdMask = decodeLongLE(treeIdMaskBytes, 0);
        }

        Transaction txn;
        if (type == RK_NEXT_TEMP_ID) {
            txn = newNoRedoTransaction();
            // Apply negative sequence, avoiding collisions.
            treeIdMask = ~treeIdMask;
        } else {
            txn = newAlwaysRedoTransaction();
        }

        // Make only one change in the transaction, using Cursor.commit to only write one redo
        // operation. This ensures that the decode stream cannot be suspended in the middle of
        // the operation, preventing replica deadlock. It can be caused when a checkpoint is
        // starting and another thread is trying to concurrently generate a new identifier. If
        // the lock is held while the decoder is suspended, and the commit lock cannot be
        // acquired by the checkpointer, deadlock is possible.

        try (Cursor c = mRegistryKeyMap.newCursor(txn)) {
            txn.lockTimeout(-1, null);
            c.find(new byte[] {type});

            byte[] nextTreeIdBytes = c.value();
            if (nextTreeIdBytes == null) {
                nextTreeIdBytes = new byte[8];
            }

            long nextTreeId = decodeLongLE(nextTreeIdBytes, 0);

            long treeId;
            do {
                treeId = scramble((nextTreeId++) ^ treeIdMask);
            } while (Tree.isInternal(treeId));

            encodeLongLE(nextTreeIdBytes, 0, nextTreeId);

            c.commit(nextTreeIdBytes);

            return treeId;
        } finally {
            txn.reset();
        }
    }

    /**
     * BTree instances retain a reference to an unevictable root node. If tree is no longer in
     * use, allow it to be evicted.
     */
    private void cleanupUnreferencedTrees() throws IOException {
        final ReferenceQueue queue = mOpenTreesRefQueue;
        if (queue == null) {
            return;
        }
        try {
            while (true) {
                Reference ref = queue.poll();
                if (ref == null) {
                    break;
                }
                if (ref instanceof TreeRef) {
                    cleanupUnreferencedTree((TreeRef) ref);
                }
            }
        } catch (Exception e) {
            if (!isClosed()) {
                throw e;
            }
        }
    }

    private void cleanupUnreferencedTree(TreeRef ref) throws IOException {
        Node root = ref.mRoot;
        root.acquireShared();
        try {
            mOpenTreesLatch.acquireExclusive();
            try {
                LHashTable.ObjEntry<TreeRef> entry = mOpenTreesById.get(ref.mId);
                if (entry == null || entry.value != ref) {
                    return;
                }
                if (ref.mName != null) {
                    mOpenTrees.remove(ref.mName);
                }
                mOpenTreesById.remove(ref.mId);
                root.makeEvictableNow();
                if (root.id() > 0) {
                    nodeMapPut(root);
                }
            } finally {
                mOpenTreesLatch.releaseExclusive();
            }
        } finally {
            root.releaseShared();
        }
    }

    private static byte[] newKey(byte type, byte[] payload) {
        var key = new byte[1 + payload.length];
        key[0] = type;
        arraycopy(payload, 0, key, 1, payload.length);
        return key;
    }

    private static byte[] newKey(byte type, int payload) {
        var key = new byte[1 + 4];
        key[0] = type;
        encodeIntBE(key, 1, payload);
        return key;
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageSize;
    }

    private int pageSize(/*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return mPageSize;
        /*P*/ // ]
    }

    /**
     * Returns the checkpoint commit lock, which can be held to prevent checkpoints from
     * capturing a safe commit point. In general, it should be acquired before any node
     * latches, but postponing acquisition reduces the total time held. Checkpoints don't have
     * to wait as long for the exclusive commit lock. Because node latching first isn't the
     * canonical ordering, acquiring the shared commit lock later must be prepared to
     * abort. Try to acquire first, and if it fails, release the node latch and do over.
     */
    @Override
    public CommitLock commitLock() {
        return mCommitLock;
    }

    /**
     * @return shared latched node if found; null if not found
     */
    Node nodeMapGetShared(long nodeId) {
        int hash = Long.hashCode(nodeId);
        while (true) {
            Node node = nodeMapGet(nodeId, hash);
            if (node == null) {
                return null;
            }
            node.acquireShared();
            if (nodeId == node.id()) {
                return node;
            }
            node.releaseShared();
        }
    }

    /**
     * @return exclusively latched node if found; null if not found
     */
    Node nodeMapGetExclusive(long nodeId) {
        int hash = Long.hashCode(nodeId);
        while (true) {
            Node node = nodeMapGet(nodeId, hash);
            if (node == null) {
                return null;
            }
            node.acquireExclusive();
            if (nodeId == node.id()) {
                return node;
            }
            node.releaseExclusive();
        }
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that the node
     * identifier matches, in case an eviction snuck in.
     */
    Node nodeMapGet(final long nodeId) {
        return nodeMapGet(nodeId, Long.hashCode(nodeId));
    }

    /**
     * Returns unconfirmed node if found. Caller must latch and confirm that the node
     * identifier matches, in case an eviction snuck in.
     */
    Node nodeMapGet(final long nodeId, final int hash) {
        final Node[] table = mNodeMapTable;
        final int slot = hash & (table.length - 1);

        var first = (Node) cNodeMapElementHandle.getVolatile(table, slot);

        if (first == null) {
            return null;
        }

        Node node = first;
        do {
            if (node.id() == nodeId) {
                return node;
            }
            node = node.mNodeMapNext;
        } while (node != null);

        // Again with lock held.

        first = nodeMapLock(table, slot, first);

        if (first == null) {
            return null;
        }

        node = first;
        do {
            if (node.id() == nodeId) {
                break;
            }
            node = node.mNodeMapNext;
        } while (node != null);

        cNodeMapElementHandle.setVolatile(table, slot, first);

        return node;
    }

    /**
     * @param first must not be null
     * @return actual first, which is null if slot is empty and not locked
     */
    private static Node nodeMapLock(final Node[] table, final int slot, Node first) {
        while (first == NM_LOCK
               || !cNodeMapElementHandle.compareAndSet(table, slot, first, NM_LOCK))
        {
            Thread.onSpinWait();
            first = (Node) cNodeMapElementHandle.getVolatile(table, slot);
            if (first == null) {
                return null;
            }
        }
        return first;
    }

    /**
     * Put a node into the map, but caller must confirm that the node isn't already present.
     *
     * @param node must be latched
     */
    void nodeMapPut(final Node node) {
        nodeMapPut(node, Long.hashCode(node.id()));
    }

    /**
     * Put a node into the map, but caller must confirm that the node isn't already present.
     *
     * @param node must be latched
     */
    void nodeMapPut(final Node node, final int hash) {
        final Node[] table = mNodeMapTable;
        final int slot = hash & (table.length - 1);

        var first = (Node) cNodeMapElementHandle.getVolatile(table, slot);

        while (true) {
            if (first != null && (first = nodeMapLock(table, slot, first)) != null) {
                break;
            }
            first = (Node) cNodeMapElementHandle.compareAndExchange(table, slot, null, node);
            if (first == null) {
                return;
            }
        }

        Node e = first;
        do {
            if (e == node) {
                cNodeMapElementHandle.setVolatile(table, slot, first);
                return;
            }
            if (e.id() == node.id()) {
                cNodeMapElementHandle.setVolatile(table, slot, first);
                throw new AssertionError("Already in NodeMap: " + node + ", " + e + ", " + hash);
            }
            e = e.mNodeMapNext;
        } while (e != null);

        node.mNodeMapNext = first;
        cNodeMapElementHandle.setVolatile(table, slot, node);
    }

    /**
     * Returns unconfirmed node if an existing node is found. Caller must latch and confirm
     * that the node identifier matches, in case an eviction snuck in.
     *
     * @param node must be latched
     * @return null if node was inserted, existing node otherwise
     */
    Node nodeMapPutIfAbsent(final Node node) {
        final int hash = Long.hashCode(node.id());
        final Node[] table = mNodeMapTable;
        final int slot = hash & (table.length - 1);

        var first = (Node) cNodeMapElementHandle.getVolatile(table, slot);

        while (true) {
            if (first != null && (first = nodeMapLock(table, slot, first)) != null) {
                break;
            }
            first = (Node) cNodeMapElementHandle.compareAndExchange(table, slot, null, node);
            if (first == null) {
                return null;
            }
        }

        Node e = first;
        do {
            if (e.id() == node.id()) {
                cNodeMapElementHandle.setVolatile(table, slot, first);
                return e;
            }
            e = e.mNodeMapNext;
        } while (e != null);

        node.mNodeMapNext = first;
        cNodeMapElementHandle.setVolatile(table, slot, node);

        return null;
    }

    /**
     * Replace a node which must be in the map already. Old and new node MUST have the same id.
     */
    void nodeMapReplace(final Node oldNode, final Node newNode) {
        final int hash = Long.hashCode(oldNode.id());
        final Node[] table = mNodeMapTable;
        final int slot = hash & (table.length - 1);

        var first = (Node) cNodeMapElementHandle.getVolatile(table, slot);
        if (first == null || (first = nodeMapLock(table, slot, first)) == null) {
            if (!isClosed()) {
                throw new AssertionError("Not found: " + oldNode + ", " + newNode);
            }
            return;
        }

        newNode.mNodeMapNext = oldNode.mNodeMapNext;

        if (first == oldNode) {
            first = newNode;
        } else {
            Node e = first;
            while (true) {
                Node next = e.mNodeMapNext;
                if (next == oldNode) {
                    e.mNodeMapNext = newNode;
                    break;
                }
                e = next;
                if (e == null) {
                    if (isClosed()) {
                        cNodeMapElementHandle.setVolatile(table, slot, first);
                        return;
                    }
                    throw new AssertionError("Not found: " + oldNode + ", " + newNode);
                }
            }
        }

        oldNode.mNodeMapNext = null;
        cNodeMapElementHandle.setVolatile(table, slot, first);
    }

    boolean nodeMapRemove(final Node node) {
        return nodeMapRemove(node, Long.hashCode(node.id()));
    }

    boolean nodeMapRemove(final Node node, final int hash) {
        final Node[] table = mNodeMapTable;
        final int slot = hash & (table.length - 1);

        var first = (Node) cNodeMapElementHandle.getVolatile(table, slot);

        if (first == null || (first = nodeMapLock(table, slot, first)) == null) {
            return false;
        }

        boolean found;

        if (first == node) {
            found = true;
            first = first.mNodeMapNext;
        } else {
            found = false;
            Node e = first;
            do {
                Node next = e.mNodeMapNext;
                if (next == node) {
                    found = true;
                    e.mNodeMapNext = next.mNodeMapNext;
                    break;
                }
                e = next;
            } while (e != null);
        }

        node.mNodeMapNext = null;
        cNodeMapElementHandle.setVolatile(table, slot, first);

        return found;
    }

    /**
     * Returns or loads the fragment node with the given id. If loaded, node is put in the cache.
     *
     * @return node with shared latch held
     */
    Node nodeMapLoadFragment(long nodeId) throws IOException {
        Node node = nodeMapGetShared(nodeId);

        if (node != null) {
            node.used();
            return node;
        }

        node = allocLatchedNode();
        node.id(nodeId);

        // node is currently exclusively locked. Insert it into the node map so that no other
        // thread tries to read it at the same time. If another thread sees it at this point
        // (before it is actually read), until the node is read, that thread will block trying
        // to get a shared lock.
        while (true) {
            Node existing = nodeMapPutIfAbsent(node);
            if (existing == null) {
                break;
            }

            // Was already loaded, or is currently being loaded.
            existing.acquireShared();
            if (nodeId == existing.id()) {
                // The item is already loaded. Throw away the node this thread was trying to
                // allocate.
                //
                // Even though node is not currently in the node map, it could have been in
                // there then got recycled. Other thread may still have a reference to it from
                // when it was in the node map. So its id needs to be invalidated.
                node.id(0);
                // This releases the exclusive latch and makes the node immediately usable for
                // new allocations.
                node.unused();
                return existing;
            }
            existing.releaseShared();
        }

        try {
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            readNode(node, nodeId);
        } catch (Throwable t) {
            // Something went wrong reading the node. Remove the node from the map, now that
            // it definitely won't get read.
            nodeMapRemove(node);
            node.id(0);
            node.releaseExclusive();
            throw t;
        }
        node.downgrade();

        return node;
    }

    /**
     * Returns or loads the fragment node with the given id. If loaded, node is put in the
     * cache. Method is intended for obtaining nodes to write into.
     *
     * @param read true if node should be fully read if it needed to be loaded
     * @return node with exclusive latch held
     */
    Node nodeMapLoadFragmentExclusive(long nodeId, boolean read) throws IOException {
        // Very similar to the nodeMapLoadFragment method. It has comments which explains
        // what's going on here. No point in duplicating that as well.

        Node node = nodeMapGetExclusive(nodeId);

        if (node != null) {
            node.used();
            return node;
        }

        node = allocLatchedNode();
        node.id(nodeId);

        while (true) {
            Node existing = nodeMapPutIfAbsent(node);
            if (existing == null) {
                break;
            }
            existing.acquireExclusive();
            if (nodeId == existing.id()) {
                node.id(0);
                node.unused();
                return existing;
            }
            existing.releaseExclusive();
        }

        try {
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            if (read) {
                readNode(node, nodeId);
            }
        } catch (Throwable t) {
            nodeMapRemove(node);
            node.id(0);
            node.releaseExclusive();
            throw t;
        }

        return node;
    }

    /**
     * @return exclusively latched node if found; null if not found
     */
    Node nodeMapGetAndRemove(long nodeId) {
        Node node = nodeMapGetExclusive(nodeId);
        if (node != null) {
            nodeMapRemove(node);
        }
        return node;
    }

    /**
     * Remove and delete nodes from map, as part of close sequence.
     */
    void nodeMapDeleteAll() {
        final Node[] table = mNodeMapTable;

        outer: for (int slot=0; slot<table.length; ) {
            var node = (Node) cNodeMapElementHandle.getVolatile(table, slot);

            if (node != null && (node = nodeMapLock(table, slot, node)) != null) {
                do {
                    if (!node.tryAcquireExclusive()) {
                        // Deadlock prevention.
                        cNodeMapElementHandle.setVolatile(table, slot, node);
                        Thread.yield();
                        continue outer;
                    }

                    try {
                        node.doDelete(this);
                        node.releaseExclusive();
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        cNodeMapElementHandle.setVolatile(table, slot, node);
                        throw e;
                    }

                    Node next = node.mNodeMapNext;
                    node.mNodeMapNext = null;
                    node = next;
                } while (node != null);

                cNodeMapElementHandle.setVolatile(table, slot, null);
            }

            slot++;
        }

        // Free up more memory in case something refers to this object for a long time.
        mNodeMapTable = null;
    }

    /**
     * With parent held shared, returns child with shared latch held, releasing the parent
     * latch. If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    final Node latchToChild(Node parent, int childPos) throws IOException {
        return latchChild(parent, childPos, Node.OPTION_PARENT_RELEASE_SHARED);
    }

    /**
     * With parent held shared, returns child with shared latch held, retaining the parent
     * latch. If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    final Node latchChildRetainParent(Node parent, int childPos) throws IOException {
        return latchChild(parent, childPos, 0);
    }

    /**
     * With parent held shared, returns child with shared latch held. If an exception is
     * thrown, parent and child latches are always released.
     *
     * @param option Node.OPTION_PARENT_RELEASE_SHARED or 0 to retain latch
     * @return child node, possibly split
     */
    final Node latchChild(Node parent, int childPos, int option) throws IOException {
        long childId = parent.retrieveChildRefId(childPos);
        Node childNode = nodeMapGetShared(childId);

        tryFind: if (childNode != null) {
            checkChild: {
                evictChild: if (childNode.mCachedState != Node.CACHED_CLEAN
                                && parent.mCachedState == Node.CACHED_CLEAN
                                // Must be a valid parent -- not a stub from Node.rootDelete.
                                && parent.id() > 1)
                {
                    // Parent was evicted before child. Evict child now and mark as clean. If
                    // this isn't done, the notSplitDirty method will short-circuit and not
                    // ensure that all the parent nodes are dirty. The splitting and merging
                    // code assumes that all nodes referenced by the cursor are dirty. The
                    // short-circuit check could be skipped, but then every change would
                    // require a full latch up the tree. Another option is to remark the parent
                    // as dirty, but this is dodgy and also requires a full latch up the tree.
                    // Parent-before-child eviction is infrequent, and so simple is better.

                    if (!childNode.tryUpgrade()) {
                        childNode.releaseShared();
                        childNode = nodeMapGetExclusive(childId);
                        if (childNode == null) {
                            break tryFind;
                        }
                        if (childNode.mCachedState == Node.CACHED_CLEAN) {
                            // Child state which was checked earlier changed when its latch was
                            // released, and now it shoudn't be evicted.
                            childNode.downgrade();
                            break evictChild;
                        }
                    }

                    if (option == Node.OPTION_PARENT_RELEASE_SHARED) {
                        parent.releaseShared();
                    }

                    try {
                        childNode.write(mPageDb);
                    } catch (Throwable e) {
                        childNode.releaseExclusive();
                        if (option == 0) {
                            // Release due to exception.
                            parent.releaseShared();
                        }
                        throw e;
                    }

                    childNode.mCachedState = Node.CACHED_CLEAN;
                    childNode.downgrade();
                    break checkChild;
                }

                if (option == Node.OPTION_PARENT_RELEASE_SHARED) {
                    parent.releaseShared();
                }
            }

            childNode.used();
            return childNode;
        }

        return parent.loadChild(this, childId, option);
    }

    /**
     * Variant of latchChildRetainParent which uses exclusive latches. With parent held
     * exclusively, returns child with exclusive latch held, retaining the parent latch. If an
     * exception is thrown, parent and child latches are always released.
     *
     * @param required pass false to allow null to be returned when child isn't immediately
     * latchable; passing false still permits the child to be loaded if necessary
     * @return child node, possibly split
     */
    final Node latchChildRetainParentEx(Node parent, int childPos, boolean required)
        throws IOException
    {
        long childId = parent.retrieveChildRefId(childPos);

        Node childNode;
        while (true) {
            childNode = nodeMapGet(childId);

            if (childNode != null) {
                if (required) {
                    childNode.acquireExclusive();
                } else if (!childNode.tryAcquireExclusive()) {
                    return null;
                }
                if (childId == childNode.id()) {
                    break;
                }
                childNode.releaseExclusive();
                continue;
            }

            return parent.loadChild(this, childId, Node.OPTION_CHILD_ACQUIRE_EXCLUSIVE);
        }

        if (childNode.mCachedState != Node.CACHED_CLEAN
            && parent.mCachedState == Node.CACHED_CLEAN
            // Must be a valid parent -- not a stub from Node.rootDelete.
            && parent.id() > 1)
        {
            // Parent was evicted before child. Evict child now and mark as clean. If
            // this isn't done, the notSplitDirty method will short-circuit and not
            // ensure that all the parent nodes are dirty. The splitting and merging
            // code assumes that all nodes referenced by the cursor are dirty. The
            // short-circuit check could be skipped, but then every change would
            // require a full latch up the tree. Another option is to remark the parent
            // as dirty, but this is dodgy and also requires a full latch up the tree.
            // Parent-before-child eviction is infrequent, and so simple is better.
            try {
                childNode.write(mPageDb);
            } catch (Throwable e) {
                childNode.releaseExclusive();
                // Release due to exception.
                parent.releaseExclusive();
                throw e;
            }
            childNode.mCachedState = Node.CACHED_CLEAN;
        }

        childNode.used();
        return childNode;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     */
    Node allocLatchedNode() throws IOException {
        return allocLatchedNode(0);
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, with an undefined id and a
     * clean state.
     *
     * @param mode MODE_UNEVICTABLE if allocated node cannot be automatically evicted
     */
    Node allocLatchedNode(int mode) throws IOException {
        mode |= mPageDb.allocMode();

        NodeGroup[] groups = mNodeGroups;
        int groupIx = ThreadLocalRandom.current().nextInt() & (groups.length - 1);
        IOException fail = null;

        for (int trial = 1; trial <= 3; trial++) {
            for (int i=0; i<groups.length; i++) {
                try {
                    Node node = groups[groupIx].tryAllocLatchedNode(trial, mode);
                    if (node != null) {
                        return node;
                    }
                } catch (IOException e) {
                    if (fail == null) {
                        fail = e;
                    }
                }
                if (--groupIx < 0) {
                    groupIx = groups.length - 1;
                }
            }

            checkClosed();

            // Try to free up nodes from unreferenced trees.
            cleanupUnreferencedTrees();
        }

        if (fail == null) {
            // Strict is false, to avoid deadlock when caller is holding latches.
            String stats = stats(false).toString();
            if (mPageDb.isDurable()) {
                throw new CacheExhaustedException(stats);
            } else {
                throw new DatabaseFullException(stats);
            }
        }

        if (fail instanceof DatabaseFullException) {
            throw fail;
        } else {
            throw new DatabaseFullException(fail);
        }
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, marked dirty, and with the
     * given id. Caller must be certain that the page with the given id can be written to.
     * Caller must also hold commit lock.
     *
     * The intent of this method is to reduce write stalls when the PageQueue drains full
     * nodes. If it needs to write another node in the process, then that's obviously not
     * helpful.
     *
     * When running in the fully memory mapped mode, this method always returns null because
     * writing to a mapped file is just a memory copy anyhow. There's no immediate write stall,
     * unless out of memory and swapping.
     *
     * @return null if another dirty node would need to be evicted
     */
    Node tryAllocRawDirtyNode(long id) throws IOException {
        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     return null;
        /*P*/ // }
        /*P*/ // ]

        NodeGroup[] groups = mNodeGroups;
        int groupIx = ThreadLocalRandom.current().nextInt(groups.length);

        Node node = groups[groupIx].tryAllocLatchedNode(1, NodeGroup.MODE_NO_EVICT);

        if (node != null) {
            /*P*/ // [
            node.type(Node.TYPE_FRAGMENT);
            /*P*/ // |
            /*P*/ // node.type(Node.TYPE_NONE);
            /*P*/ // ]
            node.id(id);
            node.mGroup.addDirty(node, mCommitState);
        }

        return node;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     */
    Node allocDirtyNode() throws IOException {
        return allocDirtyNode(0);
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively, marked
     * dirty and unevictable. Caller must hold commit lock.
     *
     * @param mode MODE_UNEVICTABLE if allocated node cannot be automatically evicted
     */
    Node allocDirtyNode(int mode) throws IOException {
        Node node = mPageDb.allocLatchedNode(this, mode);

        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     node.mPage = mPageDb.dirtyPage(node.id());
        /*P*/ // }
        /*P*/ // ]

        node.mGroup.addDirty(node, mCommitState);
        return node;
    }

    /**
     * Returns a new or recycled Node instance, latched exclusively and marked
     * dirty. Caller must hold commit lock.
     */
    Node allocDirtyFragmentNode() throws IOException {
        Node node = allocDirtyNode();
        nodeMapPut(node);
        /*P*/ // [
        node.type(TYPE_FRAGMENT);
        /*P*/ // ]
        return node;
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean isMutable(Node node) {
        return node.mCachedState == mCommitState && node.id() > 1;
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(Node node) {
        return node.mCachedState != mCommitState && node.id() >= 0;
    }

    /**
     * Mark a tree node as dirty. Caller must hold commit lock and exclusive latch on
     * node. Method does nothing if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(BTree tree, Node node) throws IOException {
        if (node.mCachedState == mCommitState || node.id() < 0) {
            return false;
        } else {
            doMarkDirty(tree, node);
            return true;
        }
    }

    /**
     * Mark a fragment node as dirty. Caller must hold commit lock and exclusive latch on
     * node. Method does nothing if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markFragmentDirty(Node node) throws IOException {
        if (node.mCachedState == mCommitState) {
            return false;
        } else {
            if (node.mCachedState != CACHED_CLEAN) {
                node.write(mPageDb);
            }

            long newId = mPageDb.allocPage();
            long oldId = node.id();

            if (oldId != 0) {
                // Must be removed from map before page is deleted. It could be recycled too
                // soon, creating a NodeMap collision.
                boolean removed = nodeMapRemove(node, Long.hashCode(oldId));

                try {
                    // No need to force delete when dirtying. Caller is responsible for
                    // cleaning up.
                    mPageDb.deletePage(oldId, false);
                } catch (Throwable e) {
                    // Try to undo things.
                    if (removed) {
                        try {
                            nodeMapPut(node);
                        } catch (Throwable e2) {
                            Utils.suppress(e, e2);
                        }
                    }
                    try {
                        mPageDb.recyclePage(newId);
                    } catch (Throwable e2) {
                        // Panic.
                        Utils.suppress(e, e2);
                        close(e);
                    }
                    throw e;
                }
            }

            dirty(node, newId);
            nodeMapPut(node);
            return true;
        }
    }

    /**
     * Mark an unmapped node as dirty (used by UndoLog). Caller must hold commit lock and
     * exclusive latch on node. Method does nothing if node is already dirty. Latch is never
     * released by this method, even if an exception is thrown.
     */
    void markUnmappedDirty(Node node) throws IOException {
        if (node.mCachedState != mCommitState) {
            if (node.mCachedState != CACHED_CLEAN) {
                node.write(mPageDb);
            }

            long newId = mPageDb.allocPage();
            long oldId = node.id();

            try {
                // No need to force delete when dirtying. Caller is responsible for cleaning up.
                mPageDb.deletePage(oldId, false);
            } catch (Throwable e) {
                try {
                    mPageDb.recyclePage(newId);
                } catch (Throwable e2) {
                    // Panic.
                    Utils.suppress(e, e2);
                    close(e);
                }
                throw e;
            }

            dirty(node, newId);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(BTree tree, Node node) throws IOException {
        if (node.mCachedState != CACHED_CLEAN) {
            node.write(mPageDb);
        }

        long newId = mPageDb.allocPage();
        long oldId = node.id();

        try {
            if (node == tree.mRoot) {
                storeTreeRootId(tree, newId);
            }
        } catch (Throwable e) {
            try {
                mPageDb.recyclePage(newId);
            } catch (Throwable e2) {
                // Panic.
                Utils.suppress(e, e2);
                close(e);
            }
            throw e;
        }

        if (oldId != 0) {
            // Must be removed from map before page is deleted. It could be recycled too soon,
            // creating a NodeMap collision.
            boolean removed = nodeMapRemove(node, Long.hashCode(oldId));

            try {
                // No need to force delete when dirtying. Caller is responsible for cleaning up.
                mPageDb.deletePage(oldId, false);
            } catch (Throwable e) {
                // Try to undo things.
                if (removed) {
                    try {
                        nodeMapPut(node);
                    } catch (Throwable e2) {
                        Utils.suppress(e, e2);
                    }
                }
                try {
                    if (node == tree.mRoot) {
                        storeTreeRootId(tree, oldId);
                    }
                    mPageDb.recyclePage(newId);
                } catch (Throwable e2) {
                    // Panic.
                    Utils.suppress(e, e2);
                    close(e);
                }
                throw e;
            }
        }

        dirty(node, newId);
        nodeMapPut(node);
    }

    private void storeTreeRootId(BTree tree, long id) throws IOException {
        if (tree.mIdBytes != null) {
            var encodedId = new byte[8];
            encodeLongLE(encodedId, 0, id);
            mRegistry.store(Transaction.BOGUS, tree.mIdBytes, encodedId);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node.
     */
    private void dirty(Node node, long newId) throws IOException {
        /*P*/ // [|
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     if (node.mPage == p_nonTreePage()) {
        /*P*/ //         node.mPage = mPageDb.dirtyPage(newId);
        /*P*/ //         node.asEmptyRoot();
        /*P*/ //     } else if (node.mPage != p_closedTreePage()) {
        /*P*/ //         node.mPage = mPageDb.copyPage(node.id(), newId); // copy on write
        /*P*/ //     }
        /*P*/ // }
        /*P*/ // ]

        node.id(newId);
        node.mGroup.addDirty(node, mCommitState);
    }

    /**
     * Remove the old node from the dirty list and swap in the new node. Caller must hold
     * commit lock and latched the old node. The cached state of the nodes is not altered.
     * Both nodes must belong to the same group.
     */
    void swapIfDirty(Node oldNode, Node newNode) {
        oldNode.mGroup.swapIfDirty(oldNode, newNode);
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Latch is always released by
     * this method, even if an exception is thrown.
     */
    void deleteNode(Node node) throws IOException {
        deleteNode(node, true);
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Latch is always released by
     * this method, even if an exception is thrown.
     */
    void deleteNode(Node node, boolean canRecycle) throws IOException {
        prepareToDelete(node);
        finishDeleteNode(node, canRecycle);
    }

    /**
     * Similar to markDirty method except no new page is reserved, and old page
     * is not immediately deleted. Caller must hold commit lock and exclusive
     * latch on node. Latch is never released by this method, unless an
     * exception is thrown.
     */
    void prepareToDelete(Node node) throws IOException {
        // Hello. My name is Inigo Montoya. You killed my father. Prepare to die. 
        if (node.mCachedState == mCheckpointFlushState) {
            // Node must be committed with the current checkpoint, and so
            // it must be written out before it can be deleted.
            try {
                node.write(mPageDb);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. The
     * prepareToDelete method must have been called first. Latch is always
     * released by this method, even if an exception is thrown.
     */
    void finishDeleteNode(Node node) throws IOException {
        finishDeleteNode(node, true);
    }

    /**
     * @param canRecycle true if node's page can be immediately re-used
     */
    void finishDeleteNode(Node node, boolean canRecycle) throws IOException {
        try {
            long id = node.id();

            if (id != 0) {
                // Must be removed from map before page is deleted. It could be recycled too
                // soon, creating a NodeMap collision.
                boolean removed = nodeMapRemove(node, Long.hashCode(id));

                try {
                    if (canRecycle && node.mCachedState == mCommitState) {
                        // Newly reserved page was never used, so recycle it.
                        mPageDb.recyclePage(id);
                    } else {
                        // Old data must survive until after checkpoint. Must force the delete,
                        // because by this point, the caller can't easily clean up.
                        mPageDb.deletePage(id, true);
                    }
                } catch (Throwable e) {
                    // Try to undo things.
                    if (removed) {
                        try {
                            nodeMapPut(node);
                        } catch (Throwable e2) {
                            Utils.suppress(e, e2);
                        }
                    }
                    throw e;
                }

                // When id is <= 1, it won't be moved to a secondary cache. Preserve the
                // original id for non-durable database to recycle it. Durable database relies
                // on the free list.
                node.id(-id);
            }

            // When node is re-allocated, it will be evicted. Ensure that eviction
            // doesn't write anything.
            node.mCachedState = CACHED_CLEAN;
        } catch (Throwable e) {
            node.releaseExclusive();
            // Panic.
            close(e);
            throw e;
        }

        // Always releases the node latch.
        node.unused();
    }

    final byte[] fragmentKey(byte[] key) throws IOException {
        return fragment(key, key.length, mMaxKeySize);
    }

    final byte[] fragment(final byte[] value, final long vlength, int max)
        throws IOException
    {
        return fragment(value, vlength, max, 65535);
    }

    /**
     * Breakup a large value into separate pages, returning a new value which
     * encodes the page references. Caller must hold commit lock.
     *
     * Returned value begins with a one byte header:
     *
     * 0b0000_ffip
     *
     * The leading 4 bits define the encoding type, which must be 0. The 'f' bits define the
     * full value length field size: 2, 4, 6, or 8 bytes. The 'i' bit defines the inline
     * content length field size: 0 or 2 bytes. The 'p' bit is clear if direct pointers are
     * used, and set for indirect pointers. Pointers are always 6 bytes.
     *
     * @param value can be null if value is all zeros
     * @param max maximum allowed size for returned byte array; must not be
     * less than 11 {@literal (can be 9 if full value length is < 65536)}
     * @param maxInline maximum allowed inline size; must not be more than 65535
     * @return null if max is too small
     */
    final byte[] fragment(final byte[] value, final long vlength, int max, int maxInline)
        throws IOException
    {
        final int pageSize = mPageSize;
        long pageCount = vlength / pageSize;
        final int remainder = (int) (vlength % pageSize);

        if (vlength >= 65536) {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 4 + 6);
        } else if (pageCount == 0 && remainder <= (max - (1 + 2 + 2))) {
            // Entire value fits inline. It didn't really need to be
            // encoded this way, but do as we're told.
            var newValue = new byte[(1 + 2 + 2) + (int) vlength];
            newValue[0] = 0x02; // ff=0, i=1, p=0
            encodeShortLE(newValue, 1, (int) vlength);     // full length
            encodeShortLE(newValue, 1 + 2, (int) vlength); // inline length
            arrayCopyOrFill(value, 0, newValue, (1 + 2 + 2), (int) vlength);
            return newValue;
        } else {
            // Subtract header size, full length field size, and size of one pointer.
            max -= (1 + 2 + 6);
        }

        if (max < 0) {
            return null;
        }

        long pointerSpace = pageCount * 6;

        byte[] newValue;
        final int inline; // length of inline field size
        if (remainder <= max && remainder <= maxInline
            && (pointerSpace <= (max + 6 - (inline = remainder == 0 ? 0 : 2) - remainder)))
        {
            // Remainder fits inline, minimizing internal fragmentation. All
            // extra pages will be full. All pointers fit too; encode direct.

            // Conveniently, 2 is the header bit and the inline length field size.
            byte header = (byte) inline;
            final int offset;
            if (vlength < (1L << (2 * 8))) {
                // (2 byte length field)
                offset = 1 + 2;
            } else if (vlength < (1L << (4 * 8))) {
                header |= 0x04; // ff = 1 (4 byte length field)
                offset = 1 + 4;
            } else if (vlength < (1L << (6 * 8))) {
                header |= 0x08; // ff = 2 (6 byte length field)
                offset = 1 + 6;
            } else {
                header |= 0x0c; // ff = 3 (8 byte length field)
                offset = 1 + 8;
            }

            int poffset = offset + inline + remainder;
            newValue = new byte[poffset + (int) pointerSpace];
            if (pageCount > 0) {
                if (value == null) {
                    // Value is sparse, so just fill with null pointers.
                    fill(newValue, poffset, poffset + ((int) pageCount) * 6, (byte) 0);
                } else {
                    try {
                        int voffset = remainder;
                        while (true) {
                            Node node = allocDirtyFragmentNode();
                            try {
                                encodeInt48LE(newValue, poffset, node.id());
                                p_copyFromArray(value, voffset, node.mPage, 0, pageSize);
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
                    } catch (DatabaseException e) {
                        if (!e.isRecoverable()) {
                            close(e);
                        } else {
                            try {
                                // Clean up the mess.
                                while ((poffset -= 6) >= (offset + inline + remainder)) {
                                    deleteFragment(decodeUnsignedInt48LE(newValue, poffset));
                                }
                            } catch (Throwable e2) {
                                suppress(e, e2);
                                close(e);
                            }
                        }
                        throw e;
                    }
                }
            }

            newValue[0] = header;

            if (remainder != 0) {
                encodeShortLE(newValue, offset, remainder); // inline length
                arrayCopyOrFill(value, 0, newValue, offset + 2, remainder);
            }
        } else {
            // Remainder doesn't fit inline, so don't encode any inline
            // content. Last extra page will not be full.
            pageCount++;
            pointerSpace += 6;

            byte header;
            final int offset;
            if (vlength < (1L << (2 * 8))) {
                header = 0x00; // ff = 0, i=0
                offset = 1 + 2;
            } else if (vlength < (1L << (4 * 8))) {
                header = 0x04; // ff = 1, i=0
                offset = 1 + 4;
            } else if (vlength < (1L << (6 * 8))) {
                header = 0x08; // ff = 2, i=0
                offset = 1 + 6;
            } else {
                header = 0x0c; // ff = 3, i=0
                offset = 1 + 8;
            }

            if (pointerSpace <= (max + 6)) {
                // All pointers fit, so encode as direct.
                newValue = new byte[offset + (int) pointerSpace];
                if (pageCount > 0) {
                    if (value == null) {
                        // Value is sparse, so just fill with null pointers.
                        fill(newValue, offset, offset + ((int) pageCount) * 6, (byte) 0);
                    } else {
                        int poffset = offset;
                        try {
                            int voffset = 0;
                            while (true) {
                                Node node = allocDirtyFragmentNode();
                                try {
                                    encodeInt48LE(newValue, poffset, node.id());
                                    var page = node.mPage;
                                    if (pageCount > 1) {
                                        p_copyFromArray(value, voffset, page, 0, pageSize);
                                    } else {
                                        p_copyFromArray(value, voffset, page, 0, remainder);
                                        // Zero fill the rest, making it easier to extend later.
                                        p_clear(page, remainder, pageSize(page));
                                        break;
                                    }
                                } finally {
                                    node.releaseExclusive();
                                }
                                pageCount--;
                                poffset += 6;
                                voffset += pageSize;
                            }
                        } catch (DatabaseException e) {
                            if (!e.isRecoverable()) {
                                close(e);
                            } else {
                                try {
                                    // Clean up the mess.
                                    while ((poffset -= 6) >= offset) {
                                        deleteFragment(decodeUnsignedInt48LE(newValue, poffset));
                                    }
                                } catch (Throwable e2) {
                                    suppress(e, e2);
                                    close(e);
                                }
                            }
                            throw e;
                        }
                    }
                }
            } else {
                // Use indirect pointers.
                header |= 0x01;
                newValue = new byte[offset + 6];
                if (value == null) {
                    // Value is sparse, so just store a null pointer.
                    encodeInt48LE(newValue, offset, 0);
                } else {
                    int levels = calculateInodeLevels(vlength);
                    Node inode = allocDirtyFragmentNode();
                    try {
                        encodeInt48LE(newValue, offset, inode.id());
                        writeMultilevelFragments(levels, inode, value, 0, vlength);
                        inode.releaseExclusive();
                    } catch (DatabaseException e) {
                        if (!e.isRecoverable()) {
                            close(e);
                        } else {
                            try {
                                // Clean up the mess. Note that inode is still latched here,
                                // because writeMultilevelFragments never releases it. The call to
                                // deleteMultilevelFragments always releases the inode latch.
                                deleteMultilevelFragments(levels, inode, vlength);
                            } catch (Throwable e2) {
                                suppress(e, e2);
                                close(e);
                            }
                        }
                        throw e;
                    } catch (Throwable e) {
                        close(e);
                        throw e;
                    }
                }
            }

            newValue[0] = header;
        }

        // Encode full length field.
        if (vlength < (1L << (2 * 8))) {
            encodeShortLE(newValue, 1, (int) vlength);
        } else if (vlength < (1L << (4 * 8))) {
            encodeIntLE(newValue, 1, (int) vlength);
        } else if (vlength < (1L << (6 * 8))) {
            encodeInt48LE(newValue, 1, vlength);
        } else {
            encodeLongLE(newValue, 1, vlength);
        }

        return newValue;
    }

    int calculateInodeLevels(long vlength) {
        long[] caps = mFragmentInodeLevelCaps;
        int levels = 0;
        while (levels < caps.length) {
            if (vlength <= caps[levels]) {
                break;
            }
            levels++;
        }
        return levels;
    }

    static long decodeFullFragmentedValueLength(int header, /*P*/ byte[] fragmented, int off) {
        switch ((header >> 2) & 0x03) {
        default:
            return p_ushortGetLE(fragmented, off);
        case 1:
            return p_intGetLE(fragmented, off) & 0xffffffffL;
        case 2:
            return p_uint48GetLE(fragmented, off);
        case 3:
            return p_longGetLE(fragmented, off);
        }
    }

    /**
     * @param level inode level; at least 1
     * @param inode exclusive latched parent inode; never released by this method
     * @param value slice of complete value being fragmented
     */
    private void writeMultilevelFragments(int level, Node inode,
                                          byte[] value, int voffset, long vlength)
        throws IOException
    {
        var page = inode.mPage;
        level--;
        long levelCap = levelCap(level);

        int childNodeCount = childNodeCount(vlength, levelCap);

        int poffset = 0;
        try {
            for (int i=0; i<childNodeCount; i++) {
                Node childNode = allocDirtyFragmentNode();
                p_int48PutLE(page, poffset, childNode.id());
                poffset += 6;

                int len = (int) Math.min(levelCap, vlength);
                if (level <= 0) {
                    var childPage = childNode.mPage;
                    p_copyFromArray(value, voffset, childPage, 0, len);
                    // Zero fill the rest, making it easier to extend later.
                    p_clear(childPage, len, pageSize(childPage));
                    childNode.releaseExclusive();
                } else {
                    try {
                        writeMultilevelFragments(level, childNode, value, voffset, len);
                    } finally {
                        childNode.releaseExclusive();
                    }
                }

                vlength -= len;
                voffset += len;
            }
        } finally {
            // Zero fill the rest, making it easier to extend later. If an exception was
            // thrown, this simplies cleanup. All of the allocated pages are referenced,
            // but the rest are not.
            p_clear(page, poffset, pageSize(page));
        }
    }

    /**
     * Determine the multi-level fragmented value child node count, at a specific level.
     */
    private static int childNodeCount(long vlength, long levelCap) {
        int count = (int) ((vlength + (levelCap - 1)) / levelCap);
        if (count < 0) {
            // Overflowed.
            count = childNodeCountOverflow(vlength, levelCap);
        }
        return count;
    }

    private static int childNodeCountOverflow(long vlength, long levelCap) {
        return BigInteger.valueOf(vlength).add(BigInteger.valueOf(levelCap - 1))
            .divide(BigInteger.valueOf(levelCap)).intValue();
    }

    /**
     * Reconstruct a fragmented key.
     */
    byte[] reconstructKey(/*P*/ byte[] fragmented, int off, int len) throws IOException {
        try {
            return reconstruct(fragmented, off, len);
        } catch (LargeValueException e) {
            throw new LargeKeyException(e.length(), e.getCause());
        }
    }

    /**
     * Reconstruct a fragmented value.
     */
    byte[] reconstruct(/*P*/ byte[] fragmented, int off, int len) throws IOException {
        return reconstruct(fragmented, off, len, null);
    }

    /**
     * Reconstruct a fragmented value.
     *
     * @param stats non-null for stats: [0]: full length, [1]: number of pages
     * {@literal (>0 if fragmented)}
     * @return null if stats requested
     */
    byte[] reconstruct(/*P*/ byte[] fragmented, int off, int len, long[] stats)
        throws IOException
    {
        int header = p_byteGet(fragmented, off++);
        len--;

        long vLen;
        switch ((header >> 2) & 0x03) {
        default:
            vLen = p_ushortGetLE(fragmented, off);
            break;

        case 1:
            vLen = p_intGetLE(fragmented, off);
            if (vLen < 0) {
                vLen &= 0xffffffffL;
                if (stats == null) {
                    throw new LargeValueException(vLen);
                }
            }
            break;

        case 2:
            vLen = p_uint48GetLE(fragmented, off);
            if (vLen > Integer.MAX_VALUE && stats == null) {
                throw new LargeValueException(vLen);
            }
            break;

        case 3:
            vLen = p_longGetLE(fragmented, off);
            if (vLen < 0 || (vLen > Integer.MAX_VALUE && stats == null)) {
                throw new LargeValueException(vLen);
            }
            break;
        }

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        byte[] value;
        if (stats != null) {
            stats[0] = vLen;
            value = null;
        } else {
            try {
                value = new byte[(int) vLen];
            } catch (OutOfMemoryError e) {
                throw new LargeValueException(vLen, e);
            }
        }

        int vOff = 0;
        if ((header & 0x02) != 0) {
            // Inline content.
            int inLen = p_ushortGetLE(fragmented, off);
            off += 2;
            len -= 2;
            if (value != null) {
                p_copyToArray(fragmented, off, value, vOff, inLen);
            }
            off += inLen;
            len -= inLen;
            vOff += inLen;
            vLen -= inLen;
        }

        long pagesRead = 0;

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = p_uint48GetLE(fragmented, off);
                off += 6;
                len -= 6;
                int pLen;
                if (nodeId == 0) {
                    // Reconstructing a sparse value. Array is already zero-filled.
                    pLen = Math.min((int) vLen, mPageSize);
                } else {
                    Node node = nodeMapLoadFragment(nodeId);
                    pagesRead++;
                    try {
                        var page = node.mPage;
                        pLen = Math.min((int) vLen, pageSize(page));
                        if (value != null) {
                            p_copyToArray(page, 0, value, vOff, pLen);
                        }
                    } finally {
                        node.releaseShared();
                    }
                }
                vOff += pLen;
                vLen -= pLen;
            }
        } else {
            // Indirect pointers.
            long inodeId = p_uint48GetLE(fragmented, off);
            if (inodeId != 0) {
                Node inode = nodeMapLoadFragment(inodeId);
                pagesRead++;
                int levels = calculateInodeLevels(vLen);
                pagesRead += readMultilevelFragments(levels, inode, value, vOff, vLen);
            }
        }

        if (stats != null) {
            stats[1] = pagesRead;
        }

        return value;
    }

    /**
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param value slice of complete value being reconstructed; initially filled with zeros;
     * pass null for stats only
     * @return number of pages read
     */
    private long readMultilevelFragments(int level, Node inode,
                                         byte[] value, int voffset, long vlength)
        throws IOException
    {
        try {
            long pagesRead = 0;

            var page = inode.mPage;
            level--;
            long levelCap = levelCap(level);

            int childNodeCount = childNodeCount(vlength, levelCap);

            for (int poffset = 0, i=0; i<childNodeCount; poffset += 6, i++) {
                long childNodeId = p_uint48GetLE(page, poffset);
                int len = (int) Math.min(levelCap, vlength);

                if (childNodeId != 0) {
                    Node childNode = nodeMapLoadFragment(childNodeId);
                    pagesRead++;
                    if (level <= 0) {
                        if (value != null) {
                            p_copyToArray(childNode.mPage, 0, value, voffset, len);
                        }
                        childNode.releaseShared();
                    } else {
                        pagesRead += readMultilevelFragments
                            (level, childNode, value, voffset, len);
                    }
                }

                vlength -= len;
                voffset += len;
            }

            return pagesRead;
        } finally {
            inode.releaseShared();
        }
    }

    /**
     * Delete the extra pages of a fragmented value. Caller must hold commit lock.
     *
     * @param fragmented page containing fragmented value 
     */
    void deleteFragments(/*P*/ byte[] fragmented, int off, int len)
        throws IOException
    {
        int header = p_byteGet(fragmented, off++);
        len--;

        long vLen;
        if ((header & 0x01) == 0) {
            // Don't need to read the value length when deleting direct pointers.
            vLen = 0;
        } else {
            switch ((header >> 2) & 0x03) {
            default:
                vLen = p_ushortGetLE(fragmented, off);
                break;
            case 1:
                vLen = p_intGetLE(fragmented, off) & 0xffffffffL;
                break;
            case 2:
                vLen = p_uint48GetLE(fragmented, off);
                break;
            case 3:
                vLen = p_longGetLE(fragmented, off);
                break;
            }
        }

        {
            int vLenFieldSize = 2 + ((header >> 1) & 0x06);
            off += vLenFieldSize;
            len -= vLenFieldSize;
        }

        if ((header & 0x02) != 0) {
            // Skip inline content.
            int inLen = 2 + p_ushortGetLE(fragmented, off);
            off += inLen;
            len -= inLen;
        }

        if ((header & 0x01) == 0) {
            // Direct pointers.
            while (len >= 6) {
                long nodeId = p_uint48GetLE(fragmented, off);
                off += 6;
                len -= 6;
                deleteFragment(nodeId);
            }
        } else {
            // Indirect pointers.
            long inodeId = p_uint48GetLE(fragmented, off);
            if (inodeId != 0) {
                Node inode = removeInode(inodeId);
                int levels = calculateInodeLevels(vLen);
                deleteMultilevelFragments(levels, inode, vLen);
            }
        }
    }

    /**
     * @param level inode level; at least 1
     * @param inode exclusive latched parent inode; always released by this method
     */
    private void deleteMultilevelFragments(int level, Node inode, long vlength)
        throws IOException
    {
        var page = inode.mPage;
        level--;
        long levelCap = levelCap(level);

        // Copy all child node ids and release parent latch early.
        int childNodeCount = childNodeCount(vlength, levelCap);
        var childNodeIds = new long[childNodeCount];
        for (int poffset = 0, i=0; i<childNodeCount; poffset += 6, i++) {
            childNodeIds[i] = p_uint48GetLE(page, poffset);
        }
        deleteNode(inode);

        if (level <= 0) for (long childNodeId : childNodeIds) {
            deleteFragment(childNodeId);
        } else for (long childNodeId : childNodeIds) {
            long len = Math.min(levelCap, vlength);
            if (childNodeId != 0) {
                Node childNode = removeInode(childNodeId);
                deleteMultilevelFragments(level, childNode, len);
            }
            vlength -= len;
        }
    }

    /**
     * @param nodeId must not be zero
     * @return non-null Node with exclusive latch held
     */
    private Node removeInode(long nodeId) throws IOException {
        Node node = nodeMapGetAndRemove(nodeId);
        if (node == null) {
            node = allocLatchedNode(NodeGroup.MODE_UNEVICTABLE);
            /*P*/ // [
            node.type(TYPE_FRAGMENT);
            /*P*/ // ]
            try {
                readNode(node, nodeId);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }
        return node;
    }

    /**
     * @param nodeId can be zero
     */
    void deleteFragment(long nodeId) throws IOException {
        if (nodeId != 0) {
            Node node = nodeMapGetAndRemove(nodeId);
            if (node != null) {
                deleteNode(node);
            } else try {
                if (mInitialReadState != CACHED_CLEAN) {
                    // Page was never used if nothing has ever been checkpointed.
                    mPageDb.recyclePage(nodeId);
                } else {
                    // Page is clean if not in a Node, and so it must survive until after the
                    // next checkpoint. Must force the delete, because by this point, the
                    // caller can't easily clean up.
                    mPageDb.deletePage(nodeId, true);
                }
            } catch (Throwable e) {
                // Panic.
                close(e);
                throw e;
            }
        }
    }

    private static long[] calculateInodeLevelCaps(int pageSize) {
        var caps = new long[10];
        long cap = pageSize;
        long scalar = pageSize / 6; // 6-byte pointers

        int i = 0;
        while (i < caps.length) {
            caps[i++] = cap;
            long next = cap * scalar;
            if (next / scalar != cap) {
                caps[i++] = Long.MAX_VALUE;
                break;
            }
            cap = next;
        }

        if (i < caps.length) {
            var newCaps = new long[i];
            arraycopy(caps, 0, newCaps, 0, i);
            caps = newCaps;
        }

        return caps;
    }

    long levelCap(int level) {
        return mFragmentInodeLevelCaps[level];
    }

    /**
     * Obtain the trash for transactionally deleting fragmented values.
     */
    BTree fragmentedTrash() throws IOException {
        BTree trash = mFragmentedTrash;
        return trash != null ? trash : openFragmentedTrash(IX_CREATE);
    }

    /**
     * Try to obtain the trash for transactionally deleting fragmented values. Returns null if
     * it doesn't exist.
     */
    BTree tryFragmentedTrash() throws IOException {
        BTree trash = mFragmentedTrash;
        return trash != null ? trash : openFragmentedTrash(IX_FIND);
    }

    /**
     * @param ixOption IX_FIND or IX_CREATE
     */
    private BTree openFragmentedTrash(long ixOption) throws IOException {
        BTree trash;

        mOpenTreesLatch.acquireExclusive();
        try {
            if ((trash = mFragmentedTrash) == null) {
                mFragmentedTrash = trash = openInternalTree(Tree.FRAGMENTED_TRASH_ID, ixOption);
            }
        } finally {
            mOpenTreesLatch.releaseExclusive();
        }

        return trash;
    }

    /**
     * Non-transactionally deletes all fragmented values except for those that are still active.
     *
     * @param activeTxns pass null to delete from non-replicated (no redo) transactions;
     * otherwise, only delete from replicated transactions that aren't in this hashtable
     */
    void emptyLingeringTrash(LHashTable<?> activeTxns) throws IOException {
        mOpenTreesLatch.acquireExclusive();
        BTree trash = mFragmentedTrash;

        if (trash != null) {
            mOpenTreesLatch.releaseExclusive();
            FragmentedTrash.emptyLingeringTrash(trash, activeTxns);
            return;
        }

        try {
            trash = openInternalTree(Tree.FRAGMENTED_TRASH_ID, IX_FIND);
            if (trash == null) {
                mOpenTreesLatch.releaseExclusive();
                return;
            }
        } catch (Throwable e) {
            mOpenTreesLatch.releaseExclusive();
            throw e;
        }

        mOpenTreesLatch.downgrade();
        try {
            FragmentedTrash.emptyLingeringTrash(trash, activeTxns);
        } finally {
            mOpenTreesLatch.releaseShared();
            trash.forceClose();
        }
    }

    /**
     * Reads the node page, sets the id and cached state. Node must be latched exclusively.
     */
    void readNode(Node node, long id) throws IOException {
        /*P*/ // [
        mPageDb.readPage(id, node.mPage);
        /*P*/ // |
        /*P*/ // if (mFullyMapped) {
        /*P*/ //     node.mPage = mPageDb.directPagePointer(id);
        /*P*/ // } else {
        /*P*/ //     mPageDb.readPage(id, node.mPage);
        /*P*/ // }
        /*P*/ // ]

        node.id(id);

        // NOTE: If initial state is clean, an optimization is possible, but it's a bit
        // tricky. Too many pages are allocated when evictions are high, write rate is high,
        // and commits are bogged down.  Keep some sort of cache of ids known to be dirty. If
        // reloaded before commit, then they're still dirty.
        //
        // A Bloom filter is not appropriate, because of false positives. A random evicting
        // cache works well -- it has no collision chains. Evict whatever else was there in
        // the slot. An array of longs should suffice.
        //
        // When a child node is loaded with a dirty state, the parent nodes must be updated
        // as well. This might force them to be evicted, and then the optimization is
        // lost. A better approach would avoid the optimization if the parent node is clean
        // or doesn't match the current commit state.

        if ((node.mCachedState = mInitialReadState) != CACHED_CLEAN) {
            node.mGroup.addDirty(node, node.mCachedState);
        }
    }

    @Override
    EventListener eventListener() {
        return mEventListener;
    }

    @Override
    void checkpoint(long sizeThreshold, long delayThresholdNanos) throws IOException {
        checkpoint(0, sizeThreshold, delayThresholdNanos);
    }

    private void forceCheckpoint() throws IOException {
        checkpoint(1, 0, 0);
    }

    /**
     * @param force 0: no force, 1: force if not closed, -1: force even if closed
     */
    private void checkpoint(int force, long sizeThreshold, long delayThresholdNanos)
        throws IOException
    {
        while (!isClosed() && mPageDb.isDurable()) {
            // Checkpoint lock ensures consistent state between page store and logs.
            mCheckpointLock.lock();
            try {
                doCheckpoint(force, sizeThreshold, delayThresholdNanos);
                return;
            } catch (Throwable e) {
                if (!isRecoverable(e)) {
                    // Panic.
                    closeQuietly(this, e);
                    throw e;
                }

                try {
                    cleanupMasterUndoLog();
                } catch (Throwable e2) {
                    // Panic.
                    closeQuietly(this, e2);
                    suppress(e2, e);
                    throw e2;
                }

                // Retry and don't rethrow if leadership was lost.
                if (!(e instanceof UnmodifiableReplicaException)) {
                    throw e;
                }
            } finally {
                mCheckpointLock.unlock();
            }

            Thread.yield();
        }
    }

    /**
     * Caller must hold mCheckpointLock.
     */
    private void cleanupMasterUndoLog() throws IOException {
        if (mCommitMasterUndoLog == null) {
            return;
        }

        LHashTable.Obj<Object> committed;
        CommitLock.Shared shared = mCommitLock.acquireSharedUnchecked();
        try {
            if (isClosed()) {
                return;
            }
            committed = mCommitMasterUndoLog.findCommitted();
        } finally {
            shared.release();
        }

        if (committed == null) {
            return;
        }

        loop: while (true) {
            checkAll: {
                for (TransactionContext txnContext : mTxnContexts) {
                    if (txnContext.anyActive(committed)) {
                        break checkAll;
                    }
                }
                break loop;
            }

            if (isClosed()) {
                return;
            }

            // Wait with a sleep. Crude, but it means that no special condition variable is
            // required. Considering that this method is only expected to be called when
            // leadership is lost during a checkpoint, there's no reason to be immediate.
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        LHashTable.Obj<Object> uncommitted = null;

        for (TransactionContext txnContext : mTxnContexts) {
            uncommitted = txnContext.moveUncommitted(uncommitted);
        }

        if (uncommitted != null) {
            shared = mCommitLock.acquireSharedUnchecked();
            try {
                if (isClosed()) {
                    return;
                }
                mCommitMasterUndoLog.markUncommitted(uncommitted);
            } finally {
                shared.release();
            }
        }
    }

    /**
     * Caller must hold mCheckpointLock.
     *
     * @param force 0: no force, 1: force if not closed, -1: force even if closed
     */
    private void doCheckpoint(int force, long sizeThreshold, long delayThresholdNanos)
        throws IOException
    {
        if (force >= 0 && isClosed()) {
            return;
        }

        // Now's a good time to clean things up.
        cleanupUnreferencedTrees();

        final Node root = mRegistry.mRoot;

        var header = mCommitHeader;

        long nowNanos = System.nanoTime();

        if (force == 0 && header == p_null()) {
            thresholdCheck : {
                if (delayThresholdNanos == 0) {
                    break thresholdCheck;
                }

                if (delayThresholdNanos > 0 &&
                    ((nowNanos - mLastCheckpointNanos) >= delayThresholdNanos))
                {
                    break thresholdCheck;
                }

                if (mRedoWriter == null || mRedoWriter.shouldCheckpoint(sizeThreshold)) {
                    break thresholdCheck;
                }

                // Thresholds not met for a full checkpoint, but fully sync the redo log
                // for durability.
                flush(2); // flush and sync metadata

                return;
            }

            // Thresholds for a checkpoint are met, but it might not be necessary.

            boolean full = false;

            root.acquireShared();
            try {
                if (root.mCachedState != CACHED_CLEAN) {
                    // Root is dirty, so do a full checkpoint.
                    full = true;
                }
            } finally {
                root.releaseShared();
            }

            if (!full && mRedoWriter != null && (mRedoWriter instanceof ReplController)) {
                if (mRedoWriter.shouldCheckpoint(1)) {
                    // Clean up the replication log.
                    full = true;
                }
            }

            if (!full) {
                // No need for full checkpoint, but fully sync the redo log for durability.
                flush(2); // flush and sync metadata
                return;
            }
        }

        mLastCheckpointNanos = nowNanos;

        if (mEventListener != null) {
            // Note: Events should not be delivered when exclusive commit lock is held.
            // The listener implementation might introduce extra blocking.
            mEventListener.notify(EventType.CHECKPOINT_BEGIN, "Checkpoint begin");
        }

        boolean resume = true;
        UndoLog masterUndoLog = mCommitMasterUndoLog;

        if (header == p_null()) {
            // Not resumed. Allocate new header early, before acquiring locks.
            header = p_callocPage(mPageDb.directPageSize());
            resume = false;
            if (masterUndoLog != null) {
                // TODO: Thrown when closed? After storage device was full.
                throw new AssertionError();
            }
        }

        final RedoWriter redo = mRedoWriter;

        try {
            int hoff = mPageDb.extraCommitDataOffset();
            p_intPutLE(header, hoff + I_ENCODING_VERSION, ENCODING_VERSION);

            if (redo != null) {
                // File-based redo log should create a new file, but not write to it yet.
                redo.checkpointPrepare();
            }

            while (true) {
                mCommitLock.acquireExclusive();

                // Registry root is infrequently modified, and so shared latch is usually
                // available. If not, cause might be a deadlock. To be safe, always release
                // commit lock and start over.
                if (root.tryAcquireShared()) {
                    break;
                }

                mCommitLock.releaseExclusive();
            }

            mCheckpointFlushState = CHECKPOINT_FLUSH_PREPARE;

            if (!resume) {
                p_longPutLE(header, hoff + I_ROOT_PAGE_ID, root.id());
            }

            final long redoNum, redoPos, redoTxnId;
            if (redo == null) {
                redoNum = 0;
                redoPos = 0;
                redoTxnId = 0;
            } else {
                // Switch and capture state while commit lock is held.
                redo.checkpointSwitch(mTxnContexts);
                redoNum = redo.checkpointNumber();
                redoPos = redo.checkpointPosition();
                redoTxnId = redo.checkpointTransactionId();
            }

            p_longPutLE(header, hoff + I_CHECKPOINT_NUMBER, redoNum);
            p_longPutLE(header, hoff + I_REDO_TXN_ID, redoTxnId);
            p_longPutLE(header, hoff + I_REDO_POSITION, redoPos);
            p_longPutLE(header, hoff + I_REPL_ENCODING, redo == null ? 0 : redo.encoding());

            // TODO: I don't like all this activity with exclusive commit lock held. UndoLog
            // can be refactored to store into a special Tree, but this requires more features
            // to be added to Tree first. Specifically, large values and appending to them.

            if (!resume) {
                long txnId = 0;
                byte[] workspace = null;

                for (TransactionContext txnContext : mTxnContexts) {
                    txnId = txnContext.higherTransactionId(txnId);

                    synchronized (txnContext) {
                        if (txnContext.hasUndoLogs()) {
                            if (masterUndoLog == null) {
                                masterUndoLog = new UndoLog(this, 0);
                                // Stash it to resume after an aborted checkpoint. Assign early
                                // to ensure that the close method can see it and delete it,
                                // even if an exception is thrown. Note that this assignment,
                                // undo log deletion, node allocation, and any accesses into
                                // the undo log acquire the commit lock. This ensures that
                                // deletion is safe from race conditions. Once deleted, the
                                // undo log object can still be used safely, except that it's
                                // empty, and new node allocations fail with an exception.
                                mCommitMasterUndoLog = masterUndoLog;
                            }
                            workspace = txnContext.writeToMaster(masterUndoLog, workspace);
                        }
                    }
                }

                final long masterUndoLogId;
                if (masterUndoLog == null) {
                    masterUndoLogId = 0;
                } else {
                    masterUndoLogId = masterUndoLog.persistReady();
                    if (masterUndoLogId == 0) {
                        // Nothing was actually written to the log.
                        mCommitMasterUndoLog = null;
                        masterUndoLog = null;
                    }
                }

                p_longPutLE(header, hoff + I_TRANSACTION_ID, txnId);
                p_longPutLE(header, hoff + I_MASTER_UNDO_LOG_PAGE_ID, masterUndoLogId);
            }

            mCommitHeader = header;

            mPageDb.commit(resume, header, this::checkpointFlush);
        } catch (Throwable e) {
            if (mCommitHeader != header) {
                p_delete(header);
            }

            if (mCheckpointFlushState == CHECKPOINT_FLUSH_PREPARE) {
                // Exception was thrown with locks still held, which means that the commit
                // state didn't change. The header might not be filled in completely, so don't
                // attempt to resume the checkpoint later. Fully delete the header and truncate
                // the master undo log.

                mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;
                root.releaseShared();
                mCommitLock.releaseExclusive();

                if (redo != null) {
                    redo.checkpointAborted();
                }

                deleteCommitHeader();
                mCommitMasterUndoLog = null;

                if (masterUndoLog != null) {
                    try {
                        masterUndoLog.truncate();
                    } catch (Throwable e2) {
                        // Panic.
                        suppress(e2, e);
                        close(e2);
                        throw e2;
                    }
                }
            }

            throw e;
        }

        // Reset for next checkpoint.
        deleteCommitHeader();
        mCommitMasterUndoLog = null;

        if (masterUndoLog != null) {
            // Delete the master undo log, which won't take effect until the next checkpoint.
            CommitLock.Shared shared = mCommitLock.acquireShared();
            try {
                if (!isClosed()) {
                    masterUndoLog.doTruncate(mCommitLock, shared);
                }
            } finally {
                shared.release();
            }
        }

        // Note: This step is intended to discard old redo data, but it can get skipped if
        // process exits at this point. Data is discarded again when database is re-opened.
        if (mRedoWriter != null) {
            mRedoWriter.checkpointFinished();
        }

        if (mEventListener != null) {
            double duration = (System.nanoTime() - mLastCheckpointNanos) / 1_000_000_000.0;
            mEventListener.notify(EventType.CHECKPOINT_COMPLETE,
                                  "Checkpoint completed in %1$1.3f seconds",
                                  duration, TimeUnit.SECONDS);
        }
    }

    /**
     * Method is invoked with exclusive commit lock and shared root node latch held. Both are
     * released by this method.
     */
    private void checkpointFlush(boolean resume, /*P*/ byte[] header) throws IOException {
        int stateToFlush = mCommitState;

        if (resume) {
            // Resume after an aborted checkpoint.
            if (header != mCommitHeader) {
                throw new AssertionError();
            }
            stateToFlush ^= 1;
        } else {
            if (mInitialReadState != CACHED_CLEAN) {
                mInitialReadState = CACHED_CLEAN; // Must be set before switching commit state.
            }
            mCommitState = (byte) (stateToFlush ^ 1);
            mCommitHeader = header;
        }

        mCheckpointFlushState = stateToFlush;

        mRegistry.mRoot.releaseShared();
        mCommitLock.releaseExclusive();

        if (mRedoWriter != null) {
            mRedoWriter.checkpointStarted();
        }

        if (mEventListener != null) {
            mEventListener.notify(EventType.CHECKPOINT_FLUSH, "Flushing all dirty nodes");
        }

        try {
            mCheckpointer.flushDirty(mNodeGroups, stateToFlush);

            if (mRedoWriter != null) {
                mRedoWriter.checkpointFlushed();
            }
        } finally {
            mCheckpointFlushState = CHECKPOINT_NOT_FLUSHING;
        }

        if (mEventListener != null) {
            mEventListener.notify(EventType.CHECKPOINT_SYNC, "Forcibly persisting all changes");
        }
    }

    // Called by DurablePageDb with header latch held.
    static long readRedoPosition(/*P*/ byte[] header, int offset) {
        return p_longGetLE(header, offset + I_REDO_POSITION);
    }
}
