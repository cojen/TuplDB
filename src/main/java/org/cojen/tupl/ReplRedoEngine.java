/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.lang.ref.SoftReference;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.util.concurrent.atomic.AtomicInteger;

import org.cojen.tupl.ext.ReplicationManager;

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ReplRedoEngine implements RedoVisitor {
    private final static long INFINITE_TIMEOUT = -1L;

    final ReplicationManager mManager;
    final Database mDatabase;

    final ReplRedoController mController;

    // Maintain soft references to indexes, allowing them to get closed if not
    // used for awhile. Without the soft references, Database maintains only
    // weak references to indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private final Latch[] mLatches;
    private final int mLatchesMask;

    private final TxnTable mTransactions;

    private final int mMaxThreads;
    private final AtomicInteger mTotalThreads;
    private final AtomicInteger mIdleThreads;
    private final ConcurrentMap<DecodeTask, Object> mTaskThreadSet;

    // Latch must be held exclusively while reading from decoder.
    private final Latch mDecodeLatch;

    private ReplRedoDecoder mDecoder;

    // Shared latch held when applying operations. Checkpoint suspends all tasks by acquiring
    // an exclusive latch. If any operation fails to be applied, shared latch is still held,
    // preventing checkpoints.
    final Latch mOpLatch;

    // Updated with exclusive decode latch and shared op latch. Values can be read with op
    // latch exclusively held, when engine is suspended.
    long mDecodePosition;
    long mDecodeTransactionId;

    /**
     * @param manager already started
     * @param txns recovered transactions; can be null; cleared as a side-effect
     */
    ReplRedoEngine(ReplicationManager manager, int maxThreads,
                   Database db, LHashTable.Obj<Transaction> txns)
        throws IOException
    {
        if (maxThreads <= 0) {
            int procs = Runtime.getRuntime().availableProcessors();
            maxThreads = maxThreads == 0 ? procs : (-maxThreads * procs);
            if (maxThreads <= 0) {
                maxThreads = Integer.MAX_VALUE;
            }
        }

        mManager = manager;
        mDatabase = db;

        mController = new ReplRedoController(this);

        mIndexes = new LHashTable.Obj<>(16);

        mDecodeLatch = new Latch();
        mOpLatch = new Latch();

        mMaxThreads = maxThreads;
        mTotalThreads = new AtomicInteger();
        mIdleThreads = new AtomicInteger();
        mTaskThreadSet = new ConcurrentHashMap<>(16, 0.75f, 1);

        int latchCount = roundUpPower2(maxThreads * 2);
        if (latchCount <= 0) {
            latchCount = 1 << 30;
        }

        mLatches = new Latch[latchCount];
        mLatchesMask = mLatches.length - 1;
        for (int i=0; i<mLatches.length; i++) {
            mLatches[i] = new Latch();
        }

        final TxnTable txnTable;
        if (txns == null) {
            txnTable = new TxnTable(16);
        } else {
            txnTable = new TxnTable(txns.size());

            txns.traverse(new LHashTable.Visitor
                          <LHashTable.ObjEntry<Transaction>, IOException>()
            {
                public boolean visit(LHashTable.ObjEntry<Transaction> entry) throws IOException {
                    // Reduce hash collisions.
                    long scrambledTxnId = scramble(entry.key);
                    Latch latch = selectLatch(scrambledTxnId);
                    Transaction txn = entry.value;
                    if (!txn.recoveryCleanup(false)) {
                        txnTable.insert(scrambledTxnId).init(txn, latch);
                    }
                    // Delete entry.
                    return true;
                }
            });
        }

        mTransactions = txnTable;

        // Initialize the decode position early.
        mDecodeLatch.acquireExclusive();
        mDecodePosition = manager.readPosition();
        mDecodeLatch.releaseExclusive();
    }

    public RedoWriter initWriter(long redoNum) {
        mController.initCheckpointNumber(redoNum);
        return mController;
    }

    public void startReceiving(long initialPosition, long initialTxnId) {
        mDecodeLatch.acquireExclusive();
        if (mDecoder == null) {
            mOpLatch.acquireExclusive();
            try {
                try {
                    mDecoder = new ReplRedoDecoder(mManager, initialPosition, initialTxnId);
                } catch (Throwable e) {
                    mDecodeLatch.releaseExclusive();
                    throw e;
                }
                mDecodeTransactionId = initialTxnId;
                nextTask();
            } finally {
                mOpLatch.releaseExclusive();
            }
        } else {
            mDecodeLatch.releaseExclusive();
        }
    }

    @Override
    public boolean reset() throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        // Reset and discard all transactions.
        mTransactions.traverse(new LHashTable.Visitor<TxnEntry, IOException>() {
            public boolean visit(TxnEntry entry) throws IOException {
                Latch latch = entry.latch();
                try {
                    entry.mTxn.recoveryCleanup(true);
                } finally {
                    latch.releaseExclusive();
                }
                return true;
            }
        });

        // Now's a good time to clean out any lingering trash.
        mDatabase.emptyAllFragmentedTrash(false);

        // Only release if no exception.
        opFinished();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean timestamp(long timestamp) {
        return nop();
    }

    @Override
    public boolean shutdown(long timestamp) {
        return nop();
    }

    @Override
    public boolean close(long timestamp) {
        return nop();
    }

    @Override
    public boolean endFile(long timestamp) {
        return nop();
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = getIndex(indexId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        // Locks must be acquired in their original order to avoid
        // deadlock, so don't allow another task thread to run yet.
        Locker locker = mDatabase.mLockManager.localLocker();
        locker.lockExclusive(indexId, key, INFINITE_TIMEOUT);

        // Allow another task thread to run while operation completes.
        nextTask();

        try {
            while (ix != null) {
                try {
                    ix.store(Transaction.BOGUS, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } finally {
            locker.scopeUnlockAll();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        // A no-lock change is created when using the UNSAFE lock mode. If the
        // application has performed its own locking, consistency can be
        // preserved by locking the index entry. Otherwise, the outcome is
        // unpredictable.

        return store(indexId, key, value);
    }

    @Override
    public boolean dropIndex(long txnId, long indexId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        Index ix;
        {
            LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.remove(indexId);
            if (entry == null || (ix = entry.value.get()) == null) {
                ix = mDatabase.anyIndexById(indexId);
            }
        }

        try {
            while (ix != null) {
                try {
                    // Sweep through index, waiting for any concurrent deletes to
                    // finish. Need to use a transaction for upgradable locks. No changes
                    // are being made, and so this doesn't create a transaction id.
                    Transaction txn = mDatabase.newTransaction();
                    try {
                        Cursor c = ix.newCursor(txn);
                        try {
                            c.autoload(false);
                            c.first();
                        } finally {
                            c.reset();
                        }
                    } finally {
                        txn.reset();
                    }

                    if (ix instanceof Tree) {
                        ((Tree) ix).drop(txnId);
                    } else {
                        ix.drop();
                    }

                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } catch (IllegalStateException e) {
            EventListener listener = mDatabase.mEventListener;
            if (listener != null) {
                listener.notify(EventType.REPLICATION_WARNING,
                                "Unable to drop index: %1$s", rootCause(e));
                // Disable notification.
                ix = null;
            }
        }

        // Only release if no exception.
        opFinished();

        if (ix != null) {
            try {
                mManager.notifyDrop(ix);
            } catch (Throwable e) {
                uncaught(e);
            }
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException {
        Index ix = getIndex(indexId);
        byte[] oldName = null;

        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        if (ix != null) {
            oldName = ix.getName();
            try {
                mDatabase.renameIndex(ix, newName, txnId);
            } catch (RuntimeException e) {
                EventListener listener = mDatabase.mEventListener;
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_WARNING,
                                    "Unable to rename index: %1$s", rootCause(e));
                    // Disable notification.
                    ix = null;
                }
            }
        }

        // Only release if no exception.
        opFinished();

        if (ix != null) {
            try {
                mManager.notifyRename(ix, oldName, newName.clone());
            } catch (Throwable e) {
                uncaught(e);
            }
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        Index ix;
        {
            LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.remove(indexId);
            if (entry == null || (ix = entry.value.get()) == null) {
                ix = mDatabase.anyIndexById(indexId);
            }
        }

        Runnable task = null;

        try {
            while (ix != null) {
                try {
                    task = mDatabase.deleteIndex(ix, txnId);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } catch (RuntimeException e) {
            EventListener listener = mDatabase.mEventListener;
            if (listener != null) {
                listener.notify(EventType.REPLICATION_WARNING,
                                "Unable to delete index: %1$s", rootCause(e));
                // Disable notification.
                ix = null;
            }
        }

        // Only release if no exception.
        opFinished();

        if (ix != null && task != null) {
            try {
                mManager.notifyDrop(ix);
            } catch (Throwable e) {
                uncaught(e);
            }

            try {
                // Allow index deletion to run concurrently. If multiple deletes are received
                // concurrently, then the application is likely doing concurrent deletes.
                Thread deletion = new Thread(task, "IndexDeletion-" + ix.getNameString());
                deletion.setDaemon(true);
                deletion.start();
            } catch (Throwable e) {
                EventListener listener = mDatabase.mEventListener;
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_WARNING,
                                    "Unable to immediately delete index: %1$s", rootCause(e));
                }
                // Index will get fully deleted when database is re-opened.
            }
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        // Reduce hash collisions.
        long scrambledTxnId = scramble(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        if (e == null) {
            Transaction txn = new Transaction
                (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);
            mTransactions.insert(scrambledTxnId).init(txn, selectLatch(scrambledTxnId));

            // Only release if no exception.
            opFinished();

            return true;
        }

        Latch latch = e.latch();
        try {
            // Cheap operation, so don't let another task thread run.
            e.mTxn.enter();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinished();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            te.mTxn.exit();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        TxnEntry te = removeTxnEntry(txnId);

        if (te == null) {
            opFinished();
            return true;
        }

        Latch latch = te.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            te.mTxn.reset();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            // Commit is expected to complete quickly, so don't let another
            // task thread run.

            Transaction txn = te.mTxn;
            try {
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinished();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        // Acquire latch before performing operations with side-effects.
        mOpLatch.acquireShared();

        TxnEntry te = removeTxnEntry(txnId);

        if (te == null) {
            // TODO: Throw a better exception.
            throw new CorruptDatabaseException("Transaction not found: " + txnId);
        }

        Latch latch = te.latch();
        try {
            // Commit is expected to complete quickly, so don't let another
            // task thread run.

            te.mTxn.commitAll();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        opFinished();

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            Transaction txn = te.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);

            // Allow another task thread to run while operation completes.
            nextTask();

            while (ix != null) {
                try {
                    ix.store(txn, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry te = getTxnEntry(txnId);

        // Allow side-effect free operations to be performed before acquiring latch.
        mOpLatch.acquireShared();

        Latch latch = te.latch();
        try {
            Transaction txn = te.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, INFINITE_TIMEOUT);

            // Allow another task thread to run while operation completes.
            nextTask();

            while (ix != null) {
                try {
                    ix.store(txn, key, value);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    ix = openIndex(indexId, null);
                }
            }

            txn.commitAll();
        } finally {
            latch.releaseExclusive();
        }

        // Only release if no exception.
        mOpLatch.releaseShared();

        notifyStore(ix, key, value);

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    /**
     * Called for an operation which is ignored.
     */
    private boolean nop() {
        mOpLatch.acquireShared();
        opFinished();
        return true;
    }

    /**
     * Called after an operation is finished which didn't spawn a task thread. Caller must hold
     * shared op latch, which is released by this method. Decode latch must also be held, which
     * caller must release.
     */
    private void opFinished() {
        // Capture the position for the next operation. Also capture the last transaction id,
        // before a delta is applied.
        ReplRedoDecoder decoder = mDecoder;
        mDecodePosition = decoder.in().mPos;
        mDecodeTransactionId = decoder.mTxnId;

        mOpLatch.releaseShared();
    }

    /**
     * Launch a task thread to continue processing more redo entries
     * concurrently. Caller must return false from the visitor method, to
     * prevent multiple threads from trying to decode the redo input stream. If
     * thread limit is reached, the remaining task threads continue working.
     *
     * Caller must hold exclusive decode latch, which is released by this method. Shared op
     * latch must also be held, which caller must release.
     */
    private void nextTask() {
        // Capture the position for the next operation. Also capture the last transaction id,
        // before a delta is applied.
        ReplRedoDecoder decoder = mDecoder;
        mDecodePosition = decoder.in().mPos;
        mDecodeTransactionId = decoder.mTxnId;

        if (mIdleThreads.get() == 0) {
            int total = mTotalThreads.get();
            if (total < mMaxThreads && mTotalThreads.compareAndSet(total, total + 1)) {
                DecodeTask task;
                try {
                    task = new DecodeTask();
                    task.start();
                } catch (Throwable e) {
                    mDecodeLatch.releaseExclusive();
                    mTotalThreads.decrementAndGet();
                    throw e;
                }
                mTaskThreadSet.put(task, this);
            }
        }

        // Allow task thread to proceed.
        mDecodeLatch.releaseExclusive();
    }

    /**
     * Waits for all incoming replication operations to finish and prevents new ones from
     * starting.
     */
    void suspend() {
        mOpLatch.acquireExclusive();
    }

    void resume() {
        mOpLatch.releaseExclusive();
    }

    /**
     * @return TxnEntry with scrambled transaction id
     */
    private TxnEntry getTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = scramble(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);

        if (e == null) {
            // Create transaction on demand if necessary. Startup transaction recovery only
            // applies to those which generated undo log entries.
            Transaction txn = new Transaction
                (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);
            e = mTransactions.insert(scrambledTxnId);
            e.init(txn, selectLatch(scrambledTxnId));
        }

        return e;
    }

    /**
     * @return TxnEntry with scrambled transaction id; null if not found
     */
    private TxnEntry removeTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = scramble(txnId);
        return mTransactions.remove(scrambledTxnId);
    }

    /**
     * Returns the index from the local cache, opening it if necessary.
     *
     * @return null if not found
     */
    private Index getIndex(long indexId) throws IOException {
        LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.get(indexId);
        if (entry != null) {
            Index ix = entry.value.get();
            if (ix != null) {
                return ix;
            }
        }
        return openIndex(indexId, entry);
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(long indexId, LHashTable.ObjEntry<SoftReference<Index>> entry)
        throws IOException
    {
        Index ix = mDatabase.anyIndexById(indexId);
        if (ix == null) {
            return null;
        }

        SoftReference<Index> ref = new SoftReference<>(ix);
        if (entry == null) {
            mIndexes.insert(indexId).value = ref;
        } else {
            entry.value = ref;
        }

        if (entry != null) {
            // Remove entries for all other cleared references, freeing up memory.
            mIndexes.traverse(new LHashTable.Visitor<
                              LHashTable.ObjEntry<SoftReference<Index>>, RuntimeException>()
            {
                public boolean visit(LHashTable.ObjEntry<SoftReference<Index>> entry) {
                    return entry.value.get() == null;
                }
            });
        }

        return ix;
    }

    private Latch selectLatch(long scrambledTxnId) {
        return mLatches[((int) scrambledTxnId) & mLatchesMask];
    }

    private static final long IDLE_TIMEOUT_NANOS = 5 * 1000000000L;

    /**
     * @return false if thread should exit
     */
    boolean decode() {
        mIdleThreads.incrementAndGet();
        try {
            while (true) {
                try {
                    if (mDecodeLatch.tryAcquireExclusiveNanos(IDLE_TIMEOUT_NANOS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    // Treat as timeout.
                    Thread.interrupted();
                }

                int total = mTotalThreads.get();
                if (total > 1 && mTotalThreads.compareAndSet(total, total - 1)) {
                    return false;
                }
            }
        } finally {
            mIdleThreads.decrementAndGet();
        }

        // At this point, decode latch is held exclusively.

        RedoDecoder decoder = mDecoder;
        if (decoder == null) {
            mTotalThreads.decrementAndGet();
            mDecodeLatch.releaseExclusive();
            return false;
        }

        try {
            if (!decoder.run(this)) {
                return true;
            }
            // End of stream reached, and so local instance is now leader.
            reset();
        } catch (Throwable e) {
            EventListener listener = mDatabase.mEventListener;
            if (listener != null) {
                listener.notify(EventType.REPLICATION_PANIC,
                                "Unexpected replication exception: %1$s", rootCause(e));
            } else {
                uncaught(e);
            }
            mTotalThreads.decrementAndGet();
            mDecodeLatch.releaseExclusive();
            // Panic.
            closeQuietly(null, mDatabase, e);
            return false;
        }

        mDecoder = null;
        mTotalThreads.decrementAndGet();
        mDecodeLatch.releaseExclusive();

        try {
            mController.leaderNotify();
        } catch (UnmodifiableReplicaException e) {
            // Should already be receiving again due to this exception.
        } catch (Throwable e) {
            // Could try to switch to receiving mode, but panic seems to be the safe option.
            closeQuietly(null, mDatabase, e);
        }

        return false;
    }

    private void notifyStore(Index ix, byte[] key, byte[] value) {
        if (ix != null && !Tree.isInternal(ix.getId())) {
            try {
                mManager.notifyStore(ix, key, value);
            } catch (Throwable e) {
                uncaught(e);
            }
        }
    }

    private static int cTaskNumber;

    static synchronized long taskNumber() {
        return (++cTaskNumber) & 0xffffffffL;
    }

    final class DecodeTask extends Thread {
        DecodeTask() {
            super("ReplicationReceiver-" + taskNumber());
            setDaemon(true);
        }

        public void run() {
            while (ReplRedoEngine.this.decode());
            mTaskThreadSet.remove(this);
        }
    }

    static final class TxnEntry extends LHashTable.Entry<TxnEntry> {
        Transaction mTxn;
        Latch mLatch;

        void init(Transaction txn, Latch latch) {
            mTxn = txn;
            mLatch = latch;
        }

        Latch latch() {
            Latch latch = mLatch;
            latch.acquireExclusive();
            return latch;
        }
    }

    static final class TxnTable extends LHashTable<TxnEntry> {
        TxnTable(int capacity) {
            super(capacity);
        }

        protected TxnEntry newEntry() {
            return new TxnEntry();
        }
    }
}
