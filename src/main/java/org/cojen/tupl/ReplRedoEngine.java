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

import java.io.IOException;

import java.lang.ref.SoftReference;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.ext.ReplicationManager;
import org.cojen.tupl.ext.TransactionHandler;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Worker;
import org.cojen.tupl.util.WorkerGroup;

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
/*P*/
class ReplRedoEngine implements RedoVisitor, ThreadFactory {
    private static final int MAX_QUEUE_SIZE = 100;
    private static final int MAX_KEEP_ALIVE_MILLIS = 60_000;
    private static final long INFINITE_TIMEOUT = -1L;
    private static final String ATTACHMENT = "replication";

    // Hash spreader. Based on rounded value of 2 ** 63 * (sqrt(5) - 1) equivalent 
    // to unsigned 11400714819323198485.
    private static final long HASH_SPREAD = -7046029254386353131L;

    final ReplicationManager mManager;
    final LocalDatabase mDatabase;

    final ReplRedoController mController;

    private final WorkerGroup mWorkerGroup;

    private final Latch mDecodeLatch;

    private final TxnTable mTransactions;

    // Maintain soft references to indexes, allowing them to get closed if not
    // used for awhile. Without the soft references, Database maintains only
    // weak references to indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private ReplRedoDecoder mDecoder;

    /**
     * @param manager already started
     * @param txns recovered transactions; can be null; cleared as a side-effect
     */
    ReplRedoEngine(ReplicationManager manager, int maxThreads,
                   LocalDatabase db, LHashTable.Obj<LocalTransaction> txns)
        throws IOException
    {
        if (maxThreads <= 0) {
            int procCount = Runtime.getRuntime().availableProcessors();
            maxThreads = maxThreads == 0 ? procCount : (-maxThreads * procCount);
            if (maxThreads <= 0) {
                // Overflowed.
                maxThreads = Integer.MAX_VALUE;
            }
        }

        mManager = manager;
        mDatabase = db;

        mController = new ReplRedoController(this);

        mDecodeLatch = new Latch();

        if (maxThreads <= 1) {
            // Just use the decoder thread and don't hand off tasks to worker threads.
            mWorkerGroup = null;
        } else {
            mWorkerGroup = WorkerGroup.make(maxThreads - 1, // one thread will be the decoder
                                            MAX_QUEUE_SIZE,
                                            MAX_KEEP_ALIVE_MILLIS, TimeUnit.MILLISECONDS,
                                            this); // ThreadFactory
        }

        final TxnTable txnTable;
        if (txns == null) {
            txnTable = new TxnTable(16);
        } else {
            txnTable = new TxnTable(txns.size());

            txns.traverse(te -> {
                long scrambledTxnId = mix(te.key);
                LocalTransaction txn = te.value;
                if (!txn.recoveryCleanup(false)) {
                    txnTable.insert(scrambledTxnId).init(txn);
                }
                // Delete entry.
                return true;
            });
        }

        mTransactions = txnTable;

        mIndexes = new LHashTable.Obj<>(16);
    }

    public RedoWriter initWriter(long redoNum) {
        mController.initCheckpointNumber(redoNum);
        return mController;
    }

    public void startReceiving(long initialPosition, long initialTxnId) {
        try {
            mDecodeLatch.acquireExclusive();
            try {
                if (mDecoder == null || mDecoder.mDeactivated) {
                    mDecoder = new ReplRedoDecoder
                        (mManager, initialPosition, initialTxnId, mDecodeLatch);
                    newThread(this::decode).start();
                }
            } finally {
                mDecodeLatch.releaseExclusive();
            }
        } catch (Throwable e) {
            fail(e);
        }
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("ReplicationReceiver-" + Long.toUnsignedString(t.getId()));
        return t;
    }

    @Override
    public boolean reset() throws IOException {
        // Reset and discard all transactions.
        mTransactions.traverse(te -> {
            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        te.mTxn.recoveryCleanup(true);
                    } catch (Throwable e) {
                        fail(e);
                    }
                }
            });
            return true;
        });

        // Wait for work to complete.
        if (mWorkerGroup != null) {
            mWorkerGroup.join(false);
        }

        // Although it might seem like a good time to clean out any lingering trash, concurrent
        // transactions are still active and need the trash to rollback properly. Waiting for
        // the worker group to finish isn't sufficient. Not all transactions are replicated.
        //mDatabase.emptyAllFragmentedTrash(false);

        return true;
    }

    @Override
    public boolean timestamp(long timestamp) throws IOException {
        return true;
    }

    @Override
    public boolean shutdown(long timestamp) throws IOException {
        return true;
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        return true;
    }

    @Override
    public boolean endFile(long timestamp) throws IOException {
        return true;
    }

    @Override
    public boolean fence() throws IOException {
        // Wait for work to complete.
        if (mWorkerGroup != null) {
            mWorkerGroup.join(false);
        }

        // Call with decode latch held, suspending checkpoints.
        mManager.fenced(mDecoder.mIn.mPos);

        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        // Must acquire the lock before task is enqueued.
        Locker locker = new Locker(mDatabase.mLockManager);
        locker.attach(ATTACHMENT);
        locker.tryLockUpgradable(indexId, key, INFINITE_TIMEOUT);

        runTaskAnywhere(new Worker.Task() {
            public void run() {
                Index ix;
                try {
                    ix = getIndex(indexId);

                    // Full exclusive lock is required.
                    locker.lockExclusive(indexId, key, INFINITE_TIMEOUT);

                    while (true) {
                        try {
                            ix = getIndex(indexId);
                            ix.store(Transaction.BOGUS, key, value);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            ix = openIndex(indexId, null);
                        }
                    }
                } catch (Throwable e) {
                    fail(e);
                    return;
                } finally {
                    locker.scopeUnlockAll();
                }

                notifyStore(ix, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        // A no-lock change is created when using the UNSAFE lock mode. If the application has
        // performed its own locking, consistency can be preserved by locking the index
        // entry. Otherwise, the outcome is unpredictable.

        return store(indexId, key, value);
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException {
        Index ix = getIndex(indexId);

        if (ix == null) {
            // No notification.
            return true;
        }

        byte[] oldName = ix.getName();

        try {
            mDatabase.renameIndex(ix, newName, txnId);
        } catch (RuntimeException e) {
            EventListener listener = mDatabase.eventListener();
            if (listener != null) {
                listener.notify(EventType.REPLICATION_WARNING,
                                "Unable to rename index: %1$s", rootCause(e));
                // No notification.
                return true;
            }
        }

        runTaskAnywhere(new Worker.Task() {
            public void run() {
                try {
                    mManager.notifyRename(ix, oldName, newName.clone());
                } catch (Throwable e) {
                    uncaught(e);
                }
            }
        });

        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    LocalTransaction txn = te.mTxn;

                    // Open the index with the transaction to prevent deadlock
                    // when the instance is not cached and has to be loaded.
                    Index ix = getIndex(txn, indexId);
                    mIndexes.remove(indexId);

                    try {
                        txn.commit();
                    } finally {
                        txn.exit();
                    }

                    if (ix != null) {
                        ix.close();
                        try {
                            mManager.notifyDrop(ix);
                        } catch (Throwable e) {
                            uncaught(e);
                        }
                    }

                    Runnable task = mDatabase.replicaDeleteTree(indexId);

                    if (task != null) {
                        try {
                            // Allow index deletion to run concurrently. If multiple deletes
                            // are received concurrently, then the application is likely doing
                            // concurrent deletes.
                            Thread deletion = new Thread
                                (task, "IndexDeletion-" +
                                 (ix == null ? indexId : ix.getNameString()));
                            deletion.setDaemon(true);
                            deletion.start();
                        } catch (Throwable e) {
                            EventListener listener = mDatabase.eventListener();
                            if (listener != null) {
                                listener.notify(EventType.REPLICATION_WARNING,
                                                "Unable to immediately delete index: %1$s",
                                                rootCause(e));
                            }
                            // Index will get fully deleted when database is re-opened.
                        }
                    }
                } catch (Throwable e) {
                    fail(e);
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        long scrambledTxnId = mix(txnId);
        TxnEntry te = mTransactions.get(scrambledTxnId);

        if (te == null) {
            // Create a new transaction.
            mTransactions.insert(scrambledTxnId).init(newTransaction(txnId));
        } else {
            // Enter nested scope of an existing transaction.

            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        te.mTxn.enter();
                    } catch (Throwable e) {
                        fail(e);
                    }
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    te.mTxn.exit();
                } catch (Throwable e) {
                    fail(e);
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        TxnEntry te = removeTxnEntry(txnId);

        if (te != null) {
            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        te.mTxn.reset();
                    } catch (Throwable e) {
                        fail(e);
                    }
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    te.mTxn.commit();
                } catch (Throwable e) {
                    fail(e);
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        TxnEntry te = removeTxnEntry(txnId);

        if (te != null) {
            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        te.mTxn.commitAll();
                    } catch (Throwable e) {
                        fail(e);
                    }
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnEnterStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        long scrambledTxnId = mix(txnId);
        TxnEntry te = mTransactions.get(scrambledTxnId);

        LocalTransaction txn;
        boolean newTxn;
        if (te == null) {
            // Create a new transaction.
            txn = newTransaction(txnId);
            te = mTransactions.insert(scrambledTxnId);
            te.init(txn);
            newTxn = true;
        } else {
            // Enter nested scope of an existing transaction.
            txn = te.mTxn;
            newTxn = false;
        }

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() {
                Index ix;
                try {
                    if (!newTxn) {
                        txn.enter();
                    }

                    if (lock != null) {
                        txn.push(lock);
                    }

                    ix = getIndex(indexId);

                    while (true) {
                        try {
                            ix.store(txn, key, value);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            ix = openIndex(indexId, null);
                        }
                    }
                } catch (Throwable e) {
                    fail(e);
                    return;
                }

                notifyStore(ix, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() {
                Index ix;
                try {
                    if (lock != null) {
                        txn.push(lock);
                    }

                    ix = getIndex(indexId);

                    while (true) {
                        try {
                            ix.store(txn, key, value);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            ix = openIndex(indexId, null);
                        }
                    }
                } catch (Throwable e) {
                    fail(e);
                    return;
                }

                notifyStore(ix, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() {
                Index ix;
                try {
                    if (lock != null) {
                        txn.push(lock);
                    }

                    ix = getIndex(indexId);

                    while (true) {
                        try {
                            ix.store(txn, key, value);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            ix = openIndex(indexId, null);
                        }
                    }

                    txn.commit();
                } catch (Throwable e) {
                    fail(e);
                    return;
                }

                notifyStore(ix, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        TxnEntry te = removeTxnEntry(txnId);

        LocalTransaction txn;
        if (te == null) {
            // Create the transaction, but don't store it in the transaction table.
            txn = newTransaction(txnId);
        } else {
            txn = te.mTxn;
        }

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        Worker.Task task = new Worker.Task() {
            public void run() {
                Index ix;
                try {
                    if (lock != null) {
                        txn.push(lock);
                    }

                    ix = getIndex(indexId);

                    while (true) {
                        try {
                            ix.store(txn, key, value);
                            break;
                        } catch (ClosedIndexException e) {
                            // User closed the shared index reference, so re-open it.
                            ix = openIndex(indexId, null);
                        }
                    }

                    txn.commitAll();
                } catch (Throwable e) {
                    fail(e);
                    return;
                }

                notifyStore(ix, key, value);
            }
        };

        if (te == null) {
            runTaskAnywhere(task);
        } else {
            runTask(te, task);
        }

        return true;
    }

    @Override
    public boolean txnLockShared(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockSharedNoPush(indexId, key);

        // TODO: No need to run special task if transaction was just created.
        if (lock != null) {
            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        txn.push(lock);
                    } catch (Throwable e) {
                        fail(e);
                        return;
                    }
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        // TODO: No need to run special task if transaction was just created.
        if (lock != null) {
            runTask(te, new Worker.Task() {
                public void run() {
                    try {
                        txn.push(lock);
                    } catch (Throwable e) {
                        fail(e);
                        return;
                    }
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) throws IOException {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        // TODO: Can acquire exclusive at first, but must know what push mode to use (0 or 1)
        // TODO: No need to run special task if transaction was just created.
        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    if (lock != null) {
                        txn.push(lock);
                    }

                    txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);
                } catch (Throwable e) {
                    fail(e);
                    return;
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnCustom(long txnId, byte[] message) throws IOException {
        TransactionHandler handler = customHandler();
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    handler.redo(mDatabase, txn, message);
                } catch (Throwable e) {
                    fail(e);
                    return;
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        TransactionHandler handler = customHandler();
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() {
                try {
                    if (lock != null) {
                        txn.push(lock);
                    }

                    txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);

                    handler.redo(mDatabase, txn, message, indexId, key);
                } catch (Throwable e) {
                    fail(e);
                    return;
                }
            }
        });

        return true;
    }

    /**
     * Returns the position of the next operation to decode. To avoid deadlocks, engine must
     * not be suspended when calling this method. Instead, call suspendedDecodePosition.
     */
    long decodePosition() {
        mDecodeLatch.acquireShared();
        try {
            return getDecodePosition();
        } finally {
            mDecodeLatch.releaseShared();
        }
    }

    private long getDecodePosition() {
        ReplRedoDecoder decoder = mDecoder;
        return decoder == null ? mManager.readPosition() : decoder.mDecodePosition;
    }

    /**
     * Returns the position of the next operation to decode, while engine is suspended.
     */
    long suspendedDecodePosition() {
        return getDecodePosition();
    }

    /**
     * Returns the last transaction id which was decoded, while engine is suspended.
     *
     * @throws IllegalStateException if not decoding
     */
    long suspendedDecodeTransactionId() {
        ReplRedoDecoder decoder = mDecoder;
        if (decoder != null) {
            return decoder.mDecodeTransactionId;
        }
        throw new IllegalStateException("Not decoding");
    }

    /**
     * Prevents new operations from starting and waits for in-flight operations to complete.
     */
    void suspend() {
        // Prevent new operations from being decoded.
        mDecodeLatch.acquireShared();

        // Wait for work to complete.
        if (mWorkerGroup != null) {
            mWorkerGroup.join(false);
        }
    }

    void resume() {
        mDecodeLatch.releaseShared();
    }

    /**
     * @return TxnEntry with scrambled transaction id
     */
    private TxnEntry getTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = mix(txnId);
        TxnEntry te = mTransactions.get(scrambledTxnId);

        if (te == null) {
            // Create transaction on demand if necessary. Startup transaction recovery only
            // applies to those which generated undo log entries.
            LocalTransaction txn = newTransaction(txnId);
            te = mTransactions.insert(scrambledTxnId);
            te.init(txn);
        }

        return te;
    }

    /**
     * Only to be called from decode thread. Selects a worker for the first task against the
     * given transaction, and then uses the same worker for subsequent tasks.
     */
    private void runTask(TxnEntry te, Worker.Task task) {
        Worker w = te.mWorker;
        if (w == null) {
            te.mWorker = runTaskAnywhere(task);
        } else {
            w.enqueue(task);
        }
    }

    private Worker runTaskAnywhere(Worker.Task task) {
        if (mWorkerGroup == null) {
            try {
                task.run();
            } catch (Throwable e) {
                uncaught(e);
            }
            return null;
        } else {
            return mWorkerGroup.enqueue(task);
        }
    }

    private LocalTransaction newTransaction(long txnId) {
        LocalTransaction txn = new LocalTransaction
            (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);
        txn.attach(ATTACHMENT);
        return txn;
    }

    /**
     * @return TxnEntry with scrambled transaction id; null if not found
     */
    private TxnEntry removeTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = mix(txnId);
        return mTransactions.remove(scrambledTxnId);
    }

    /**
     * Returns the index from the local cache, opening it if necessary.
     *
     * @return null if not found
     */
    private Index getIndex(Transaction txn, long indexId) throws IOException {
        LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.get(indexId);
        if (entry != null) {
            Index ix = entry.value.get();
            if (ix != null) {
                return ix;
            }
        }
        return openIndex(txn, indexId, entry);
    }


    /**
     * Returns the index from the local cache, opening it if necessary.
     *
     * @return null if not found
     */
    private Index getIndex(long indexId) throws IOException {
        return getIndex(null, indexId);
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(Transaction txn, long indexId,
                            LHashTable.ObjEntry<SoftReference<Index>> entry)
        throws IOException
    {
        Index ix = mDatabase.anyIndexById(txn, indexId);
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
            mIndexes.traverse(e -> e.value.get() == null);
        }

        return ix;
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(long indexId, LHashTable.ObjEntry<SoftReference<Index>> entry)
        throws IOException
    {
        return openIndex(null, indexId, entry);
    }

    private void decode() {
        final ReplRedoDecoder decoder = mDecoder;

        try {
            while (!decoder.run(this));

            // End of stream reached, and so local instance is now the leader.

            // Wait for work to complete.
            if (mWorkerGroup != null) {
                mWorkerGroup.join(false);
            }

            // Rollback any lingering transactions.
            reset();
        } catch (Throwable e) {
            fail(e);
            return;
        } finally {
            decoder.mDeactivated = true;
        }

        try {
            mController.leaderNotify();
        } catch (UnmodifiableReplicaException e) {
            // Should already be receiving again due to this exception.
        } catch (Throwable e) {
            // Could try to switch to receiving mode, but panic seems to be the safe option.
            closeQuietly(null, mDatabase, e);
        }
    }

    private TransactionHandler customHandler() throws DatabaseException {
        TransactionHandler handler = mDatabase.mCustomTxnHandler;
        if (handler == null) {
            throw new DatabaseException("Custom transaction handler is not installed");
        }
        return handler;
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

    private void fail(Throwable e) {
        if (!mDatabase.isClosed()) {
            EventListener listener = mDatabase.eventListener();
            if (listener != null) {
                listener.notify(EventType.REPLICATION_PANIC,
                                "Unexpected replication exception: %1$s", rootCause(e));
            } else {
                uncaught(e);
            }
        }
        // Panic.
        closeQuietly(null, mDatabase, e);
    }

    UnmodifiableReplicaException unmodifiable() throws DatabaseException {
        mDatabase.checkClosed();
        return new UnmodifiableReplicaException();
    }

    private static long mix(long txnId) {
        return HASH_SPREAD * txnId;
    }

    static final class TxnEntry extends LHashTable.Entry<TxnEntry> {
        LocalTransaction mTxn;
        Worker mWorker;

        void init(LocalTransaction txn) {
            mTxn = txn;
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
