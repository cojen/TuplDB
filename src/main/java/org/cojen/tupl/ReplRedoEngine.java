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

import java.util.Arrays;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.ext.ReplicationManager;
import org.cojen.tupl.ext.TransactionHandler;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Worker;
import org.cojen.tupl.util.WorkerGroup;

import static org.cojen.tupl.Utils.*;

/**
 * When database is acting as a replica, this class drives all the decoding logic.
 *
 * @author Brian S O'Neill
 * @see ReplRedoController
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

    private final CursorTable mCursors;

    private ReplRedoDecoder mDecoder;

    /**
     * @param manager already started
     * @param txns recovered transactions; can be null; cleared as a side-effect
     */
    ReplRedoEngine(ReplicationManager manager, int maxThreads,
                   LocalDatabase db, LHashTable.Obj<LocalTransaction> txns,
                   LHashTable.Obj<TreeCursor> cursors)
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
                    txnTable.insert(scrambledTxnId).mTxn = txn;
                }
                // Delete entry.
                return true;
            });
        }

        mTransactions = txnTable;

        mIndexes = new LHashTable.Obj<>(16);

        final CursorTable cursorTable;
        if (cursors == null) {
            cursorTable = new CursorTable(4);
        } else {
            cursorTable = new CursorTable(cursors.size());

            cursors.traverse(ce -> {
                long scrambledCursorId = mix(ce.key);
                cursorTable.insert(scrambledCursorId).mCursor = ce.value;
                // Delete entry.
                return true;
            });
        }

        mCursors = cursorTable;
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
        t.setUncaughtExceptionHandler((thread, exception) -> fail(exception, true));
        return t;
    }

    @Override
    public boolean reset() throws IOException {
        doReset();
        return true;
    }

    /**
     * @return remaining 2PC transactions, or null if none
     */
    private LHashTable.Obj<LocalTransaction> doReset() throws IOException {
        // Reset and discard all non-2PC transactions.

        final LHashTable.Obj<LocalTransaction> remaining;

        if (mTransactions.size() == 0) {
            remaining = null;
        } else {
            remaining = new LHashTable.Obj<>(16); 

            mTransactions.traverse(te -> {
                runTask(te, new Worker.Task() {
                    public void run() throws IOException {
                        LocalTransaction txn = te.mTxn;
                        if (!txn.recoveryCleanup(true)) {
                            synchronized (remaining) {
                                remaining.insert(te.key).value = txn;
                            }
                        }
                    }
                });

                return true;
            });
        }

        // Wait for work to complete.
        if (mWorkerGroup != null) {
            mWorkerGroup.join(false);
        }

        synchronized (mCursors) {
            mCursors.traverse(entry -> {
                TreeCursor cursor = entry.mCursor;
                // Unregister first, to prevent close from writing a redo log entry.
                mDatabase.unregisterCursor(cursor);
                cursor.close();
                return true;
            });
        }

        if (remaining == null || remaining.size() == 0) {
            return null;
        }

        remaining.traverse(entry -> {
            mTransactions.insert(entry.key).mTxn = entry.value;
            return false;
        });

        return remaining;
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
    public boolean control(byte[] message) throws IOException {
        // Wait for work to complete.
        if (mWorkerGroup != null) {
            mWorkerGroup.join(false);
        }

        // Call with decode latch held, suspending checkpoints.
        mManager.control(mDecoder.mIn.mPos, message);

        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        // Must acquire the lock before task is enqueued.
        Locker locker = new Locker(mDatabase.mLockManager);
        locker.attach(ATTACHMENT);
        locker.tryLockUpgradable(indexId, key, INFINITE_TIMEOUT);

        runTaskAnywhere(new Worker.Task() {
            public void run() throws IOException {
                try {
                    Index ix = getIndex(indexId);

                    // Full exclusive lock is required.
                    locker.lockExclusive(indexId, key, INFINITE_TIMEOUT);

                    doStore(Transaction.BOGUS, indexId, key, value);
                } finally {
                    locker.scopeUnlockAll();
                }
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
                mManager.notifyRename(ix, oldName, newName.clone());
            }
        });

        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                LocalTransaction txn = te.mTxn;

                // Open the index with the transaction to prevent deadlock
                // when the instance is not cached and has to be loaded.
                Index ix = getIndex(txn, indexId);
                synchronized (mIndexes) {
                    mIndexes.remove(indexId);
                }

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
            }
        });

        return true;
    }

    @Override
    public boolean txnPrepare(long txnId) throws IOException {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                te.mTxn.prepareNoRedo();
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
            mTransactions.insert(scrambledTxnId).mTxn = newTransaction(txnId);
        } else {
            // Enter nested scope of an existing transaction.

            runTask(te, new Worker.Task() {
                public void run() throws IOException {
                    te.mTxn.enter();
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnRollback(long txnId) {
        TxnEntry te = getTxnEntry(txnId);

        runTask(te, new Worker.Task() {
            public void run() {
                te.mTxn.exit();
            }
        });

        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) {
        TxnEntry te = removeTxnEntry(txnId);

        if (te != null) {
            runTask(te, new Worker.Task() {
                public void run() {
                    te.mTxn.reset();
                }
            });
        }

        return true;
    }

    @Override
    public boolean txnCommit(long txnId) {
        TxnEntry te = getTxnEntry(txnId);
        runTask(te, new CommitTask(te));
        return true;
    }

    private static final class CommitTask extends Worker.Task {
        private final TxnEntry mEntry;

        CommitTask(TxnEntry entry) {
            mEntry = entry;
        }

        @Override
        public void run() throws IOException {
            mEntry.mTxn.commit();
        }
    }

    @Override
    public boolean txnCommitFinal(long txnId) {
        TxnEntry te = removeTxnEntry(txnId);
        if (te != null) {
            runTask(te, new CommitFinalTask(te));
        }
        return true;
    }

    private static final class CommitFinalTask extends Worker.Task {
        private final TxnEntry mEntry;

        CommitFinalTask(TxnEntry entry) {
            mEntry = entry;
        }

        @Override
        public void run() throws IOException {
            mEntry.mTxn.commitAll();
        }
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
            te.mTxn = txn;
            newTxn = true;
        } else {
            // Enter nested scope of an existing transaction.
            txn = te.mTxn;
            newTxn = false;
        }

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (!newTxn) {
                    txn.enter();
                }
                if (lock != null) {
                    txn.push(lock);
                }
                doStore(txn, indexId, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                doStore(txn, indexId, key, value);
            }
        });

        return true;
    }

    @Override
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                doStore(txn, indexId, key, value);
                txn.commit();
            }
        });

        return true;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws LockFailureException
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
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                doStore(txn, indexId, key, value);
                txn.commitAll();
            }
        };

        if (te == null) {
            runTaskAnywhere(task);
        } else {
            runTask(te, task);
        }

        return true;
    }

    private void doStore(Transaction txn, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);

        while (ix != null) {
            try {
                ix.store(txn, key, value);
                return;
            } catch (Throwable e) {
                ix = reopenIndex(e, indexId);
            }
        }
    }

    @Override
    public boolean cursorRegister(long cursorId, long indexId) throws IOException {
        long scrambledCursorId = mix(cursorId);
        Index ix = getIndex(indexId);
        if (ix != null) {
            TreeCursor tc = (TreeCursor) ix.newCursor(Transaction.BOGUS);
            tc.mKeyOnly = true;
            synchronized (mCursors) {
                mCursors.insert(scrambledCursorId).mCursor = tc;
            }
        }
        return true;
    }

    @Override
    public boolean cursorUnregister(long cursorId) {
        long scrambledCursorId = mix(cursorId);
        CursorEntry ce;
        synchronized (mCursors) {
            ce = mCursors.remove(scrambledCursorId);
        }

        if (ce != null) {
            // Need to enqueue a task with the correct thread, to ensure that the reset doesn't
            // run concurrently with any unfinished cursor actions.
            TreeCursor tc = ce.mCursor;
            Worker w = ce.mWorker;
            if (w == null) {
                // Cursor was never actually used.
                tc.reset();
            } else {
                w.enqueue(new Worker.Task() {
                    public void run() throws IOException {
                        tc.reset();
                    }
                });
            }
        }

        return true;
    }

    @Override
    public boolean cursorStore(long cursorId, long txnId, byte[] key, byte[] value)
        throws LockFailureException
    {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            return true;
        }

        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;
        TreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        ce.mKey = key;
        Lock lock = txn.lockUpgradableNoPush(tc.mTree.mId, key);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                TreeCursor tc = ce.mCursor;
                tc.mTxn = txn;
                tc.findNearby(key);

                do {
                    try {
                        tc.store(value);
                        tc.mValue = Cursor.NOT_LOADED;
                        break;
                    } catch (ClosedIndexException e) {
                        tc = reopenCursor(e, ce);
                    }
                } while (tc != null);
            }
        });

        return true;
    }

    @Override
    public boolean cursorFind(long cursorId, long txnId, byte[] key) throws LockFailureException {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            return true;
        }

        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;
        TreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        ce.mKey = key;
        Lock lock = txn.lockUpgradableNoPush(tc.mTree.mId, key);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                TreeCursor tc = ce.mCursor;
                tc.mTxn = txn;
                tc.findNearby(key);
            }
        });

        return true;
    }

    @Override
    public boolean cursorValueSetLength(long cursorId, long txnId, long length)
        throws LockFailureException
    {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            return true;
        }

        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;
        TreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(tc.mTree.mId, ce.mKey);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                TreeCursor tc = ce.mCursor;
                tc.mTxn = txn;

                do {
                    try {
                        tc.valueLength(length);
                        break;
                    } catch (ClosedIndexException e) {
                        tc = reopenCursor(e, ce);
                    }
                } while (tc != null);
            }
        });

        return true;
    }

    @Override
    public boolean cursorValueWrite(long cursorId, long txnId,
                                    long pos, byte[] buf, int off, int len)
        throws LockFailureException
    {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            return true;
        }

        // Need to copy the data, since it will be accessed by another thread.
        // TODO: Use a buffer pool.
        byte[] data = Arrays.copyOfRange(buf, off, off + len);

        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;
        TreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(tc.mTree.mId, ce.mKey);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                TreeCursor tc = ce.mCursor;
                tc.mTxn = txn;

                do {
                    try {
                        tc.valueWrite(pos, data, 0, data.length);
                        break;
                    } catch (ClosedIndexException e) {
                        tc = reopenCursor(e, ce);
                    }
                } while (tc != null);
            }
        });

        return true;
    }

    @Override
    public boolean cursorValueClear(long cursorId, long txnId, long pos, long length)
        throws LockFailureException
    {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            return true;
        }

        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;
        TreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(tc.mTree.mId, ce.mKey);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                TreeCursor tc = ce.mCursor;
                tc.mTxn = txn;

                do {
                    try {
                        tc.valueClear(pos, length);
                        break;
                    } catch (ClosedIndexException e) {
                        tc = reopenCursor(e, ce);
                    }
                } while (tc != null);
            }
        });

        return true;
    }

    private void runCursorTask(CursorEntry ce, TxnEntry te, Worker.Task task) {
        Worker w = ce.mWorker;
        if (w == null) {
            w = te.mWorker;
            if (w == null) {
                w = runTaskAnywhere(task);
                te.mWorker = w;
            } else {
                w.enqueue(task);
            }
            ce.mWorker = w;
        } else {
            Worker txnWorker = te.mWorker;
            if (w != txnWorker) {
                if (txnWorker == null) {
                    txnWorker = w;
                    te.mWorker = w;
                } else {
                    // When the transaction changes, the assigned worker can change too. Wait
                    // for tasks in the original queue to drain before enqueueing against the
                    // new worker. This prevents cursor operations from running out of order,
                    // or from accessing the cursor concurrently.
                    w.join(false);
                    ce.mWorker = txnWorker;
                }
            }
            txnWorker.enqueue(task);
        }
    }

    @Override
    public boolean txnLockShared(long txnId, long indexId, byte[] key)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockSharedNoPush(indexId, key);

        // TODO: No need to run special task if transaction was just created.
        if (lock != null) {
            runTask(te, new LockPushTask(txn, lock));
        }

        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        // TODO: No need to run special task if transaction was just created.
        if (lock != null) {
            runTask(te, new LockPushTask(txn, lock));
        }

        return true;
    }

    private static final class LockPushTask extends Worker.Task {
        private final LocalTransaction mTxn;
        private final Lock mLock;

        LockPushTask(LocalTransaction txn, Lock lock) {
            mTxn = txn;
            mLock = lock;
        }

        @Override
        public void run() {
            mTxn.push(mLock);
        }
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.lockUpgradableNoPush(indexId, key);

        // TODO: Can acquire exclusive at first, but must know what push mode to use (0 or 1)
        // TODO: No need to run special task if transaction was just created.
        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);
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
            public void run() throws IOException {
                handler.redo(mDatabase, txn, message);
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
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);

                handler.redo(mDatabase, txn, message, indexId, key);
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
     * @return TxnEntry via scrambled transaction id
     */
    private TxnEntry getTxnEntry(long txnId) {
        long scrambledTxnId = mix(txnId);
        TxnEntry te = mTransactions.get(scrambledTxnId);

        if (te == null) {
            // Create transaction on demand if necessary. Startup transaction recovery only
            // applies to those which generated undo log entries.
            LocalTransaction txn = newTransaction(txnId);
            te = mTransactions.insert(scrambledTxnId);
            te.mTxn = txn;
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
     * @return TxnEntry via scrambled transaction id; null if not found
     */
    private TxnEntry removeTxnEntry(long txnId) {
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
     * @param cleanup non-null to remove cleared references
     * @return null if not found
     */
    private Index openIndex(Transaction txn, long indexId, Object cleanup) throws IOException {
        Index ix = mDatabase.anyIndexById(txn, indexId);
        if (ix == null) {
            return null;
        }

        SoftReference<Index> ref = new SoftReference<>(ix);

        synchronized (mIndexes) {
            mIndexes.insert(indexId).value = ref;

            if (cleanup != null) {
                // Remove entries for all other cleared references, freeing up memory.
                mIndexes.traverse(e -> e.value.get() == null);
            }
        }

        return ix;
    }

    /**
     * Opens the index and puts it into the local cache, replacing the existing entry.
     *
     * @return null if not found
     */
    private Index openIndex(long indexId) throws IOException {
        return openIndex(null, indexId, null);
    }

    /**
     * Check if user closed the shared index reference, and re-open it. Returns null if index
     * is truly gone.
     *
     * @param e cause, which is rethrown if not due to index closure
     */
    private Index reopenIndex(Throwable e, long indexId) throws IOException {
        checkClosedIndex(e);
        return openIndex(indexId);
    }

    /**
     * @return CursorEntry via scrambled cursor id
     */
    private CursorEntry getCursorEntry(long cursorId) {
        long scrambledCursorId = mix(cursorId);
        CursorEntry ce = mCursors.get(scrambledCursorId);
        if (ce == null) {
            synchronized (mCursors) {
                ce = mCursors.get(scrambledCursorId);
            }
        }
        return ce;
    }

    /**
     * Check if user closed the shared index reference, and re-open the affected
     * cursor. Returns null if index is truly gone.
     *
     * @param e cause, which is rethrown if not due to index closure
     */
    private TreeCursor reopenCursor(Throwable e, CursorEntry ce) throws IOException {
        checkClosedIndex(e);

        long scrambledCursorId = mix(ce.key);
        TreeCursor tc = ce.mCursor;
        Index ix = openIndex(tc.mTree.mId);

        if (ix == null) {
            synchronized (mCursors) {
                mCursors.remove(scrambledCursorId);
            }
        } else {
            LocalTransaction txn = tc.mTxn;
            byte[] key = tc.key();
            tc.reset();

            tc = (TreeCursor) ix.newCursor(txn);
            tc.mKeyOnly = true;
            tc.mTxn = txn;
            tc.find(key);

            synchronized (mCursors) {
                if (ce == mCursors.get(scrambledCursorId)) {
                    ce.mCursor = tc;
                    return tc;
                }
            }
        }

        tc.reset();

        return null;
    }

    private static void checkClosedIndex(final Throwable e) {
        Throwable cause = e;
        while (true) {
            if (cause instanceof ClosedIndexException) {
                break;
            }
            cause = cause.getCause();
            if (cause == null) {
                rethrow(e);
            }
        }
    }

    private void decode() {
        final ReplRedoDecoder decoder = mDecoder;
        LHashTable.Obj<LocalTransaction> remaining;

        try {
            while (!decoder.run(this));

            // End of stream reached, and so local instance is now the leader.

            // Wait for work to complete.
            if (mWorkerGroup != null) {
                mWorkerGroup.join(false);
            }

            // Rollback any lingering non-2PC transactions.
            remaining = doReset();
        } catch (Throwable e) {
            fail(e);
            return;
        } finally {
            decoder.mDeactivated = true;
        }

        RedoWriter redo;

        try {
            redo = mController.leaderNotify();
        } catch (UnmodifiableReplicaException e) {
            // Should already be receiving again due to this exception.
            return;
        } catch (Throwable e) {
            // Could try to switch to receiving mode, but panic seems to be the safe option.
            closeQuietly(mDatabase, e);
            return;
        }

        if (mDatabase.shouldInvokeRecoveryHandler(remaining) && redo != null) {
            mDatabase.invokeRecoveryHandler(remaining, redo);
        }
    }

    private TransactionHandler customHandler() throws DatabaseException {
        TransactionHandler handler = mDatabase.mCustomTxnHandler;
        if (handler == null) {
            throw new DatabaseException("Custom transaction handler is not installed");
        }
        return handler;
    }

    void fail(Throwable e) {
        fail(e, false);
    }

    void fail(Throwable e, boolean isUncaught) {
        if (!mDatabase.isClosed()) {
            EventListener listener = mDatabase.eventListener();
            if (listener != null) {
                listener.notify(EventType.REPLICATION_PANIC,
                                "Unexpected replication exception: %1$s", rootCause(e));
            } else if (isUncaught) {
                Thread t = Thread.currentThread();
                t.getThreadGroup().uncaughtException(t, e);
            } else {
                uncaught(e);
            }
        }
        // Panic.
        closeQuietly(mDatabase, e);
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
    }

    static final class TxnTable extends LHashTable<TxnEntry> {
        TxnTable(int capacity) {
            super(capacity);
        }

        @Override
        protected TxnEntry newEntry() {
            return new TxnEntry();
        }
    }

    static final class CursorEntry extends LHashTable.Entry<CursorEntry> {
        TreeCursor mCursor;
        Worker mWorker;
        byte[] mKey;
    }

    static final class CursorTable extends LHashTable<CursorEntry> {
        CursorTable(int capacity) {
            super(capacity);
        }

        @Override
        protected CursorEntry newEntry() {
            return new CursorEntry();
        }
    }
}
