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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.SoftReference;

import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.ext.CustomHandler;

import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;
import org.cojen.tupl.util.Runner;
import org.cojen.tupl.util.WeakPool;
import org.cojen.tupl.util.Worker;
import org.cojen.tupl.util.WorkerGroup;

import static org.cojen.tupl.core.Utils.*;

/**
 * When database is acting as a replica, this class drives all the decoding logic.
 *
 * @author Brian S O'Neill
 * @see ReplController
 */
/*P*/
class ReplEngine implements RedoVisitor, ThreadFactory {
    private static final int MAX_QUEUE_SIZE = 1000;
    private static final int MAX_KEEP_ALIVE_MILLIS = 60_000;
    static final long INFINITE_TIMEOUT = -1L;
    static final String ATTACHMENT = "replication";

    // Hash spreader. Based on rounded value of 2 ** 63 * (sqrt(5) - 1) equivalent 
    // to unsigned 11400714819323198485.
    private static final long HASH_SPREAD = -7046029254386353131L;

    private static final VarHandle cDecodeExceptionHandle;

    static {
        try {
            cDecodeExceptionHandle =
                MethodHandles.lookup().findVarHandle
                (ReplEngine.class, "mDecodeException", Throwable.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    final StreamReplicator mRepl;
    final LocalDatabase mDatabase;

    final ReplController mController;

    // Used for finishing pending transactions created by the leader.
    final PendingTxnFinisher mFinisher;

    private final WorkerGroup mWorkerGroup;

    private final Latch mDecodeLatch;

    private final TxnTable mTransactions;

    // Used by stashForRecovery and awaitPreparedTransactions.
    private LatchCondition mStashedCond;

    // Maintain soft references to indexes, allowing them to get closed if not used for
    // awhile. Without the soft references, LocalDatabase maintains only weak references to
    // indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private final CursorTable mCursors;

    private ReplDecoder mDecoder;

    private volatile Throwable mDecodeException;

    /**
     * @param txns recovered transactions; can be null; cleared as a side-effect; keyed by
     * unscrambled id
     */
    ReplEngine(StreamReplicator repl, int maxThreads,
               LocalDatabase db, LHashTable.Obj<LocalTransaction> txns,
               LHashTable.Obj<BTreeCursor> cursors)
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

        mRepl = repl;
        mDatabase = db;

        mController = new ReplController(this);

        mFinisher = new PendingTxnFinisher(maxThreads);

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

        mIndexes = new LHashTable.Obj<>(0);

        final CursorTable cursorTable;
        if (cursors == null) {
            cursorTable = new CursorTable(4);
        } else {
            cursorTable = new CursorTable(cursors.size());

            cursors.traverse(ce -> {
                long scrambledCursorId = mix(ce.key);
                cursorTable.insert(scrambledCursorId).recovered(ce.value);
                // Delete entry.
                return true;
            });
        }

        mCursors = cursorTable;
    }

    public ReplController initWriter(long redoNum) {
        mController.initCheckpointNumber(redoNum);
        return mController;
    }

    /**
     * @return the ReplDecoder instance or null if failed
     */
    public ReplDecoder startReceiving(long initialPosition, long initialTxnId) {
        ReplDecoder decoder;

        try {
            mDecodeLatch.acquireExclusive();
            try {
                decoder = mDecoder;
                if (decoder == null || decoder.mDeactivated) {
                    mDecoder = decoder = new ReplDecoder
                        (mRepl, initialPosition, initialTxnId, mDecodeLatch);
                    newThread(this::decode).start();
                }
            } finally {
                mDecodeLatch.releaseExclusive();
            }
        } catch (Throwable e) {
            fail(e);
            return null;
        }

        return decoder;
    }

    @Override
    public Thread newThread(Runnable r) {
        return newThread(r, "ReplicationReceiver");
    }

    private Thread newThread(Runnable r, String namePrefix) {
        var t = new Thread(r);
        t.setDaemon(true);
        t.setName(namePrefix + '-' + Long.toUnsignedString(t.getId()));
        t.setUncaughtExceptionHandler((thread, exception) -> fail(exception, true));
        return t;
    }

    @Override
    public boolean reset() throws IOException {
        final LHashTable.Obj<LocalTransaction> remaining = doReset(false);

        if (remaining != null) {
            remaining.traverse(entry -> {
                mTransactions.insert(entry.key).mTxn = entry.value;
                return false;
            });
        }

        return true;
    }

    /**
     * Note: Caller must hold mDecodeLatch exclusively.
     *
     * @param interrupt pass true to interrupt all worker threads such that they exit when done
     * @return remaining prepared transactions, or null if none; keyed by scrambled id
     */
    private LHashTable.Obj<LocalTransaction> doReset(boolean interrupt) throws IOException {
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
            // Assume that mDecodeLatch is held exclusively.
            mWorkerGroup.join(interrupt);
        }

        synchronized (mCursors) {
            mCursors.traverse(entry -> {
                BTreeCursor cursor = entry.mCursor;
                mDatabase.unregisterCursor(cursor.mCursorId);
                reset(cursor);
                return true;
            });
        }

        mDatabase.emptyLingeringTrash(remaining);

        return remaining == null || remaining.size() == 0 ? null : remaining;
    }

    /**
     * Reset and interrupt all worker threads. Intended to be used by RedoLogApplier subclass.
     *
     * @return remaining prepared transactions, or null if none; keyed by scrambled id
     */
    public LHashTable.Obj<LocalTransaction> finish() throws IOException {
        mDecodeLatch.acquireExclusive();
        try {
            EventListener listener = mDatabase.eventListener();

            if (listener != null) {
                int amt = mTransactions.size();
                if (amt != 0) {
                    listener.notify(EventType.RECOVERY_PROCESS_REMAINING,
                                    "Processing remaining transactions: %1$d", amt);
                }
            }

            return doReset(true);
        } finally {
            mDecodeLatch.releaseExclusive();
        }
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
            // Assume that mDecodeLatch is held exclusively.
            mWorkerGroup.join(false);
        }

        // Call with decode latch held, suspending checkpoints.
        mRepl.controlMessageReceived(mDecoder.mIn.mPos, message);

        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        // Must acquire the lock before task is enqueued.

        var locker = new Locker(mDatabase.mLockManager) {
            // Superclass doesn't support attachments by default.
            @Override
            public Object attachment() {
                return ReplEngine.this.attachment();
            }
        };

        locker.doTryLockUpgradable(indexId, key, INFINITE_TIMEOUT);

        runTaskAnywhere(new Worker.Task() {
            public void run() throws IOException {
                try {
                    // Full exclusive lock is required.
                    locker.doLockExclusive(indexId, key, INFINITE_TIMEOUT);

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

        if (ix != null) {
            try {
                mDatabase.renameIndex(ix, newName, txnId);
            } catch (RuntimeException e) {
                EventListener listener = mDatabase.eventListener();
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_WARNING,
                                    "Unable to rename index: %1$s", rootCause(e));
                }
            }
        }

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

                mDatabase.redoMoveToTrash(txn, indexId);

                try {
                    txn.commit();
                } finally {
                    txn.exit();
                }

                if (ix != null) {
                    ix.close();
                }

                Runnable task = mDatabase.replicaDeleteTree(indexId);

                // Allow index deletion to run concurrently. If multiple deletes are received
                // concurrently, then the application is likely doing concurrent deletes. If
                // there's an exception, the index will get fully deleted when the database is
                // re-opened.
                Runner.start("IndexDeletion-" + (ix == null ? indexId : ix.nameString()), task);
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
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

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
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

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
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

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
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

        var task = new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                // Manually lock and store with a bogus transaction to avoid creating an
                // unnecessary undo log entry.
                txn.doLockExclusive(indexId, key, INFINITE_TIMEOUT);
                doStore(Transaction.BOGUS, indexId, key, value);
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
            var tc = (BTreeCursor) ix.newCursor(Transaction.BOGUS);
            tc.mKeyOnly = true;
            tc.mCursorId = cursorId;
            register(tc);
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
            BTreeCursor tc = ce.mCursor;
            Worker w = ce.mWorker;
            if (w == null) {
                // Cursor was never actually used.
                reset(tc);
            } else {
                w.enqueue(new Worker.Task() {
                    public void run() throws IOException {
                        reset(tc);
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

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        ce.mKey = key;
        Lock lock = txn.doLockUpgradableNoPush(ce.mCursor.mTree.mId, key);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                BTreeCursor tc = findAndRegister(ce, txn, key);

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

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        ce.mKey = key;
        Lock lock = txn.doLockUpgradableNoPush(ce.mCursor.mTree.mId, key);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                findAndRegister(ce, txn, key);
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
        BTreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.doLockUpgradableNoPush(tc.mTree.mId, ce.mKey);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                BTreeCursor tc = ce.mCursor;
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
    public boolean cursorValueWrite(long cursorId, long txnId, long pos,
                                    WeakPool.Entry<byte[]> entry, byte[] buf, int off, int len)
        throws LockFailureException
    {
        CursorEntry ce = getCursorEntry(cursorId);
        if (ce == null) {
            entry.release();
            return true;
        }

        try {
            TxnEntry te = getTxnEntry(txnId);
            LocalTransaction txn = te.mTxn;
            BTreeCursor tc = ce.mCursor;

            // Acquire the lock on behalf of the transaction, but push it using the correct thread.
            Lock lock = txn.doLockUpgradableNoPush(tc.mTree.mId, ce.mKey);

            runCursorTask(ce, te, new Worker.Task() {
                public void run() throws IOException {
                    try {
                        if (lock != null) {
                            txn.push(lock);
                        }

                        BTreeCursor tc = ce.mCursor;
                        tc.mTxn = txn;

                        do {
                            try {
                                tc.valueWrite(pos, buf, off, len);
                                break;
                            } catch (ClosedIndexException e) {
                                tc = reopenCursor(e, ce);
                            }
                        } while (tc != null);
                    } finally {
                        entry.release();
                    }
                }
            });

            return true;
        } catch (Throwable e) {
            entry.release();
            throw e;
        }
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
        BTreeCursor tc = ce.mCursor;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.doLockUpgradableNoPush(tc.mTree.mId, ce.mKey);

        runCursorTask(ce, te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                BTreeCursor tc = ce.mCursor;
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
                    // or from accessing the cursor concurrently. Assume that mDecodeLatch is
                    // held exclusively.
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

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        runLockPushTask(te, te.mTxn.doLockSharedNoPush(indexId, key));

        return true;
    }

    @Override
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        runLockPushTask(te, te.mTxn.doLockUpgradableNoPush(indexId, key));

        return true;
    }

    private void runLockPushTask(TxnEntry te, Lock lock) {
        if (lock != null) {
            LocalTransaction txn = te.mTxn;
            Worker w = te.mWorker;
            if (w == null) {
                // No worker has been assigned yet, so no need to delegate.
                txn.push(lock);
            } else {
                w.enqueue(new Worker.Task() {
                    public void run() {
                        txn.push(lock);
                    }
                });
            }
        }
    }

    @Override
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key)
        throws LockFailureException
    {
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

        // Run a task in case the exclusive request must wait.
        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }
                // Must write lock request to undo log, ensuring that it survives a checkpoint
                // and then a recovery. Is more critical for prepared transactions.
                txn.lockExclusive(indexId, key, INFINITE_TIMEOUT);
            }
        });

        return true;
    }

    @Override
    public boolean txnPrepare(long txnId, long prepareTxnId,
                              int handlerId, byte[] message, boolean commit)
        throws IOException
    {
        // Run the task against the transaction being prepared, not the carrier transaction.
        TxnEntry te = getTxnEntry(prepareTxnId);
        LocalTransaction txn = te.mTxn;

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                txn.prepareRedo(handlerId, message, commit);
            }
        });

        return true;
    }

    @Override
    public boolean txnPrepareRollback(long txnId, long prepareTxnId) throws IOException {
        // Run the task against the transaction being prepared, not the carrier transaction.
        TxnEntry te = getTxnEntry(prepareTxnId);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                BTree preparedTxns = mDatabase.tryPreparedTxns();
                if (preparedTxns != null) {
                    var prepareKey = new byte[8];
                    encodeLongBE(prepareKey, 0, prepareTxnId);
                    preparedTxns.store(Transaction.BOGUS, prepareKey, null);
                }
            }
        });

        return true;
    }

    @Override
    public boolean txnCustom(long txnId, int handlerId, byte[] message) throws IOException {
        CustomHandler handler = mDatabase.findCustomRecoveryHandler(handlerId);
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                handler.redo(txn, message);
            }
        });

        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, int handlerId, byte[] message,
                                 long indexId, byte[] key)
        throws IOException
    {
        CustomHandler handler = mDatabase.findCustomRecoveryHandler(handlerId);
        TxnEntry te = getTxnEntry(txnId);
        LocalTransaction txn = te.mTxn;

        // Acquire the lock on behalf of the transaction, but push it using the correct thread.
        Lock lock = txn.doLockUpgradableNoPush(indexId, key);

        runTask(te, new Worker.Task() {
            public void run() throws IOException {
                if (lock != null) {
                    txn.push(lock);
                }

                txn.doLockExclusive(indexId, key, INFINITE_TIMEOUT);

                handler.redo(txn, message, indexId, key);
            }
        });

        return true;
    }

    /**
     * Returns the position of the next operation to decode.
     */
    long decodePosition() {
        return decoder().decodePositionOpaque();
    }

    private RedoDecoder decoder() {
        ReplDecoder decoder = mDecoder;
        if (decoder == null) {
            throw new IllegalStateException("No decoder");
        } else {
            return decoder;
        }
    }

    /**
     * Returns the position of the next operation to decode, while engine is suspended.
     */
    long suspendedDecodePosition() {
        return decoder().mDecodePosition;
    }

    /**
     * Returns the last transaction id which was decoded, while engine is suspended.
     *
     * @throws IllegalStateException if not decoding
     */
    long suspendedDecodeTransactionId() {
        ReplDecoder decoder = mDecoder;
        if (decoder != null) {
            return decoder.mDecodeTransactionId;
        }
        throw new IllegalStateException("Not decoding");
    }

    /**
     * Prevents new operations from starting and waits for in-flight operations to complete.
     *
     * @throws InterruptedIOException if wait was interrupted
     * @throws IOException if a failure had occurred decoding an operation
     */
    void suspend() throws IOException {
        // Prevent new operations from being decoded.
        try {
            mDecodeLatch.acquireExclusiveInterruptibly();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        try {
            // Wait for work to complete.
            if (mWorkerGroup != null) {
                // To call this in a thread-safe fashion, mDecodeLatch must be held.
                mWorkerGroup.join(false);
            }

            Throwable ex = mDecodeException;

            if (ex != null) {
                throw new IOException("Cannot advance decode position", ex);
            }
        } catch (Throwable e) {
            mDecodeLatch.releaseExclusive();
            throw e;
        }
    }

    void resume() {
        mDecodeLatch.releaseExclusive();
    }

    /**
     * Send an interrupt to any workers, to help the threads exit sooner.
     */
    void interrupt() {
        if (mWorkerGroup != null) {
            mWorkerGroup.interrupt();
        }
        mFinisher.interrupt();
    }

    /**
     * Called by ReplWriter.
     *
     * @param txn prepared transaction
     */
    void stashForRecovery(LocalTransaction txn) {
        long scrambledTxnId = mix(txn.id());

        mDecodeLatch.acquireExclusive();
        try {
            mTransactions.insert(scrambledTxnId).mTxn = txn;
            if (mStashedCond != null) {
                mStashedCond.signal(mDecodeLatch);
            }
        } finally {
            mDecodeLatch.releaseExclusive();
        }
    }

    /**
     * Called by ReplController.
     */
    void awaitPreparedTransactions() throws IOException {
        // Report on stuck transactions after timing out.
        long nanosTimeout = 1_000_000_000L;
        boolean report = false;

        while (!awaitPreparedTransactions(nanosTimeout, report) && !mDatabase.isClosed()) {
            // Still waiting for prepared transactions to transfer ownership. Double the
            // timeout each time, up to a maximum of one minute.
            nanosTimeout = Math.min(nanosTimeout << 1, 60_000_000_000L);
            report = true;
        }
    }

    /**
     * @return true if not waiting on any prepared transactions
     */
    private boolean awaitPreparedTransactions(long nanosTimeout, boolean report)
        throws IOException
    {
        BTree preparedTxns = mDatabase.tryPreparedTxns();

        if (preparedTxns == null) {
            return true;
        }

        long nanosEnd = System.nanoTime() + nanosTimeout;

        while (true) {
            boolean finished = true;

            try (var c = new BTreeCursor(preparedTxns)) {
                c.mTxn = LocalTransaction.BOGUS;
                c.autoload(false);

                for (byte[] key = c.firstKey(); key != null; key = c.nextKey()) {
                    long txnId = decodeLongBE(key, 0);

                    TxnEntry te;
                    long scrambledTxnId = mix(txnId);

                    mDecodeLatch.acquireExclusive();
                    try {
                        te = mTransactions.get(scrambledTxnId);
                        if (te == null) {
                            if (mStashedCond == null) {
                                mStashedCond = new LatchCondition();
                            }
                        }
                    } finally {
                        mDecodeLatch.releaseExclusive();
                    }

                    if (te == null) {
                        finished = false;
                        if (!report) {
                            continue;
                        }
                    }

                    EventListener listener = mDatabase.eventListener();
                    if (listener != null) {
                        listener.notify(EventType.RECOVERY_AWAIT_RELEASE,
                                        "Prepared transaction must be reset: %1$d", txnId);
                    }
                }
            }

            int result;

            mDecodeLatch.acquireExclusive();
            try {
                if (finished) {
                    mStashedCond = null;
                    return true;
                }

                result = mStashedCond.await(mDecodeLatch, nanosTimeout, nanosEnd);
            } finally {
                mDecodeLatch.releaseExclusive();
            }

            if (result <= 0) {
                if (result == 0) {
                    // Timed out.
                    return false;
                }
                throw new InterruptedIOException();
            }

            nanosTimeout = Math.max(nanosEnd - System.nanoTime(), 0);

            // Report again after timing out.
            report = false;
        }
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

    protected LocalTransaction newTransaction(long txnId) {
        var txn = new LocalTransaction
            (mDatabase, txnId, LockMode.UPGRADABLE_READ, INFINITE_TIMEOUT);
        txn.attach(ATTACHMENT);
        return txn;
    }

    /**
     * @return the attachment for non-transaction Locker instances
     */
    protected Object attachment() {
        return ATTACHMENT;
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
            SoftReference<Index> ref = entry.value;
            if (ref != null) {
                Index ix = ref.get();
                if (ix != null) {
                    return ix;
                }
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

        var ref = new SoftReference<Index>(ix);

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

    private void register(BTreeCursor tc) throws IOException {
        BTree cursorRegistry = mDatabase.cursorRegistry();
        CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            mDatabase.registerCursor(cursorRegistry, tc);
        } finally {
            shared.release();
        }
    }

    private BTreeCursor findAndRegister(CursorEntry ce, LocalTransaction txn, byte[] key)
        throws IOException
    {
        BTreeCursor tc = ce.mCursor;
        tc.mTxn = txn;
        tc.findNearby(key);
        register(tc);
        return tc;
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
    private BTreeCursor reopenCursor(Throwable e, CursorEntry ce) throws IOException {
        checkClosedIndex(e);

        BTreeCursor tc = ce.mCursor;
        Index ix = openIndex(tc.mTree.mId);

        if (ix == null) {
            synchronized (mCursors) {
                mCursors.remove(ce.key);
            }
        } else {
            long cursorId = tc.mCursorId;

            LocalTransaction txn = tc.mTxn;
            byte[] key = tc.key();
            reset(tc);

            tc = (BTreeCursor) ix.newCursor(txn);
            tc.mKeyOnly = true;
            tc.mTxn = txn;
            tc.mCursorId = cursorId;
            // After cursor id has been assigned, call findNearby instead of find. The find
            // method unregisters the cursor, which then tries to write a redo log entry.
            tc.findNearby(key);

            synchronized (mCursors) {
                if (ce == mCursors.get(ce.key)) {
                    ce.mCursor = tc;
                    return tc;
                }
            }
        }

        reset(tc);

        return null;
    }

    private static void checkClosedIndex(final Throwable e) {
        Throwable cause = e;
        while (!(cause instanceof ClosedIndexException)) {
            cause = cause.getCause();
            if (cause == null) {
                rethrow(e);
            }
        }
    }

    private void decode() {
        final ReplDecoder decoder = mDecoder;
        LHashTable.Obj<LocalTransaction> remaining;

        try {
            while (!decoder.run(this));

            // End of stream reached, and so local instance is now the leader.

            mDecodeLatch.acquireExclusive();
            try {
                // Wait for work to complete.
                if (mWorkerGroup != null) {
                    // Can only call mWorkerGroup when mDecodeLatch is held. Otherwise, call
                    // isn't thread-safe.
                    mWorkerGroup.join(false);
                }

                remaining = doReset(false);
            } finally {
                mDecodeLatch.releaseExclusive();
            }
        } catch (Throwable e) {
            fail(e);
            return;
        } finally {
            decoder.mDeactivated = true;
            // No need to reference these anymore.
            synchronized (mIndexes) {
                mIndexes.clear(0);
            }
        }

        StreamReplicator.Writer writer = decoder.extractWriter();

        if (writer == null) {
            // Panic.
            fail(new IllegalStateException("No writer for the leader"));
            return;
        }

        RedoWriter redo = null;

        try {
            redo = mController.leaderNotify(writer);
        } catch (UnmodifiableReplicaException e) {
            // Should already be receiving again due to this exception.
        } catch (Throwable e) {
            // Could try to switch to receiving mode, but panic seems to be the safe option.
            closeQuietly(mDatabase, e);
            return;
        }

        if (remaining != null) {
            if (redo != null) {
                final var fredo = redo;
                Runner.start(() -> {
                    mDatabase.invokeRecoveryHandler(remaining, fredo);
                });
            } else {
                mDatabase.invokeRecoveryHandler(remaining, null);

                remaining.traverse(entry -> {
                    stashForRecovery(entry.value);
                    return true;
                });
            }
        }
    }

    void fail(Throwable e) {
        fail(e, false);
    }

    void fail(Throwable e, boolean isUncaught) {
        // Capture the first failure only. Setting it ensures that the decode position cannot
        // advance when the next checkpoint runs.
        cDecodeExceptionHandle.compareAndSet(this, null, e);

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

    private void reset(BTreeCursor cursor) {
        // Clear cursor id first, to prevent reset from writing a redo log entry.
        long cursorId = cursor.mCursorId;
        cursor.mCursorId = 0;

        cursor.reset();

        if (cursorId != 0) {
            mDatabase.unregisterCursor(cursorId);
        }
    }

    static final class CursorEntry extends LHashTable.Entry<CursorEntry> {
        BTreeCursor mCursor;
        Worker mWorker;
        byte[] mKey;

        void recovered(BTreeCursor c) {
            mCursor = c;
            mKey = c.key();
        }
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
