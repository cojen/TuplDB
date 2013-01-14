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

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ReplRedoReceiver extends Latch implements RedoVisitor {
    private final Database mDb;

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

    private RedoDecoder mDecoder;
    private ReplRedoLog mLog;

    /**
     * @param txns recovered transactions; can be null; cleared as a side-effect
     */
    ReplRedoReceiver(Database db, LHashTable.Obj<Transaction> txns) {
        mDb = db;
        mIndexes = new LHashTable.Obj<SoftReference<Index>>(16);

        // FIXME: configurable
        mMaxThreads = 32;
        mTotalThreads = new AtomicInteger();
        mIdleThreads = new AtomicInteger();
        mTaskThreadSet = new ConcurrentHashMap<DecodeTask, Object>(16, 0.75f, 1);

        mLatches = new Latch[roundUpPower2(mMaxThreads * 2)];
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
                          <LHashTable.ObjEntry<Transaction>, RuntimeException>()
            {
                public boolean visit(LHashTable.ObjEntry<Transaction> entry) {
                    // Reduce hash collisions.
                    long scrambledTxnId = scramble(entry.key);
                    Latch latch = selectLatch(scrambledTxnId);
                    txnTable.insert(scrambledTxnId).init(entry.value, latch);
                    return true;
                }
            });
        }

        mTransactions = txnTable;
    }

    /**
     * @param log can be null for testing
     */
    public void setDecoder(RedoDecoder decoder, ReplRedoLog log) {
        acquireExclusive();
        try {
            if (mDecoder != null) {
                throw new IllegalStateException();
            }
            mDecoder = decoder;
            mLog = log;
            nextTask();
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public boolean reset() throws IOException {
        // Reset and discard all transactions.
        mTransactions.traverse(new LHashTable.Visitor<TxnEntry, IOException>() {
            public boolean visit(TxnEntry entry) throws IOException {
                Latch latch = entry.latch();
                try {
                    entry.mTxn.reset();
                } finally {
                    latch.releaseExclusive();
                }
                return true;
            }
        });

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean timestamp(long timestamp) {
        return true;
    }

    @Override
    public boolean shutdown(long timestamp) {
        return true;
    }

    @Override
    public boolean close(long timestamp) {
        return true;
    }

    @Override
    public boolean endFile(long timestamp) {
        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = getIndex(indexId);

        // Locks must be acquired in their original order to avoid
        // deadlock, so don't allow another task thread to run yet.
        Locker locker = mDb.mLockManager.localLocker();
        locker.lockExclusive(indexId, key, -1);

        // Allow another task thread to run while operation completes.
        nextTask();

        try {
            ix.store(Transaction.BOGUS, key, value);
        } finally {
            locker.scopeUnlockAll();
        }

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
    public boolean txnEnter(long txnId) throws IOException {
        // Reduce hash collisions.
        long scrambledTxnId = scramble(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);

        if (e == null) {
            Latch latch = selectLatch(scrambledTxnId);
            mTransactions.insert(scrambledTxnId)
                .init(new Transaction(mDb, txnId, LockMode.UPGRADABLE_READ, -1), latch);
            return true;
        }

        Latch latch = e.latch();
        try {
            // Cheap operation, so don't let another task thread run.
            e.mTxn.enter();
        } finally {
            latch.releaseExclusive();
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        TxnEntry e = getTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            e.mTxn.exit();
        } finally {
            latch.releaseExclusive();
        }

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        TxnEntry e = removeTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            // Allow another task thread to run while operation completes.
            nextTask();

            e.mTxn.reset();
        } finally {
            latch.releaseExclusive();
        }

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        TxnEntry e = getTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            // Commit is expected to complete quickly, so don't let another
            // task thread run.

            Transaction txn = e.mTxn;
            try {
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        TxnEntry e = removeTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            // Commit is expected to complete quickly, so don't let another
            // task thread run.

            Transaction txn = e.mTxn;
            try {
                txn.commit();
            } finally {
                txn.reset();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Return true and allow RedoDecoder to loop back.
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry e = getTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            Transaction txn = e.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, -1);

            // Allow another task thread to run while operation completes.
            nextTask();

            ix.store(txn, key, value);
        } finally {
            latch.releaseExclusive();
        }

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = getIndex(indexId);
        TxnEntry e = getTxnEntry(txnId);

        Latch latch = e.latch();
        try {
            Transaction txn = e.mTxn;

            // Locks must be acquired in their original order to avoid
            // deadlock, so don't allow another task thread to run yet.
            txn.lockUpgradable(indexId, key, -1);

            // Allow another task thread to run while operation completes.
            nextTask();

            try {
                ix.store(txn, key, value);
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            latch.releaseExclusive();
        }

        // Return false to prevent RedoDecoder from looping back.
        return false;
    }

    // Caller must hold exclusive latch, which is released by this method.
    private void nextTask() {
        if (mIdleThreads.get() == 0) {
            int total = mTotalThreads.get();
            if (total < mMaxThreads && mTotalThreads.compareAndSet(total, total + 1)) {
                DecodeTask task;
                try {
                    task = new DecodeTask();
                    task.start();
                } catch (Throwable e) {
                    mTotalThreads.decrementAndGet();
                    throw rethrow(e);
                }
                mTaskThreadSet.put(task, this);
            }
        }

        // Allow task thread to proceed.
        releaseExclusive();
    }

    /**
     * @return TxnEntry with scrambled transaction id
     */
    private TxnEntry getTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = scramble(txnId);
        TxnEntry e = mTransactions.get(scrambledTxnId);
        if (e == null) {
            // TODO: Throw a better exception.
            throw new DatabaseException("Transaction not found: " + txnId);
        }
        return e;
    }

    /**
     * @return TxnEntry with scrambled transaction id
     */
    private TxnEntry removeTxnEntry(long txnId) throws IOException {
        long scrambledTxnId = scramble(txnId);
        TxnEntry e = mTransactions.remove(scrambledTxnId);
        if (e == null) {
            // TODO: Throw a better exception.
            throw new DatabaseException("Transaction not found: " + txnId);
        }
        return e;
    }

    private Index getIndex(long indexId) throws IOException {
        LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.get(indexId);
        if (entry != null) {
            Index ix = entry.value.get();
            if (ix != null) {
                return ix;
            }
        }

        Index ix = mDb.anyIndexById(indexId);
        if (ix == null) {
            // TODO: Throw a better exception.
            throw new DatabaseException("Index not found: " + indexId);
        }

        SoftReference<Index> ref = new SoftReference<Index>(ix);
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
                    if (tryAcquireExclusiveNanos(IDLE_TIMEOUT_NANOS)) {
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

        RedoDecoder decoder = mDecoder;
        if (decoder == null) {
            releaseExclusive();
            return false;
        }

        try {
            if (!decoder.run(this)) {
                return true;
            }
            // End of stream reached.
            reset();
        } catch (Throwable e) {
            releaseExclusive();
            // FIXME: panic
            e.printStackTrace(System.out);
            return false;
        }

        mDecoder = null;
        ReplRedoLog log = mLog;
        mLog = null;
        releaseExclusive();

        if (log != null) {
            // FIXME: position
            log.leaderNotify(0);
        }

        return false;
    }

    private static int cTaskNumber;

    static synchronized long taskNumber() {
        return (++cTaskNumber) & 0xffffffffL;
    }

    class DecodeTask extends Thread {
        DecodeTask() {
            super("RedoReceiver-" + taskNumber());
            setDaemon(true);
        }

        public void run() {
            while (ReplRedoReceiver.this.decode());
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
