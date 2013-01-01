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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ReplRedoProcessor implements RedoVisitor {
    // FIXME: configurable
    private final long mTimeoutNanos = 10L * 1000 * 1000 * 1000;

    private final Database mDatabase;

    // Maintain soft references to indexes, allowing them to get closed if not
    // used for awhile. Without the soft references, Database maintains only
    // weak references to indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private final LHashTable.Obj<Transaction> mTransactions;

    ReplRedoProcessor(Database db) {
        mDatabase = db;
        mIndexes = new LHashTable.Obj<SoftReference<Index>>(16);
        mTransactions = new LHashTable.Obj<Transaction>(16);
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
    public boolean close(long timestamp) throws IOException {
        // FIXME: As described in ReplicationManager comment, special log
        // message is required to indicate leadership change. This ensures that
        // replicas kill off all active transactions. Use OP_CLOSE, but it
        // doesn't encode the new leader id. It isn't really required,
        // however. Note that OP_CLOSE implies all active transactions
        // rollback, but OP_END_FILE doesn't.

        mTransactions.traverse
            (new LHashTable.Visitor<LHashTable.ObjEntry<Transaction>, IOException>()
        {
            public boolean visit(LHashTable.ObjEntry<Transaction> entry) throws IOException {
                entry.value.reset();
                return true;
            }
        });

        return true;
    }

    @Override
    public boolean endFile(long timestamp) {
        return true;
    }

    @Override
    public boolean reset(long txnId) {
        return true;
    }

    // FIXME: To allow for concurrency, locks (upgradable) should be explicitly
    // performed here. After lock has been acquired, log processing can advance
    // concurrently. Additional threads can also be managed in this class.

    // FIXME: Deadlocks and timeouts are transient failures. When it happens,
    // deliver an event and retry. Replication can be plugged up indefinitely
    // if lock cannot be acquired at all. This is by design, since lock
    // acquisition order must be serialized. Long lock timeouts here are fine.
    // If deadlocked, user thread is likely using a shorter timeout and will
    // get the exception. No deadlocks will be created by replication itself.

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix == null) {
            // TODO: deliver an error event
            return true;
        }

        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_REDO);

        try {
            txn.lockUpgradable(ix.getId(), key, mTimeoutNanos);
        } catch (Throwable e) {
            txn.reset();
            throw Utils.rethrow(e);
        }

        // FIXME: spawn next thread and return false; if no thread, continue in this thread

        try {
            ix.store(txn, key, value);
            txn.commit();
        } finally {
            txn.reset();
        }

        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_REDO);
        mTransactions.insert(txnId).value = txn;
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        // FIXME
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an error event
            return true;
        }
        txn.exit();
        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        // FIXME
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an error event
            return true;
        }
        txn.reset();
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an error event
            return true;
        }
        txn.commit();
        txn.exit();
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        // FIXME
        return true;
    }

    private Transaction removeTxn(long txnId) {
        Transaction txn;
        LHashTable.ObjEntry<Transaction> entry = mTransactions.get(txnId);
        if (entry == null) {
            txn = null;
        } else {
            txn = entry.value;
            mTransactions.remove(txnId);
        }
        return txn;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = openIndex(indexId);
        if (ix == null) {
            // TODO: deliver an error event
            return true;
        }

        Transaction txn;
        LHashTable.ObjEntry<Transaction> entry = mTransactions.get(txnId);
        if (entry == null) {
            // TODO: deliver an error event; normal if after rollback due to leader change
            return true;
        } else {
            txn = entry.value;
        }

        txn.lockUpgradable(ix.getId(), key, mTimeoutNanos);

        // FIXME: spawn next thread and return false; if no thread, continue in this thread

        ix.store(txn, key, value);

        return true;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Index ix = openIndex(indexId);
        if (ix == null) {
            // TODO: deliver an error event
            return true;
        }

        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an error event
            return true;
        }

        txn.lockUpgradable(ix.getId(), key, mTimeoutNanos);

        // FIXME: spawn next thread and return false; if no thread, continue in this thread

        ix.store(txn, key, value);
        txn.commit();
        txn.exit();

        return true;
    }

    private Index openIndex(long indexId) throws IOException {
        LHashTable.ObjEntry<SoftReference<Index>> entry = mIndexes.get(indexId);
        if (entry != null) {
            Index ix = entry.value.get();
            if (ix != null) {
                return ix;
            }
        }

        Index ix = mDatabase.anyIndexById(indexId);
        if (ix != null) {
            SoftReference<Index> ref = new SoftReference<Index>(ix);
            if (entry == null) {
                mIndexes.insert(indexId).value = ref;
            } else {
                entry.value = ref;
            }
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
}
