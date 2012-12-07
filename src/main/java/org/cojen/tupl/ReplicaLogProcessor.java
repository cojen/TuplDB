/*
 *  Copyright 2012 Brian S O'Neill
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
class ReplicaLogProcessor implements RedoLogVisitor {
    private final Database mDatabase;

    // Maintain soft references to indexes, allowing them to get closed if not
    // used for awhile. Without the soft references, Database maintains only
    // weak references to indexes. They'd get closed too soon.
    private final LHashTable.Obj<SoftReference<Index>> mIndexes;

    private final LHashTable.Obj<Transaction> mTransactions;

    ReplicaLogProcessor(Database db) {
        mDatabase = db;
        mIndexes = new LHashTable.Obj<SoftReference<Index>>(16);
        mTransactions = new LHashTable.Obj<Transaction>(16);
    }

    @Override
    public void timestamp(long timestamp) {}

    @Override
    public void shutdown(long timestamp) {}

    @Override
    public void close(long timestamp) {}

    @Override
    public void endFile(long timestamp) {}

    // FIXME: To allow for concurrency, locking should be explicitly performed
    // here. After lock has been acquired, log processing can advance
    // concurrently. Additional threads can also be managed in this class.

    // FIXME: Deadlocks and timeouts are transient failures.

    public void store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix == null) {
            // TODO: deliver an event
            return;
        }

        Transaction txn = mDatabase.newTransaction(DurabilityMode.NO_LOG);
        try {
            ix.store(txn, key, value);
            txn.commit();
        } catch (Throwable e) {
            txn.reset();
            Utils.rethrow(e);
        }
    }

    public void txnRollback(long txnId) throws IOException {
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an event
            return;
        }
        txn.exit();
    }

    public void txnRollbackChild(long txnId, long parentTxnId) throws IOException {
        // FIXME: use parentTxnId?
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an event
            return;
        }
        txn.exit();
    }

    public void txnCommit(long txnId) throws IOException {
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an event
            return;
        }
        txn.commit();
    }

    public void txnCommitChild(long txnId, long parentTxnId) throws IOException {
        // FIXME: use parentTxnId?
        Transaction txn = removeTxn(txnId);
        if (txn == null) {
            // TODO: deliver an event
            return;
        }
        txn.commit();
    }

    private Transaction removeTxn(long txnId) {
        Transaction txn;
        synchronized (mTransactions) {
            LHashTable.ObjEntry<Transaction> entry = mTransactions.get(txnId);
            if (entry == null) {
                txn = null;
            } else {
                txn = entry.value;
                mTransactions.remove(txnId);
            }
        }
        return txn;
    }

    // FIXME: This design does not permit nested transactions! The redo log
    // format needs to be updated. Add an op (lazily flushed) which describes
    // newly created parent-child relationships. OP_TXN_BEGIN_CHILD is
    // commented out. Feature only needs to be enabled for replication. Child
    // txnId maps to shared Transaction object.

    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix == null) {
            // TODO: deliver an event
            return;
        }

        Transaction txn;
        synchronized (mTransactions) {
            LHashTable.ObjEntry<Transaction> entry = mTransactions.get(txnId);
            if (entry == null) {
                txn = mDatabase.newTransaction(DurabilityMode.NO_LOG);
                mTransactions.insert(txnId).value = txn;
            } else {
                txn = entry.value;
            }
        }

        ix.store(txn, key, value);
    }

    private Index openIndex(long indexId) throws IOException {
        synchronized (mIndexes) {
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
}
