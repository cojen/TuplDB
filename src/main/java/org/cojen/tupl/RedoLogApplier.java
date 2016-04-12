/*
 *  Copyright 2011-2015 Cojen.org
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

import org.cojen.tupl.ext.TransactionHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoLogRecovery
 */
/*P*/
final class RedoLogApplier implements RedoVisitor {
    private final LocalDatabase mDatabase;
    private final LHashTable.Obj<LocalTransaction> mTransactions;
    private final LHashTable.Obj<Index> mIndexes;

    long mHighestTxnId;

    RedoLogApplier(LocalDatabase db, LHashTable.Obj<LocalTransaction> txns) {
        mDatabase = db;
        mTransactions = txns;
        mIndexes = new LHashTable.Obj<>(16);
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
    public boolean reset() {
        return true;
    }

    @Override
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        // No need to actually acquire a lock for log based recovery.
        return storeNoLock(indexId, key, value);
    }

    @Override
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            ix.store(Transaction.BOGUS, key, value);
        }
        return true;
    }

    @Override
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException {
        checkHighest(txnId);
        Index ix = openIndex(indexId);
        if (ix != null) {
            mDatabase.renameIndex(ix, newName, txnId);
        }
        return true;
    }

    @Override
    public boolean deleteIndex(long txnId, long indexId) throws IOException {
        LocalTransaction txn = txn(txnId);

        // Close the index for now. After recovery is complete, trashed indexes are deleted in
        // a separate thread.

        Index ix;
        {
            LHashTable.ObjEntry<Index> entry = mIndexes.remove(indexId);
            if (entry == null) {
                ix = mDatabase.anyIndexById(txn, indexId);
            } else {
                ix = entry.value;
            }
        }

        if (ix != null) {
            ix.close();
        }

        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        LocalTransaction txn = txn(txnId);
        if (txn == null) {
            txn = new LocalTransaction(mDatabase, txnId, LockMode.UPGRADABLE_READ, 0L);
            mTransactions.insert(txnId).value = txn;
        } else {
            txn.enter();
        }
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.exit();
        }
        return true;
    }

    @Override
    public boolean txnRollbackFinal(long txnId) throws IOException {
        checkHighest(txnId);
        Transaction txn = mTransactions.removeValue(txnId);
        if (txn != null) {
            txn.reset();
        }
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            txn.commit();
            txn.exit();
        }
        return true;
    }

    @Override
    public boolean txnCommitFinal(long txnId) throws IOException {
        checkHighest(txnId);
        LocalTransaction txn = mTransactions.removeValue(txnId);
        if (txn != null) {
            txn.commitAll();
        }
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        Transaction txn = txn(txnId);
        if (txn != null) {
            Index ix = openIndex(indexId);
            if (ix != null) {
                ix.store(txn, key, value);
            }
        }
        return true;
    }

    @Override
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnStore(txnId, indexId, key, value);
        return txnCommitFinal(txnId);
    }

    @Override
    public boolean txnCustom(long txnId, byte[] message) throws IOException {
        Transaction txn = txn(txnId);
        if (txn != null) {
            LocalDatabase db = mDatabase;
            TransactionHandler handler = db.mCustomTxnHandler;
            if (handler == null) {
                throw new DatabaseException("Custom transaction handler is not installed");
            }
            handler.redo(db, txn, message);
        }
        return true;
    }

    @Override
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        Transaction txn = txn(txnId);
        if (txn != null) {
            LocalDatabase db = mDatabase;
            TransactionHandler handler = db.mCustomTxnHandler;
            if (handler == null) {
                throw new DatabaseException("Custom transaction handler is not installed");
            }
            txn.lockExclusive(indexId, key);
            handler.redo(db, txn, message, indexId, key);
        }
        return true;
    }

    private LocalTransaction txn(long txnId) {
        checkHighest(txnId);
        return mTransactions.getValue(txnId);
    }

    private void checkHighest(long txnId) {
        if (txnId > mHighestTxnId) {
            mHighestTxnId = txnId;
        }
    }

    private Index openIndex(long indexId) throws IOException {
        LHashTable.ObjEntry<Index> entry = mIndexes.get(indexId);
        if (entry != null) {
            return entry.value;
        }
        Index ix = mDatabase.anyIndexById(indexId);
        if (ix != null) {
            // Maintain a strong reference to the index.
            mIndexes.insert(indexId).value = ix;
        }
        return ix;
    }
}
