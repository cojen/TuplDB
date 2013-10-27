/*
 *  Copyright 2011-2013 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoLogRecovery
 */
class RedoLogApplier implements RedoVisitor {
    private final Database mDb;
    private final LHashTable.Obj<Transaction> mTransactions;
    private final LHashTable.Obj<Index> mIndexes;

    long mHighestTxnId;

    RedoLogApplier(Database db, LHashTable.Obj<Transaction> txns) {
        mDb = db;
        mTransactions = txns;
        mIndexes = new LHashTable.Obj<Index>(16);
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
    public boolean dropIndex(long indexId) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            try {
                ix.drop();
            } catch (IllegalStateException e) {
                // Assume not empty due to NO_REDO delete.
                return true;
            }
            mIndexes.remove(indexId);
        }
        return true;
    }

    @Override
    public boolean renameIndex(long indexId, byte[] newName) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            mDb.renameIndex(ix, newName, false);
        }
        return true;
    }

    @Override
    public boolean txnEnter(long txnId) throws IOException {
        Transaction txn = txn(txnId);
        if (txn == null) {
            txn = new Transaction(mDb, txnId, LockMode.UPGRADABLE_READ, 0L);
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
        Transaction txn = mTransactions.removeValue(txnId);
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

    private Transaction txn(long txnId) {
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
        Index ix = mDb.anyIndexById(indexId);
        if (ix != null) {
            // Maintain a strong reference to the index.
            mIndexes.insert(indexId).value = ix;
        }
        return ix;
    }
}
