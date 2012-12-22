/*
 *  Copyright 2011-2012 Brian S O'Neill
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
    private final RedoLogTxnScanner mScanner;
    private final LHashTable.Obj<UndoLog> mUndoLogs;
    private final LHashTable.Obj<Index> mIndexes;

    RedoLogApplier(Database db, RedoLogTxnScanner scanner, LHashTable.Obj<UndoLog> undoLogs) {
        mDb = db;
        mScanner = scanner;
        mUndoLogs = undoLogs;
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
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            ix.store(Transaction.BOGUS, key, value);
        }
        return true;
    }

    @Override
    public boolean txnBegin(long txnId) {
        return true;
    }

    @Override
    public boolean txnBeginChild(long txnId, long parentTxnId) {
        return true;
    }

    @Override
    public boolean txnRollback(long txnId) throws IOException {
        processUndo(txnId, 0, false);
        return true;
    }

    @Override
    public boolean txnRollbackChild(long txnId, long parentTxnId) throws IOException {
        processUndo(txnId, parentTxnId, false);
        return true;
    }

    @Override
    public boolean txnCommit(long txnId) throws IOException {
        processUndo(txnId, 0, true);
        return true;
    }

    @Override
    public boolean txnCommitChild(long txnId, long parentTxnId) throws IOException {
        processUndo(txnId, parentTxnId, true);
        return true;
    }

    @Override
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        if (mScanner.isCommitted(txnId)) {
            Index ix = openIndex(indexId);
            if (ix != null) {
                ix.store(Transaction.BOGUS, key, value);
            }
        }
        return true;
    }

    @Override
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnStore(txnId, indexId, key, value);
        return txnCommit(txnId);
    }

    @Override
    public boolean txnStoreCommitChild(long txnId, long parentTxnId,
                                       long indexId, byte[] key, byte[] value)
        throws IOException
    {
        txnStore(txnId, indexId, key, value);
        return txnCommitChild(txnId, parentTxnId);
    }

    private void processUndo(long txnId, long parentTxnId, boolean commit) throws IOException {
        LHashTable.ObjEntry<UndoLog> entry;
        UndoLog log;
        if (mUndoLogs != null
            && (entry = mUndoLogs.get(txnId)) != null
            && (log = entry.value) != null
            && (commit
                ? log.truncateScope(txnId, parentTxnId)
                : log.rollbackScope(txnId, parentTxnId)))
        {
            mUndoLogs.remove(txnId);
            mUndoLogs.insert(log.activeTransactionId()).value = log;
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
