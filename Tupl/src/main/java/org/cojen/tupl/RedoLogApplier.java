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
 */
class RedoLogApplier implements RedoLogVisitor {
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
    public void timestamp(long timestamp) {}

    @Override
    public void shutdown(long timestamp) {}

    @Override
    public void close(long timestamp) {}

    @Override
    public void endFile(long timestamp) {}

    @Override
    public void store(long indexId, byte[] key, byte[] value) throws IOException {
        Index ix = openIndex(indexId);
        if (ix != null) {
            ix.store(Transaction.BOGUS, key, value);
        }
    }

    @Override
    public void txnRollback(long txnId, long parentTxnId) throws IOException {
        processUndo(txnId, parentTxnId, false);
    }

    @Override
    public void txnCommit(long txnId, long parentTxnId) throws IOException {
        processUndo(txnId, parentTxnId, true);
    }

    @Override
    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException {
        if (mScanner.isCommitted(txnId)) {
            Index ix = openIndex(indexId);
            if (ix != null) {
                ix.store(Transaction.BOGUS, key, value);
            }
        }
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
