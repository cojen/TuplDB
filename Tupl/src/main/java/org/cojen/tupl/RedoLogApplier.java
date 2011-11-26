/*
 *  Copyright 2011 Brian S O'Neill
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

    RedoLogApplier(Database db, RedoLogTxnScanner scanner) {
        mDb = db;
        mScanner = scanner;
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
        mDb.anyIndexById(indexId).store(Transaction.BOGUS, key, value);
    }

    @Override
    public void clear(long indexId) throws IOException {
        mDb.anyIndexById(indexId).clear(Transaction.BOGUS);
    }

    @Override
    public void txnRollback(long txnId, long parentTxnId) {}

    @Override
    public void txnCommit(long txnId, long parentTxnId) {}

    @Override
    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException {
        if (mScanner.isCommitted(txnId)) {
            mDb.anyIndexById(indexId).store(Transaction.BOGUS, key, value);
        }
    }
}
