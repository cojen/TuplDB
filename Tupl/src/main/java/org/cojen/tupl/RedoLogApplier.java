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

    RedoLogApplier(Database db) {
        mDb = db;
    }

    public void timestamp(long timestamp) {
    }

    public void shutdown(long timestamp) {
    }

    public void close(long timestamp) {
    }

    public void endFile(long timestamp) {
    }

    public void store(long indexId, byte[] key, byte[] value) throws IOException {
        mDb.anyIndexById(indexId).store(Transaction.BOGUS, key, value);
    }

    public void clear(long indexId) throws IOException {
        mDb.anyIndexById(indexId).clear(Transaction.BOGUS);
    }

    public void txnRollback(long txnId, long parentTxnId) throws IOException {
        // FIXME
        throw null;
    }

    public void txnCommit(long txnId, long parentTxnId) throws IOException {
        // FIXME
        throw null;
    }

    public void txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }
}
