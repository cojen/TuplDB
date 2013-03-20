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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ReplRedoRecovery implements RedoRecovery {
    private final ReplicationManager mReplManager;

    private ReplRedoEngine mEngine;

    ReplRedoRecovery(ReplicationManager manager) {
        mReplManager = manager;
    }

    /**
     * @param txns cleared as a side effect
     */
    @Override
    public boolean recover(Database db, DatabaseConfig config,
                           long position, long txnId,
                           LHashTable.Obj<Transaction> txns)
        throws IOException
    {
        mReplManager.start(position);
        mEngine = new ReplRedoEngine(mReplManager, db, txns);
        mEngine.startReceiving(txnId);
        // FIXME: Wait until caught up?
        return false;
    }

    @Override
    public long highestTxnId() {
        // FIXME
        return 0;
    }

    @Override
    public RedoWriter newWriter() throws IOException {
        return mEngine.getWriter();
    }

    @Override
    public void cleanup() throws IOException {
        // Nothing to do.
    }
}
