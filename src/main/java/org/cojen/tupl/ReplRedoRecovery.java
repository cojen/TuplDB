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
// FIXME: This class can probably just go away.
class ReplRedoRecovery implements RedoRecovery {
    private final ReplicationManager mReplManager;

    private ReplRedoReceiver mReceiver;

    ReplRedoRecovery(ReplicationManager manager) {
        mReplManager = manager;
    }

    @Override
    public boolean recover(Database db, DatabaseConfig config,
                           long position, long txnId,
                           LHashTable.Obj<Transaction> txns)
        throws IOException
    {
        // FIXME: position and txnId
        mReceiver = new ReplRedoReceiver(db, txns);
        // FIXME
        return false;
    }

    @Override
    public long highestTxnId() {
        // FIXME
        return 0;
    }

    @Override
    public RedoWriter newWriter() throws IOException {
        // FIXME: position
        return new ReplRedoLog(mReplManager, 0, mReceiver);
    }

    @Override
    public void cleanup() throws IOException {
        // Nothing to do.
    }
}
