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
 * Single-use recovery workflow and RedoWriter factory.
 *
 * @author Brian S O'Neill
 */
// FIXME: Interface can go away
interface RedoRecovery {
    /**
     * Perform main recovery workflow. As new transactions are recovered, they
     * must be added to the given hashtable. When transactions complete, they
     * must be removed from the hashtable.
     *
     * @param position position to start recovery from
     * @param txnId first transaction id at recovery position
     * @param txns all active transactions during last checkpoint; all
     * committed and (explicitly) rolled back entries must be removed
     * @return true if anything was recovered
     */
    boolean recover(Database db, DatabaseConfig config,
                    long position, long txnId,
                    LHashTable.Obj<Transaction> txns)
        throws IOException;

    /**
     * Called after recovery, to obtain the highest recovered transaction
     * id. Only valid if anything was recovered.
     */
    long highestTxnId();

    /**
     * Called after recovery, to obtain a writer for new transactions.
     */
    RedoWriter newWriter() throws IOException;

    /**
     * Called after recovery checkpoint, to perform additional cleanup. Only
     * called if anything was recovered and checkpointed.
     */
    void cleanup() throws IOException;
}
