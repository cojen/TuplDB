/*
 *  Copyright 2015 Brian S O'Neill
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

package org.cojen.tupl.ext;

import java.io.IOException;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Transaction;

/**
 * Handler for custom transactional redo operations, which are applied by recovery and
 * replication.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#customRedoHandler
 */
public interface RedoHandler {
    /**
     * Non-transactionally apply an idempotent redo operation.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message message originally provided to {@link Transaction#customRedo}
     */
    void redo(Database db, Transaction txn, byte[] message)
        throws IOException;

    /**
     * Non-transactionally apply an idempotent redo operation. Locking an index key allows
     * replication to operate concurrently.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message message originally provided to {@link Transaction#customRedo}
     * @param indexId non-zero index for lock acquisition
     * @param key non-null key which has been locked exclusively
     */
    void redo(Database db, Transaction txn, byte[] message, long indexId, byte[] key)
        throws IOException;
}
