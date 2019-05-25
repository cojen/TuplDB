/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.ext;

import java.io.IOException;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Transaction;

/**
 * Handler for custom transactional operations. Undo operations are applied to roll back
 * transactions, and redo operations are applied by recovery and replication.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#customTransactionHandler DatabaseConfig.customTransactionHandler
 */
public interface TransactionHandler {
    /**
     * Called once when the database is opened, immediately before recovery is performed.
     */
    void init(Database db) throws IOException;

    /**
     * Called to apply an idempotent redo operation.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message message originally provided to {@link Transaction#customRedo}
     */
    void redo(Transaction txn, byte[] message) throws IOException;

    /**
     * Called to apply an idempotent redo operation which locked an index key. The lock ensures
     * that redo operations are ordered with respect to other transactions which locked the
     * same key.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message message originally provided to {@link Transaction#customRedo}
     * @param indexId non-zero index for lock acquisition
     * @param key non-null key which has been locked exclusively
     */
    void redo(Transaction txn, byte[] message, long indexId, byte[] key)
        throws IOException;

    /**
     * Called to apply an idempotent undo operation.
     *
     * @param message message originally provided to {@link Transaction#customUndo}
     */
    void undo(byte[] message) throws IOException;
}
