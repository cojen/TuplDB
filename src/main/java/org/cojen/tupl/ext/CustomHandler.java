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
 * transactions, and redo operations are applied by recovery and replication. A companion
 * instance for writing custom operations is provided by the {@link Database#customWriter
 * Database.customWriter} method.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#customHandlers DatabaseConfig.customHandlers
 */
public interface CustomHandler extends Handler {
    /**
     * Called to write or apply an idempotent redo operation.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message custom message
     * @throws NullPointerException if transaction or message is null
     */
    void redo(Transaction txn, byte[] message) throws IOException;

    /**
     * Called to write or apply an idempotent redo operation which locked an index key. The
     * lock ensures that redo operations are ordered with respect to other transactions which
     * locked the same key.
     *
     * @param txn transaction the operation applies to; can be modified
     * @param message custom message
     * @param indexId non-zero index for lock acquisition
     * @param key non-null key which has been locked exclusively
     * @throws NullPointerException if transaction or message is null
     * @throws IllegalStateException if index and key are provided but lock isn't held
     * @throws IllegalArgumentException if index id is zero and key is non-null
     */
    void redo(Transaction txn, byte[] message, long indexId, byte[] key)
        throws IOException;

    /**
     * Called to write or apply an idempotent undo operation.
     *
     * @param txn transaction the operation applies to; is null when rolling back
     * @param message custom message
     * @throws NullPointerException if transaction or message is null; applicable to writer
     * instance only
     */
    void undo(Transaction txn, byte[] message) throws IOException;
}
