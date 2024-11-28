/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

/**
 * A listener of incoming replication operations
 *
 * @author Brian S O'Neill
 * @see LocalDatabase#addRedoListener
 */
public interface RedoListener {
    /**
     * Is invoked after a store operation has been applied, but before the transaction has
     * committed. Any exception thrown by this method is logged as an uncaught exception.
     *
     * @param txn transaction that includes the store operation, using NO_REDO mode
     * @param ix the index that was stored into
     * @param key the stored key
     * @param value the stored value, which is null for a delete
     */
    void store(Transaction txn, Index ix, byte[] key, byte[] value);
}
