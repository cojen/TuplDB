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

package org.cojen.tupl.rows;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.CommitLock;

/**
 * Defines the single main trigger that a table can have. A trigger implementation is expected
 * to have several sub-tasks, and it might also provide a way to quickly add and remove
 * sub-tasks. Adding and removing sub-tasks can have race conditions, and so a copy-on-write
 * approach which replaces the trigger all at once might be safer.
 *
 * @author Brian S O'Neill
 * @see AbstractTable#setTrigger
 */
public class Trigger<R> extends CommitLock {
    public static final int ACTIVE = 0, SKIP = 1, DISABLED = 2;

    // Set by AbstractTable.
    int mMode;

    /**
     * Called after a row has been stored, but before the row has been marked clean. By
     * default, this method always throws an exception.
     *
     * @param txn never null, although can be BOGUS
     * @param row never null
     * @param key never null
     * @param oldValue might be null (is always null for insert)
     * @param newValue might be null (is always null for delete)
     */
    public void store(Transaction txn, R row, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called after a row has been updated, but before the row has been marked undirty. This
     * variant is only called from the "update" method when the row is partially dirtied. The
     * oldValue and newValue are fully specified, but the row object remains partial. When the
     * "merge" method is called (or any other), or when all columns are dirty, the regular
     * trigger store method is called with a fully populated row. By default, this method
     * always throws an exception.
     *
     * @param txn never null, although can be BOGUS
     * @param row never null
     * @param key never null
     * @param oldValue might be null (is always null for insert)
     * @param newValue might be null (is always null for delete)
     */
    public void update(Transaction txn, R row, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * If active, then must call the store or update method. If skip, then don't call a trigger
     * method, but still hold the shared lock for the whole operation. If disabled, then
     * release the shared lock and retry with the latest trigger instance.
     */
    public int mode() {
        return mMode;
    }
}
