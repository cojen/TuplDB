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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;


/**
 * Generates secondary indexes and alternate keys.
 *
 * @author Brian S O'Neill
 */
class IndexManager<R> {
    /**
     * Update the set of indexes, based on what is found in the given View. If nothing changed,
     * null is returned. Otherwise, a new Trigger is returned which replaces the existing
     * primary table trigger. Caller is expected to hold a lock which prevents concurrent calls
     * to this method, which isn't thread-safe.
     *
     * @param rs used to open tables for indexes
     * @param txn holds the lock
     * @param secondaries maps index descriptor to index id and state
     * @param table owns the secondaries
     */
    Trigger<R> update(RowStore rs, Transaction txn, View secondaries, AbstractTable<R> table)
        throws IOException
    {
        // FIXME: First compare the secondaries to what's known. Return null if no change.
        return null;
    }
}
