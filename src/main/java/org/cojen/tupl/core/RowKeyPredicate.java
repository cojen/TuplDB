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

/**
 * Defines an interface intended for locking rows based on a rule that matches the key columns
 * of a row.
 *
 * @author Brian S O'Neill
 * @see RowKeyLockSet
 */
public interface RowKeyPredicate<R> {
    /**
     * Called by an insert or delete operation. Although the row will be full if called by an
     * insert, the delete will only fill in the key columns.
     */
    public boolean testRow(R row);

    /**
     * Determine if a lock held against the given key matches the row predicate. This variant
     * is called for transactions which were created before the predicate lock.
     */
    public boolean testKey(byte[] key);
}
