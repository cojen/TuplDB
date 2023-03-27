/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;

/**
 * Support for scanning through all rows in a table, updating them along the way. Any exception
 * thrown when acting upon an updater automatically closes it.
 *
 * <p>Update operations only affect columns which have a modified state, and the rest of the
 * columns remain the same as what was served by the updater. Columns which have an unmodified
 * state, regardless of the column value, aren't updated. Consider the case in which a row key
 * is changed, the row is loaded, and then one column is modified. Although all columns are
 * effectively different than what was served by the updater, only one column is updated
 * against the original row. For debugging, call the row's {@code toString} method to identify
 * which columns are modified. The name of a modified column is prefixed with an asterisk.
 *
 * <p>Updater instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a Updater instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see Table#newUpdater Table.newUpdater
 * @see Scanner
 *
 * @author Brian S O'Neill
 */
public interface Updater<R> extends Scanner<R> {
    /**
     * Update the current row and then step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     * @throws UniqueConstraintException if update creates a conflicting primary or alternate key
     */
    default R update() throws IOException {
        return update(null);
    }

    /**
     * Update the current row and then step to the next row.
     *
     * @param row use this for the next row instead of creating a new one; if null is passed
     * in, a new instance will be created if necessary
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     * @throws UniqueConstraintException if update creates a conflicting primary or alternate key
     */
    R update(R row) throws IOException;

    /**
     * Delete the current row and then step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     */
    default R delete() throws IOException {
        return delete(null);
    }

    /**
     * Delete the current row and then step to the next row.
     *
     * @param row use this for the next row instead of creating a new one; if null is passed
     * in, a new instance will be created if necessary
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     */
    R delete(R row) throws IOException;
}
