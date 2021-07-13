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

import java.io.Flushable;
import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RowUpdater<R> extends RowScanner<R>, Flushable {
    /**
     * Update the current row and then step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     */
    R update() throws IOException;

    /**
     * Update the current row and then step to the next row.
     *
     * @param row use this for the next row instead of creating a new one
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws NullPointerException if the given row object is null
     * @throws IllegalStateException if no current row
     */
    R update(R row) throws IOException;

    /**
     * Delete the current row and then step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws IllegalStateException if no current row
     */
    R delete() throws IOException;

    /**
     * Delete the current row and then step to the next row.
     *
     * @param row use this for the next row instead of creating a new one
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws NullPointerException if the given row object is null
     * @throws IllegalStateException if no current row
     */
    R delete(R row) throws IOException;

    /**
     * Ensures that any queued update operations are applied; flushing is automatically
     * performed when the updater is closed.
     */
    @Override
    default void flush() throws IOException {
    }
}
