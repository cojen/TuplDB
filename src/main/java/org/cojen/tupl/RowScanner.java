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

import java.io.Closeable;
import java.io.IOException;

import java.util.Spliterator;

/**
 * Support for scanning through all rows in a table. Any exception thrown when acting upon a
 * scanner automatically closes it.
 *
 * <p>RowScanner instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a RowScanner instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see Table#newRowScanner Table.newRowScanner
 * @see RowUpdater
 *
 * @author Brian S O'Neill
 */
public interface RowScanner<R> extends Spliterator<R>, Closeable {
    /**
     * Returns a reference to the current row, which is null if the scanner is closed.
     */
    R row();

    /**
     * Step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     */
    R step() throws IOException;

    /**
     * Step to the next row.
     *
     * @param row use this for the next row instead of creating a new one
     * @return the next row or null if no more rows remain and scanner has been closed
     * @throws NullPointerException if the given row object is null
     */
    R step(R row) throws IOException;

    @Override
    void close() throws IOException;
}
