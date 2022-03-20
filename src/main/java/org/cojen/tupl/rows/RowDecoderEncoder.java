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
import org.cojen.tupl.LockResult;
import org.cojen.tupl.UnmodifiableViewException;

/**
 * See BasicRowScanner.
 *
 * @author Brian S O'Neill
 */
public interface RowDecoderEncoder<R> {
    /**
     * @param result LockResult from cursor access
     * @param row can pass null to construct a new instance
     * @return null if row is filtered out
     */
    R decodeRow(Cursor c, LockResult result, R row) throws IOException;

    /**
     * Decode variant used when updating via a secondary index. By positioning a cursor over
     * the primary table, it can be updated directly without the cost of an additional search.
     *
     * @param result LockResult from secondary cursor access
     * @param row can pass null to construct a new instance
     * @param primary cursor is positioned as a side effect
     * @return null if row is filtered out
     */
    default R decodeRow(Cursor secondary, LockResult result, R row, Cursor primary)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by BasicRowUpdater.
     *
     * @return null if the key columns didn't change
     */
    default byte[] updateKey(R row, byte[] original) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Called by BasicRowUpdater.
     *
     * @return non-null value
     */
    default byte[] updateValue(R row, byte[] original) throws IOException {
        throw new UnmodifiableViewException();
    }
}
