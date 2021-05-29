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

/**
 * See BasicRowScanner.
 *
 * @author Brian S O'Neill
 */
public interface RowDecoderEncoder<R> {
    /**
     * @param row can pass null to construct a new instance
     * @return null if row is filtered out
     */
    // Note: This variant exists to support a potential optimization in which the cursor value
    // isn't autoloaded. A filter might not need to eagerly load the cursor value.
    default R decodeRow(byte[] key, Cursor c, R row) throws IOException {
        return decodeRow(key, c.value(), row);
    }

    /**
     * @param row can pass null to construct a new instance
     * @return null if row is filtered out
     */
    R decodeRow(byte[] key, byte[] value, R row) throws IOException;

    /**
     * @return null if the key columns didn't change
     */
    byte[] encodeKey(R row);

    /**
     * @return non-null value
     */
    byte[] encodeValue(R row);
}
