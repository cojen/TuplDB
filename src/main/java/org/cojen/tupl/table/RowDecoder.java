/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Entry;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RowDecoder<R> {
    /**
     * Decodes a key and value into a row and marks all projected columns clean.
     *
     * @param row can pass null to construct a new instance
     * @return non-null row
     */
    R decodeRow(R row, byte[] key, byte[] value) throws IOException;

    default R decodeRow(R row, Entry e) throws IOException {
        return decodeRow(row, e.key(), e.value());
    }
}
