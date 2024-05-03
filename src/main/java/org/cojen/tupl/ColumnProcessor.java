/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl;

/**
 * Represents an operation which accepts a column name and value.
 *
 * @author Brian S. O'Neill
 * @see Table#forEach
 */
@FunctionalInterface
public interface ColumnProcessor<R> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param row the row the column belongs to
     * @param name column name
     * @param value column value
     */
    void accept(R row, String name, Object value);
}
