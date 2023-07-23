/*
 *  Copyright (C) 2023 Cojen.org
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

import java.io.IOException;

import java.lang.invoke.MethodHandle;

import java.util.Objects;

/**
 * Interface for mapping source rows to target rows.
 *
 * @author Brian S O'Neill
 * @see Table#map Table.map
 */
@FunctionalInterface
public interface Mapper<R, T> {
    // FIXME: Define a method which returns the set of source columns that the map method
    // depends on, to help optimize query projection. By default, all columns are required.

    // FIXME: Define a method which indicates whether or not the map method performs any
    // filtering, is is true by default. When false, it can help query sorting. In particular,
    // partial sorting.

    /**
     * Maps source rows to target rows.
     *
     * @param source never null
     * @param target never null; all columns initially are unset
     * @return null if filtered out
     */
    T map(R source, T target) throws IOException;

    /**
     * Returns a source column inverse mapper for querying target rows.
     *
     * @return null if the source column cannot always be derived from the target column
     * @throws ReflectiveOperationException if obtaining a MethodHandle fails
     */
    default SourceColumn sourceColumn(String targetColumnName)
        throws ReflectiveOperationException
    {
        return null;
    }

    /**
     * @param name non-null source column name
     * @param mapper source column mapper, which can be null if an identity mapping suffices
     */
    public record SourceColumn(String name, MethodHandle mapper) {
        public SourceColumn {
            Objects.requireNonNull(name);
        }
    }
}
