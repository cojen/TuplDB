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

package org.cojen.tupl.rows;

import java.util.Map;
import java.util.NavigableMap;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ColumnSet {
    // Map is ordered lexicographically by name.
    NavigableMap<String, ColumnInfo> allColumns;

    // Map is ordered lexicographically by name and excludes key columns.
    NavigableMap<String, ColumnInfo> valueColumns;

    // Map order matches declaration order and excludes value columns.
    Map<String, ColumnInfo> keyColumns;

    final boolean matches(ColumnSet other) {
        return allColumns.equals(other.allColumns) &&
            valueColumns.equals(other.valueColumns) && keyColumns.equals(other.keyColumns);
    }

    /**
     * Returns a descriptor consisting of the key columns.
     */
    String keyDescriptor() {
        var b = new StringBuilder();
        for (ColumnInfo ci : keyColumns.values()) {
            b.append(ci.isDescending() ? '-' : '+').append(ci.name);
        }
        return b.toString();
    }

    @Override
    public String toString() {
        return keyColumns.values() + " -> " + valueColumns.values() +
            ", allColumns: " + allColumns.values();
    }
}
