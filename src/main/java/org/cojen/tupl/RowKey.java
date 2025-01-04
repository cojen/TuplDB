/*
 *  Copyright (C) 2025 Cojen.org
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

import java.util.function.BiConsumer;

import org.cojen.tupl.table.BasicRowKey;

/**
 * Describes a key as sequence of columns.
 *
 * @author Brian S. O'Neill
 */
public interface RowKey {
    /**
     * Returns the number of columns in the key, which can be zero.
     */
    public int size();

    /**
     * Returns the column name at the given position.
     *
     * @param pos zero-based position
     * @throws IndexOutOfBoundsException
     */
    public String column(int pos);

    /**
     * Returns the column ordering at the given position.
     *
     * @param pos zero-based position
     * @throws IndexOutOfBoundsException
     */
    public Ordering ordering(int i);

    /**
     * Performs the given action for each column.
     */
    public default void forEach(BiConsumer<String, Ordering> action) {
        int size = size();
        for (int i=0; i<size; i++) {
            action.accept(column(i), ordering(i));
        }
    }

    /**
     * Returns a non-null string of the form: {@code (('+' | '-' | '~') ['!'] name) ...}
     */
    public default String spec() {
        int size = size();
        if (size == 0) {
            return "";
        }
        var b = new StringBuilder(size * 10);
        appendSpec(b);
        return b.toString();
    }

    /**
     * Appends a string of the form: {@code (('+' | '-' | '~') ['!'] name) ...}
     */
    public default void appendSpec(StringBuilder b) {
        BasicRowKey.appendSpec(this, b);
    }

    /**
     * Parse a key from a {@link #spec specification}.
     *
     * @throws IllegalArgumentException
     */
    public static RowKey parse(String spec) {
        return BasicRowKey.parse(spec, false);
    }
}
