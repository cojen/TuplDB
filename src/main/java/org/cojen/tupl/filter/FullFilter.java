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

package org.cojen.tupl.filter;

import java.util.Map;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * Describes a fully parsed filter specification.
 *
 * @param projection null if projection is all columns
 * @param filter never null
 * @see Parser#parseFull
 */
public record FullFilter(Map<String, ColumnInfo> projection, RowFilter filter) {
    public FullFilter reduce() {
        RowFilter rf = filter.reduce();
        return rf.equals(filter) ? this : new FullFilter(projection, rf);
    }

    @Override
    public String toString() {
        if (projection == null) {
            return filter.toString();
        }

        var b = new StringBuilder();

        b.append('{');
        for (String name : projection.keySet()) {
            if (b.length() != 1) {
                b.append(',').append(' ');
            }
            b.append(name);
        }
        b.append('}');

        if (filter != TrueFilter.THE) {
            b.append(':').append(' ');
            filter.appendTo(b);
        }

        return b.toString();
    }    
}
