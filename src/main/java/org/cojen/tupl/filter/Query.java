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
import java.util.Set;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.OrderBy;

/**
 * Describes a fully parsed query specification.
 *
 * @param projection null if projection is all columns
 * @param orderBy can be null
 * @param filter never null
 * @see Parser#parseQuery
 */
public record Query(Map<String, ColumnInfo> projection, OrderBy orderBy, RowFilter filter) {
    public Query reduce() {
        RowFilter rf = filter.reduce();
        return rf.equals(filter) ? this : new Query(projection, orderBy, rf);
    }

    @Override
    public String toString() {
        Set<String> names;

        if (projection != null) {
            names = projection.keySet();
        } else if (orderBy != null) {
            names = orderBy.keySet();
        } else {
            return filter.toString();
        }

        var b = new StringBuilder().append('{');

        for (String name : names) {
            if (b.length() != 1) {
                b.append(',').append(' ');
            }
            OrderBy.Rule rule;
            if (orderBy != null && (rule = orderBy.get(name)) != null) {
                rule.appendTo(b);
            } else {
                b.append(name);
            }
        }

        if (projection == null) {
            b.append(", *");
        }

        b.append('}');

        if (filter != TrueFilter.THE) {
            b.append(':').append(' ');
            filter.appendTo(b);
        }

        return b.toString();
    }
}
