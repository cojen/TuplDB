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

package org.cojen.tupl.table.filter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.OrderBy;

/**
 * Describes a fully parsed query specification.
 *
 * @param projection can be null if projection is all columns
 * @param orderBy can be null if none; all orderBy columns must also be in the projection
 * @param filter never null
 * @see Parser#parseQuery
 */
public record QuerySpec(Map<String, ColumnInfo> projection, OrderBy orderBy, RowFilter filter) {
    public QuerySpec {
        if (orderBy != null && orderBy.isEmpty()) {
            orderBy = null;
        }
        Objects.requireNonNull(filter);
    }

    public QuerySpec withProjection(Map<String, ColumnInfo> proj) {
        return proj.equals(projection) ? this : new QuerySpec(proj, orderBy, filter);
    }

    public QuerySpec withOrderBy(OrderBy ob) {
        if (ob != null && ob.isEmpty()) {
            ob = null;
        }

        if (Objects.equals(orderBy, ob)) {
            return this;
        }

        if (ob == null || projection == null || projection.keySet().containsAll(ob.keySet())) {
            return new QuerySpec(projection, ob, filter);
        }

        // Expand the projection to include the additional orderBy columns.

        var proj = new LinkedHashMap<>(projection);

        for (OrderBy.Rule rule : ob.values()) {
            ColumnInfo column = rule.column();
            proj.putIfAbsent(column.name, column);
        }

        return new QuerySpec(proj, ob, filter);
    }

    public QuerySpec withFilter(RowFilter rf) {
        return rf.equals(filter) ? this : new QuerySpec(projection, orderBy, rf);
    }

    public QuerySpec reduce() {
        return withFilter(filter.reduce());
    }

    /**
     * Returns the column order by rule, or null if not specified.
     */
    public OrderBy.Rule orderByRule(String name) {
        return orderBy == null ? null : orderBy.get(name);
    }

    /**
     * Returns true if the effective query is "{*}".
     */
    public boolean isFullScan() {
        return projection == null && orderBy == null && filter == TrueFilter.THE;
    }

    /**
     * Returns a primary key specification consisting of all the projected columns, with the
     * proper orderBy.
     */
    public String[] primaryKey() {
        var map = new LinkedHashMap<String, String>();

        if (orderBy != null) {
            for (OrderBy.Rule rule : orderBy.values()) {
                map.computeIfAbsent(rule.column().name, _ -> rule.toString());
            }
        }

        if (projection != null) {
            for (String name : projection.keySet()) {
                map.putIfAbsent(name, name);
            }
        }

        return map.values().toArray(String[]::new);
    }

    @Override
    public String toString() {
        Set<String> names;

        if (projection != null) {
            names = projection.keySet();
        } else if (orderBy != null) {
            names = orderBy.keySet();
        } else if (filter != TrueFilter.THE) {
            return filter.toString();
        } else {
            return "{*}";
        }

        var b = new StringBuilder().append('{');

        if (projection == null) {
            b.append('*');
        }

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

        b.append('}');

        if (filter != TrueFilter.THE) {
            b.append(' ');
            filter.appendTo(b);
        }

        return b.toString();
    }
}
