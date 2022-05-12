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

package org.cojen.tupl.rows;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @see ComparatorMaker
 * @author Brian S O'Neill
 */
final class OrderBy extends LinkedHashMap<String, OrderBy.Rule> {
    static OrderBy forPrimaryKey(RowInfo rowInfo) {
        var orderBy = new OrderBy();
        for (ColumnInfo column : rowInfo.keyColumns.values()) {
            orderBy.put(column.name, new Rule(column, column.isDescending() ? 1 : 0));
        }
        return orderBy;
    }

    static OrderBy forSpec(RowInfo rowInfo, String spec) {
        try {
            OrderBy orderBy = parseSpec(rowInfo.allColumns, spec);
            if (!orderBy.isEmpty()) {
                return orderBy;
            }
        } catch (IndexOutOfBoundsException e) {
        }

        throw new IllegalArgumentException("Malformed ordering specification: " + spec);
    }

    String spec() {
        var b = new StringBuilder();
        for (OrderBy.Rule rule : values()) {
            rule.appendTo(b);
        }
        return b.toString();
    }

    // flags: bit 0: descending,  bit 1: null low
    static record Rule(ColumnInfo column, int flags) {
        boolean isDescending() {
            return (flags & 1) != 0;
        }

        boolean isNullLow() {
            return (flags & 2) != 0;
        }

        void appendTo(StringBuilder b) {
            b.append(isDescending() ? '-' : '+');
            if (isNullLow()) {
                b.append('!');
            }
            b.append(column.name);
        }
    }

    private static OrderBy parseSpec(Map<String, ColumnInfo> columns, String spec) {
        var orderBy = new OrderBy();

        int length = spec.length();
        for (int pos = 0; pos < length; ) {
            int flags = 0;

            int order = spec.charAt(pos++);
            if (order == '-') {
                flags |= 1;
            } else if (order != '+') {
                break;
            }

            if (spec.charAt(pos) == '!') {
                flags |= 2;
                pos++;
            }

            int end = pos;
            while (end < length) {
                order = spec.charAt(end);
                if (order == '-' || order == '+') {
                    break;
                }
                end++;
            }

            if (end == pos) {
                break;
            }

            String name = spec.substring(pos, end);
            ColumnInfo column = columns.get(name);
            if (column == null) {
                throw new IllegalStateException
                    ("Unknown column \"" + name + "\" in ordering specification: " + spec);
            }

            pos = end;

            if (column.isPrimitive()) {
                // Can't be null.
                flags &= ~2;
            }

            if (!orderBy.containsKey(name)) {
                orderBy.put(name, new Rule(column, flags));
            }
        }

        return orderBy;
    }
}
