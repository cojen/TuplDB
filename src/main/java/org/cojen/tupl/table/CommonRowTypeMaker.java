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

package org.cojen.tupl.table;

import java.util.HashMap;
import java.util.Set;

import org.cojen.tupl.Row;
import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.expr.TupleType;

import org.cojen.tupl.table.filter.ColumnFilter;

/**
 * Makes a row type which serves the requirements of multiple row types, possibly applying
 * column conversions. If a common column type cannot be determined (unlikely), an
 * IllegalArgumentException is thrown. When possible, the generated row type has a suitable
 * primary key defined.
 *
 * @author Brian S. O'Neill
 * @see Table#concat
 */
public final class CommonRowTypeMaker {
    private static final WeakCache<Object, Class<Row>, Object> cCache = new WeakCache<>() {
        @Override
        public Class<Row> newValue(Object key, Object unused) {
            if (key instanceof TupleKey tk) {
                return doMakeFor((Class<?>[]) tk.get(0));
            } else {
                return doMakeFor((Class<?>) key);
            }
        }
    };

    public static Class<Row> makeFor(Table<?> table) {
        return makeFor(table.rowType());
    }

    public static Class<Row> makeFor(Table<?>... tables) {
        Class<?>[] rowTypes = new Class[tables.length];
        for (int i=0; i<rowTypes.length; i++) {
            rowTypes[i] = tables[i].rowType();
        }
        return makeFor(rowTypes);
    }

    public static Class<Row> makeFor(Class<?> rowType) {
        return cCache.obtain(rowType, null);
    }

    public static Class<Row> makeFor(Class<?>... rowTypes) {
        return cCache.obtain(TupleKey.make.with(rowTypes), null);
    }

    @SuppressWarnings("unchecked")
    private static Class<Row> doMakeFor(Class<?>... rowTypes) {
        if (rowTypes.length == 0) {
            return Row.class;
        }

        // If all rowTypes are the same, and it implements Row, just use that.
        sameCheck: {
            Class<?> rowType = rowTypes[0];
            if (!Row.class.isAssignableFrom(rowType)) {
                break sameCheck;
            }
            for (int i=1; i<rowTypes.length; i++) {
                if (rowTypes[i] != rowType) {
                    break sameCheck;
                }
            }
            return (Class<Row>) rowType;
        }

        var allColumns = new HashMap<String, ColumnInfo>();

        Set<String> pkColumns = null;
        boolean hasPk = true;

        for (int i=0; i<rowTypes.length; i++) {
            RowInfo info = RowInfo.find(rowTypes[i]);

            for (ColumnInfo column : info.allColumns.values()) {
                allColumns.merge(column.name, column, (ColumnInfo current, ColumnInfo mix) -> {
                    // Pass OP_EQ for lenient conversion, which might select a string type.
                    ColumnInfo common = ConvertUtils.commonType(current, mix, ColumnFilter.OP_EQ);
                    if (common == null) {
                        throw new IllegalArgumentException("Unsupported column type: " + mix.type);
                    }
                    if (common.name == null) {
                        common.name = current.name;
                    }
                    return common;
                });
            }

            if (hasPk) {
                if (pkColumns == null) {
                    pkColumns = info.keyColumns.keySet();
                } else if (!pkColumns.equals(info.keyColumns.keySet())) {
                    hasPk = false;
                }
            }
        }

        if (allColumns.isEmpty()) {
            return Row.class;
        }

        String[] pk = hasPk ? pkColumns.toArray(new String[pkColumns.size()]) : null;

        return (Class<Row>) TupleType.makeForColumns(allColumns.values(), pk).clazz();
    }
}
