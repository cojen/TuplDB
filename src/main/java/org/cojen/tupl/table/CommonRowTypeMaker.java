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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Iterator;

import org.cojen.tupl.Row;
import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.expr.TupleType;

import org.cojen.tupl.table.filter.ColumnFilter;

/**
 * Makes a row type which serves the requirements of multiple row types, possibly applying
 * column conversions. If a common column type cannot be determined (unlikely), an
 * IllegalArgumentException is thrown. The primary key of the generated row type references all
 * of the columns, to facilitate defining a union as concat plus a distinct step.
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

        // If all rowTypes are the same, and it implements Row, and it has a primary key
        // consisting of all columns, just use that.
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

            try {
                if (!RowInfo.find(rowType).valueColumns.isEmpty()) {
                    break sameCheck;
                }
            } catch (IllegalArgumentException e) {
                break sameCheck;
            }

            return (Class<Row>) rowType;
        }

        var allColumns = new HashMap<String, ColumnInfo>();

        // Used to determine what the common primary key should be.
        class PkScore implements Comparable<PkScore> {
            final RowInfo info;
            int[] matchCounts;

            PkScore(RowInfo info) {
                this.info = info;
            }

            /**
             * @return a negative number if this score is worse than the other one
             */
            @Override
            public int compareTo(PkScore other) {
                return Arrays.compare(matchCounts, other.matchCounts);
            }
        }

        var pkScores = new PkScore[rowTypes.length];

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

            pkScores[i] = new PkScore(info);
        }

        if (allColumns.isEmpty()) {
            return Row.class;
        }

        // Now determine what the best primary key should be. It should match as many source
        // row types as possible.

        for (PkScore score : pkScores) {
            score.matchCounts = new int[score.info.keyColumns.size()];

            for (PkScore other : pkScores) {
                if (other == score) {
                    continue;
                }

                int matchCount = 0;
                boolean flipped = false;

                Iterator<ColumnInfo> it1 = score.info.keyColumns.values().iterator();
                Iterator<ColumnInfo> it2 = other.info.keyColumns.values().iterator();

                while (it1.hasNext() && it2.hasNext()) {
                    ColumnInfo c1 = it1.next();
                    ColumnInfo c2 = it2.next();
                    if (!c1.name.equals(c2.name)) {
                        continue;
                    }
                    if ((c1.isDescending() == c2.isDescending()) ^ flipped) {
                        matchCount++;
                    } else if (!flipped) {
                        flipped = true;
                        matchCount++;
                    } else {
                        break;
                    }
                }

                if (matchCount > 0) {
                    score.matchCounts[score.matchCounts.length - matchCount]++;
                }
            }
        }

        PkScore best = pkScores[0];
        for (int i=1; i<pkScores.length; i++) {
            PkScore next = pkScores[i];
            if (best.compareTo(next) < 0) {
                best = next;
            }
        }

        // Fill in the primary key columns by first adding in those from the best score.

        var pkMap = new LinkedHashMap<String, ColumnInfo>(allColumns.size() << 1);

        for (ColumnInfo col : best.info.keyColumns.values()) {
            pkMap.putIfAbsent(col.name, col);
        }

        // Next, fold in all the key columns that haven't been added yet.

        for (PkScore score : pkScores) {
            for (ColumnInfo col : score.info.keyColumns.values()) {
                pkMap.putIfAbsent(col.name, col);
            }
        }

        // Finally, fold in all the remaining columns.

        for (PkScore score : pkScores) {
            for (ColumnInfo col : score.info.valueColumns.values()) {
                pkMap.putIfAbsent(col.name, col);
            }
        }

        var pk = new String[pkMap.size()];

        int i = 0;
        for (ColumnInfo col : pkMap.values()) {
            String name = col.name;
            if (col.isDescending()) {
                name = '-' + name;
            }
            pk[i++] = name;
        }

        return (Class<Row>) TupleType.makeForColumns(allColumns.values(), pk).clazz();
    }
}
