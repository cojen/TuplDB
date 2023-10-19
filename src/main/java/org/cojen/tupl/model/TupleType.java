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

package org.cojen.tupl.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.RowTypeMaker;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class TupleType extends Type {
    /**
     * @param columns columns must have names, but they don't need to be valid identifiers
     */
    public static TupleType make(Column... columns) {
        RowTypeMaker.Result makerResult;
        if (columns == null || columns.length == 0) {
            makerResult = RowTypeMaker.find(null, null);
        } else {
            var keyTypes = new ArrayList<Class>();
            var valueTypes = new ArrayList<Class>();
            for (Column column : columns) {
                (column.key() ? keyTypes : valueTypes).add(column.type().clazz());
            }
            makerResult = RowTypeMaker.find(keyTypes, valueTypes);
        }

        return new TupleType(makerResult.rowType(), columns, makerResult.allNames());
    }

    /**
     * @param projection maps column names to target names; can pass null to project all columns
     * @throws IllegalArgumentException if projection refers to a non-existent column
     */
    public static TupleType make(Class rowType, Map<String, String> projection) {
        RowInfo info = RowInfo.find(rowType);

        Map<String, ColumnInfo> keys, values;

        if (projection == null) {
            keys = info.keyColumns;
            values = info.valueColumns;
        } else {
            keys = new LinkedHashMap<>();
            values = new LinkedHashMap<>();

            for (String colName : projection.keySet()) {
                ColumnInfo column = info.keyColumns.get(colName);
                if (column != null) {
                    keys.put(colName, column);
                } else if ((column = info.valueColumns.get(colName)) != null) {
                    values.put(colName, column);
                } else {
                    throw new IllegalArgumentException("Unknown column: " + colName);
                }
            }
        }

        int numColumns = keys.size() + values.size();

        var columns = new Column[numColumns];
        var fields = new String[numColumns];

        int ix = fill(columns, fields, 0, true, keys, projection);
        ix = fill(columns, fields, ix, false, values, projection);

        if (ix != numColumns) {
            throw new AssertionError();
        }

        return new TupleType(rowType, columns, fields);
    }

    /**
     * @return updated ix
     */
    private static int fill(Column[] columns, String[] fields, int ix, boolean key,
                            Map<String, ColumnInfo> columnMap, Map<String, String> projection)
    {
        for (ColumnInfo info : columnMap.values()) {
            String name = info.name;
            fields[ix] = name;
            if (projection != null) {
                name = projection.get(name);
            }
            columns[ix] = new Column(BasicType.make(info.type, info.typeCode), name, key);
            ix++;
        }

        return ix;
    }

    private final Column[] mColumns;
    private final String[] mFields;

    private Map<String, Integer> mColumnMap;
    private Map<String, Integer> mCaseInsensitiveMap;

    private TupleType(Class clazz, Column[] columns, String[] fields) {
        super(clazz, ColumnInfo.TYPE_REFERENCE);
        mColumns = columns;
        mFields = fields;
    }

    @Override
    public int hashCode() {
        int hash = mClazz.hashCode();
        hash = hash * 31 + Arrays.hashCode(mColumns);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TupleType tt
            && mClazz == tt.mClazz && Arrays.equals(mColumns, tt.mColumns);
    }

    @Override
    public String toString() {
        var bob = new StringBuilder();
        bob.append('{');

        int num = numColumns();
        for (int i=0; i<num; i++) {
            if (i > 0) {
                bob.append(", ");
            }
            Column column = column(i);
            bob.append(column.type()).append(' ');
            String field = field(i);
            bob.append(field);
            String name = column.name();
            if (!field.equals(name)) {
                bob.append(" as \"").append(column.name()).append('"');
            }
        }

        return bob.append('}').toString();
    }

    public int numColumns() {
        return mColumns.length;
    }

    public Column column(int index) {
        return mColumns[index];
    }

    /**
     * Returns a field in the tuple's type class.
     */
    public String field(int index) {
        return mFields[index];
    }

    /**
     * Find a column which matches the given name. The name of the returned column is a fully
     * qualified field, which means that for joins, it's a dotted name.
     *
     * @param name qualified or unqualified column name to find
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or ambiguous
     */
    public Column findColumn(String name, boolean caseInsensitive) {
        Map<String, Integer> map;
        if (!caseInsensitive) {
            map = mColumnMap;
        } else {
            map = mCaseInsensitiveMap;
        }

        if (map == null) {
            map = buildColumnMap(caseInsensitive);
            if (!caseInsensitive) {
                mColumnMap = map;
            } else {
                mCaseInsensitiveMap = map;
            }
        }

        Integer ix = map.get(name);

        if (ix != null) {
            if (ix < 0) {
                throw new IllegalArgumentException("Column name is ambiguous: " + name);
            }
            return column(ix).withName(field(ix));
        }

        // FIXME: If name is dotted, follow the dots.

        // FIXME: If column type is an interface, search wildcards paths: *.a, *.*.a, etc.
        // Might be ambiguous however. Can call RowInfo.find with the interface to see if
        // column is a true join column, although it might throw an exception.

        throw null;
    }

    private Map<String, Integer> buildColumnMap(boolean caseInsensitive) {
        Map<String, Integer> map = caseInsensitive
            ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>();

        for (int i=0; i<mColumns.length; i++) {
            Column column = mColumns[i];
            if (map.put(column.name(), i) != null) {
                // Duplicate, and so finding it is ambiguous.
                map.put(column.name(), -1);
            }
        }

        return map;
    }
}
