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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ColumnSet;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowTypeMaker;
import org.cojen.tupl.table.RowUtils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class TupleType extends Type {
    /**
     * Makes a type which has a generated row type class.
     *
     * @param columns columns must have names, but they don't need to be valid identifiers
     */
    public static TupleType make(Column... columns) {
        RowTypeMaker.Result makerResult;
        if (columns == null || columns.length == 0) {
            makerResult = RowTypeMaker.find(null, null);
        } else {
            var keyTypes = new ArrayList<Object>();
            var valueTypes = new ArrayList<Object>();
            for (Column column : columns) {
                Type type = column.type();
                Object typeObj = RowTypeMaker.columnType(type.clazz(), type.typeCode());
                (column.key() ? keyTypes : valueTypes).add(typeObj);
            }
            makerResult = RowTypeMaker.find(keyTypes, valueTypes);
        }

        return new TupleType(makerResult.rowType(), columns, makerResult.allNames());
    }

    /**
     * Makes a type which has a generated row type class.
     */
    public static TupleType make(Node[] projection) {
        var columns = new Column[projection.length];

        for (int i=0; i<projection.length; i++) {
            Node node = projection[i];
            Type type = node.type();
            if (node.isNullable()) {
                type = type.nullable();
            }
            // FIXME: Try to infer if the column is a key or not. If full primary key is
            // composite, then all of its columns must be projected in order for any columns to
            // have key=true.
            columns[i] = Column.make(type, node.name(), false);
        }

        return make(columns);
    }

    /**
     * Makes a type which uses the given row type class.
     *
     * @param projection maps column names to target names; can pass null to project all columns
     * @throws IllegalArgumentException if projection refers to a non-existent column
     */
    public static TupleType make(Class rowType, Map<String, String> projection) {
        RowInfo info = RowInfo.find(rowType);

        Column[] columns;
        String[] fields;

        if (projection == null) {
            Map<String, ColumnInfo> keys = info.keyColumns;
            Map<String, ColumnInfo> values = info.valueColumns;

            int numColumns = keys.size() + values.size();

            columns = new Column[numColumns];
            fields = new String[numColumns];

            int ix = fill(columns, fields, 0, true, keys, projection);
            ix = fill(columns, fields, ix, false, values, projection);

            if (ix != numColumns) {
                throw new AssertionError();
            }
        } else {
            columns = new Column[projection.size()];
            fields = new String[columns.length];

            int ix = 0;
            for (String colName : projection.keySet()) {
                ColumnInfo column = ColumnSet.findColumn(info.valueColumns, colName);
                boolean key = false;
                if (column == null) {
                    column = ColumnSet.findColumn(info.keyColumns, colName);
                    if (column == null) {
                        throw new IllegalArgumentException("Unknown column: " + colName);
                    }
                    key = true;
                }
                columns[ix] = Column.make(BasicType.make(column), colName, key);
                fields[ix] = column.name;
                ix++;
            }
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
            columns[ix] = Column.make(BasicType.make(info), name, key);
            ix++;
        }

        return ix;
    }

    private final Column[] mColumns;
    private final String[] mFields;

    private Map<String, Column> mFieldToColumnMap;

    private Map<String, Integer> mColumnMap;
    private Map<String, Integer> mCaseInsensitiveMap;

    private TupleType(Class clazz, Column[] columns, String[] fields) {
        this(clazz, TYPE_REFERENCE, columns, fields);
    }

    private TupleType(Class clazz, int typeCode, Column[] columns, String[] fields) {
        super(clazz, typeCode);
        mColumns = columns;
        mFields = fields;
    }

    @Override
    public TupleType nullable() {
        return isNullable() ? this
            : new TupleType(clazz(), TYPE_REFERENCE | TYPE_NULLABLE, mColumns, mFields);
    }

    @Override
    public int hashCode() {
        int hash = clazz().hashCode();
        hash = hash * 31 + Arrays.hashCode(mColumns);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TupleType tt
            && clazz() == tt.clazz() && Arrays.equals(mColumns, tt.mColumns);
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
                bob.append(" as ");
                RowUtils.appendQuotedString(bob, name);
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
     * Returns true if the given projection exactly matches the fields of tuple's type class.
     * If given null, this implies a full projection, and so true is returned.
     */
    public boolean matches(Node[] projection) {
        if (projection != null) {
            if (projection.length != numColumns()) {
                return false;
            }
            for (int i=0; i<projection.length; i++) {
                Node n = projection[i];
                if (!(n instanceof ColumnNode cn) || !cn.column().name().equals(mFields[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns true if the given projection exactly matches the fields of tuple's type class.
     * If given null, this implies a full projection, and so true is returned.
     */
    public boolean matches(Collection<String> projection) {
        if (projection != null) {
            if (projection.size() != numColumns()) {
                return false;
            }
            int i = 0;
            for (String name : projection) {
                if (!mFields[i++].equals(name)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return null if not found
     */
    public Column fieldColumn(String name) {
        Map<String, Column> map = mFieldToColumnMap;
        if (map == null) {
            map = new HashMap<>();
            for (int i=0; i<mFields.length; i++) {
                map.put(mFields[i], mColumns[i]);
            }
            mFieldToColumnMap = map;
        }
        return map.get(name);
    }

    /**
     * Find a column which matches the given name. The name of the returned column is a fully
     * qualified field, which means that for joins, it's a dotted name.
     *
     * @param name qualified or unqualified column name to find
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or is ambiguous
     */
    public Column findColumn(final String name, final boolean caseInsensitive) {
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
                throw new IllegalArgumentException("Column is ambiguous: " + name);
            }
            return column(ix).withName(field(ix));
        }

        int dotIx = name.indexOf('.');

        if (dotIx >= 0) {
            String head = name.substring(0, dotIx);
            ix = map.get(head);

            if (ix != null) {
                if (ix < 0) {
                    throw new IllegalArgumentException("Column is ambiguous: " + name);
                }
                Column column = column(ix);
                if (column.type() instanceof TupleType tt) {
                    column = tt.findColumn(name.substring(dotIx + 1), caseInsensitive);
                    return column.withName(field(ix) + '.' + column.name());
                }
                // FIXME: What other column types are possible?
            }
        }

        // FIXME: If column type is an interface, search wildcards paths: *.a, *.*.a, etc.
        // Might be ambiguous however. Can call RowInfo.find with the interface to see if
        // column is a true join column, although it might throw an exception.

        throw new IllegalArgumentException("Column isn't found: " + name);
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

    /**
     * @return a map of field names to column names
     */
    public Map<String, String> makeProjectionMap() {
        return makeProjectionMap(numColumns());
    }

    /**
     * @param max must not be more than numColumns
     * @return a map of field names to column names
     */
    public Map<String, String> makeProjectionMap(int max) {
        var projectionMap = new LinkedHashMap<String, String>();
        for (int i=0; i<max; i++) {
            projectionMap.put(field(i), column(i).name());
        }
        return projectionMap;
    }
}
