/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * @throws IllegalArgumentException if any names are duplicated
     */
    public static TupleType make(List<ProjExpr> projection) {
        var types = new ArrayList<Object>(projection.size());

        for (ProjExpr pe : projection) {
            Type type = pe.type();
            if (pe.isNullable()) {
                type = type.nullable();
            }
            types.add(type.clazz());
        }

        RowTypeMaker.Result makerResult = RowTypeMaker.find(null, types);

        var projectionMap = new LinkedHashMap<String, String>(projection.size() * 2);

        Iterator<ProjExpr> it = projection.iterator();

        for (String fieldName : makerResult.allNames()) {
            projectionMap.put(fieldName, it.next().name());
        }

        assert !it.hasNext();

        return make(makerResult.rowType(), projectionMap);
    }

    /**
     * Makes a type which uses the given row type class.

     * @param projection maps row field column names to target names; can pass null to project
     * all columns without renaming them
     * @throws QueryException if projection refers to a non-existent column or if any
     * target column names are duplicated
     */
    public static TupleType make(Class rowType, Map<String, String> projection) {
        RowInfo info = RowInfo.find(rowType);

        Column[] columns;
        int ix = 0;

        if (projection == null) {
            Map<String, ColumnInfo> keys = info.keyColumns;
            Map<String, ColumnInfo> values = info.valueColumns;

            columns = new Column[keys.size() + values.size()];

            for (ColumnInfo ci : keys.values()) {
                String name = ci.name;
                columns[ix++] = Column.make(BasicType.make(ci), name, name);
            }
            for (ColumnInfo ci : values.values()) {
                String name = ci.name;
                columns[ix++] = Column.make(BasicType.make(ci), name, name);
            }
        } else {
            columns = new Column[projection.size()];

            for (String name : projection.keySet()) {
                ColumnInfo column = ColumnSet.findColumn(info.allColumns, name);
                if (column == null) {
                    throw new IllegalArgumentException("Unknown column: " + name);
                }
                columns[ix++] = Column.make(BasicType.make(column), name, column.name);
            }
        }

        if (ix != columns.length) {
            throw new AssertionError();
        }

        Map<String, Integer> columnMap = new HashMap<>(columns.length * 2);

        for (int i=0; i<columns.length; i++) {
            String name = columns[i].name();
            if (columnMap.putIfAbsent(name, i) != null) {
                throw new IllegalArgumentException("Duplicate column: " + name);
            }
        }

        return new TupleType(rowType, columns, columnMap);
    }

    private final Column[] mColumns;
    private final Map<String, Integer> mColumnMap;

    private TupleType(Class clazz, Column[] columns, Map<String, Integer> columnMap) {
        this(clazz, TYPE_REFERENCE, columns, columnMap);
    }

    private TupleType(Class clazz, int typeCode, Column[] columns, Map<String, Integer> columnMap) {
        super(clazz, typeCode);
        mColumns = columns;
        mColumnMap = columnMap;
    }

    @Override
    public TupleType nullable() {
        return isNullable() ? this
            : new TupleType(clazz(), TYPE_REFERENCE | TYPE_NULLABLE, mColumns, mColumnMap);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeUnsignedVarInt(mColumns.length);
            for (Column c : mColumns) {
                c.encodeKey(enc);
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = clazz().hashCode();
        hash = hash * 31 + Arrays.hashCode(mColumns);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof TupleType tt
            && clazz() == tt.clazz() && Arrays.equals(mColumns, tt.mColumns);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b) {
        b.append('{');

        int num = numColumns();
        for (int i=0; i<num; i++) {
            if (i > 0) {
                b.append(", ");
            }
            Column column = column(i);
            b.append(column.type()).append(' ');
            String name = column.name();
            String fieldName = column.fieldName();
            if (name.equals(fieldName)) {
                b.append(name);
            } else {
                RowUtils.appendQuotedString(b, name);
                b.append('=').append(fieldName);
            }
        }

        b.append('}');
    }

    public int numColumns() {
        return mColumns.length;
    }

    public Column column(int index) {
        return mColumns[index];
    }

    /**
     * Returns the column which has the given name.
     *
     * @throws IllegalArgumentException if not found
     */
    public Column columnFor(String name) {
        Column c = tryColumnFor(name);
        if (c == null) {
            throw new IllegalArgumentException("Unknown column: " + name);
        }
        return c;
    }

    /**
     * Returns the column which has the given name.
     *
     * @return null if not found
     */
    public Column tryColumnFor(String name) {
        Map<String, Integer> map = mColumnMap;
        Integer ix = map.get(name);
        return ix == null ? null : mColumns[ix];
    }

    /**
     * Find a column which matches the given name. The name of the returned column is fully
     * qualified, which means that for joins, it has dotted names.
     *
     * @param name qualified or unqualified column name to find
     * @return null if not found
     */
    public Column tryFindColumn(String name) {
        Map<String, Integer> map = mColumnMap;
        Integer ix = map.get(name);

        if (ix != null) {
            return mColumns[ix];
        }

        int dotIx = name.indexOf('.');

        if (dotIx >= 0 && (ix = map.get(name.substring(0, dotIx))) != null) {
            Column column = mColumns[ix];
            if (column.type() instanceof TupleType tt) {
                Column sub = tt.tryFindColumn(name.substring(dotIx + 1));
                if (sub != null) {
                    return sub.withName(name, column.fieldName() + '.' + sub.fieldName());
                }
            }
        }

        return null;
    }

    /**
     * Returns true if the given projection exactly matches the tuples columns, in the same
     * order. If null is passed in, this implies a full projection, and so true is returned in
     * this case too.
     */
    public boolean matches(Collection<ProjExpr> projection) {
        if (projection != null) {
            if (projection.size() != numColumns()) {
                return false;
            }
            int i = 0;
            for (ProjExpr pe : projection) {
                Expr e = pe.wrapped();
                if (!(e instanceof ColumnExpr ce) || !ce.column().equals(mColumns[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns true if the given projection exactly matches the fields of tuple's type class,
     * in the same order. If null is passed in, this implies a full projection, and so true is
     * returned in this case too.
     */
    public boolean matchesFields(Collection<String> projection) {
        if (projection != null) {
            if (projection.size() != numColumns()) {
                return false;
            }
            int i = 0;
            for (String name : projection) {
                if (!mColumns[i++].fieldName().equals(name)) {
                    return false;
                }
            }
        }
        return true;
    }
}
