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

import java.util.regex.Pattern;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ColumnSet;
import org.cojen.tupl.table.IdentityTable;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.Unpersisted;
import org.cojen.tupl.table.WeakCache;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class TupleType extends Type {
    /**
     * Makes a type which has a generated row type class. Columns aren't defined for excluded
     * projections.
     *
     * @throws IllegalArgumentException if any names are duplicated
     */
    public static TupleType make(List<ProjExpr> projection) {
        // Matches characters which cannot appear in field and method names.
        final Pattern p = Pattern.compile("\\.|\\;|\\[|\\/|\\<|\\>");

        // Initially maps column names which don't need to be renamed to a usage count of 1,
        // and maps columns which need to be renamed to the tentative field name.
        var fieldMap = new HashMap<String, Object>();

        for (ProjExpr pe : projection) {
            if (pe.hasExclude()) {
                continue;
            }

            String name = pe.name();
            String fieldName = name.strip();

            if (fieldName.isEmpty()) {
                fieldName = "_";
            } else {
                fieldName = p.matcher(fieldName).replaceAll("_");
            }

            boolean rename = !fieldName.equals(name);

            if (!rename) {
                // Rename if there's a conflict with an inherited method name.
                switch (fieldName) {
                    case "hashCode", "equals", "toString", "clone", "getClass", "wait" -> {
                        rename = true;
                        fieldName += "_";
                    }
                }
            }

            fieldMap.put(name, rename ? fieldName : 1);
        }

        var columns = new ArrayList<Column>(projection.size());

        for (ProjExpr pe : projection) {
            if (pe.hasExclude()) {
                continue;
            }

            String name = pe.name();
            Object field = fieldMap.get(name);
            String fieldName;

            if (field instanceof Integer) {
                fieldName = name;
            } else {
                fieldName = (String) field;

                // Select the actual field name while ensuring that all field names are unique,
                // renaming them again if necessary.
                while (true) {
                    var count = (Integer) fieldMap.putIfAbsent(fieldName, 1);
                    if (count == null) {
                        break;
                    }
                    count++;
                    fieldMap.put(fieldName, count);
                    if (fieldName.endsWith("_")) {
                        fieldName += count;
                    } else {
                        fieldName = fieldName + "_" + count;
                    }
                }
            }

            columns.add(Column.make(pe.type(), name, fieldName, false));
        }

        // Temporarily use the IdentityTable.Row class.
        TupleType tt = new TupleType(IdentityTable.Row.class, columns.toArray(Column[]::new));

        if (tt.numColumns() != 0) {
            tt = tt.withRowType(cCache.obtain(tt.makeKey(), tt));
        }

        return tt;
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
                columns[ix++] = Column.make(BasicType.make(ci), name, name, ci.isHidden());
            }
            for (ColumnInfo ci : values.values()) {
                String name = ci.name;
                columns[ix++] = Column.make(BasicType.make(ci), name, name, ci.isHidden());
            }
        } else {
            columns = new Column[projection.size()];

            for (String name : projection.keySet()) {
                ColumnInfo ci = ColumnSet.findColumn(info.allColumns, name);
                if (ci == null) {
                    throw new IllegalArgumentException("Unknown column: " + name);
                }
                columns[ix++] = Column.make(BasicType.make(ci), name, ci.name, ci.isHidden());
            }
        }

        if (ix != columns.length) {
            throw new AssertionError();
        }

        return new TupleType(rowType, columns);
    }

    private final Column[] mColumns;
    private final Map<String, Integer> mColumnMap;

    private TupleType(Class clazz, Column[] columns) {
        this(clazz, TYPE_REFERENCE, columns, makeColumnMap(columns));
    }

    private static Map<String, Integer> makeColumnMap(Column[] columns) {
        Map<String, Integer> columnMap = new HashMap<>(columns.length * 2);

        for (int i=0; i<columns.length; i++) {
            String name = columns[i].name();
            if (columnMap.putIfAbsent(name, i) != null) {
                throw new IllegalArgumentException("Duplicate column: " + name);
            }
        }

        return columnMap;
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

    private TupleType withRowType(Class<?> clazz) {
        if (clazz() == clazz) {
            return this;
        }
        return new TupleType(clazz, typeCode, mColumns, mColumnMap);
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

    private static final WeakCache<Object, Class<?>, TupleType> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public Class<?> newValue(Object key, TupleType tt) {
                return tt.makeRowTypeClass();
            }
        };
    }

    private Class<?> makeRowTypeClass() {
        ClassMaker cm = RowGen.beginClassMakerForRowType(TupleType.class.getPackageName(), "Type");
        cm.sourceFile(getClass().getSimpleName()).addAnnotation(Unpersisted.class, true);

        var names = new String[mColumns.length];

        for (int i=0; i<names.length; i++) {
            Column c = mColumns[i];
            Type type = c.type();
            Class<?> clazz = type.clazz();
            String name = c.fieldName();
            names[i] = name;

            MethodMaker mm = cm.addMethod(clazz, name).public_().abstract_();

            if (c.type().isNullable()) {
                mm.addAnnotation(Nullable.class, true);
            }

            cm.addMethod(null, name, clazz).public_().abstract_();
        }

        // All columns must be in the primary key to preserve the numerical sequence.
        cm.addAnnotation(PrimaryKey.class, true).put("value", names);

        return cm.finish();
    }
}
