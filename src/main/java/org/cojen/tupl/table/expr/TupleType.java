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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowMethodsMaker;
import org.cojen.tupl.table.Unpersisted;
import org.cojen.tupl.table.WeakCache;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Hidden;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.Row;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class TupleType extends Type implements Iterable<Column> {
    /**
     * Makes a type which has a generated row type class. Columns aren't defined for excluded
     * projections.
     *
     * @throws QueryException if any names are duplicated
     */
    public static TupleType make(Collection<ProjExpr> projExprs) {
        var columns = new TreeMap<String, Column>();

        var enc = new KeyEncoder();

        for (ProjExpr pe : projExprs) {
            if (!pe.hasExclude()) {
                boolean hidden = pe.wrapped() instanceof ColumnExpr ce && ce.isHidden();
                Column column = Column.make(pe.type(), pe.name(), hidden);
                addColumn(columns, column);
                column.encodeKey(enc);
            }
        }

        Class rowType = cGeneratedCache.obtain(enc.finish(), columns);

        return new TupleType(rowType, TYPE_REFERENCE, null, columns);
    }

    /**
     * Makes a type which has a generated row type class. Columns aren't defined for excluded
     * projections.
     *
     * @throws QueryException if any names are duplicated
     */
    public static TupleType makeForColumns(Collection<ColumnInfo> columnList) {
        var columns = new TreeMap<String, Column>();

        var enc = new KeyEncoder();

        for (ColumnInfo ci : columnList) {
            Column column = toColumn(ci);
            addColumn(columns, column);
            column.encodeKey(enc);
        }

        Class rowType = cGeneratedCache.obtain(enc.finish(), columns);

        return new TupleType(rowType, TYPE_REFERENCE, null, columns);
    }

    /**
     * Makes a type which uses the given row type class.
     *
     * @param projection consists of column names; can pass null to project all columns
     * @throws QueryException if projection refers to a non-existent column
     */
    public static TupleType make(Class rowType, Set<String> projection) {
        // Validate the rowType definition.
        RowInfo.find(rowType);

        SortedSet<String> sortedProjection;

        if (projection == null) {
            sortedProjection = null;
        } else if (projection instanceof SortedSet<String> ss) {
            sortedProjection = ss;
        } else {
            sortedProjection = new TreeSet<>(projection);
        }

        return new TupleType(rowType, TYPE_REFERENCE, sortedProjection, null);
    }

    private final SortedSet<String> mProjection;

    private Map<String, Column> mColumns;

    private TupleType(Class rowType, int typeCode,
                      SortedSet<String> projection, Map<String, Column> columns)
    {
        super(rowType, typeCode);
        mProjection = projection;
        mColumns = columns;
    }

    private TupleType(TupleType tt, int typeCode) {
        super(tt.clazz(), typeCode);
        mProjection = tt.mProjection;
        mColumns = tt.mColumns;
    }

    private TupleType(ColumnInfo ci) {
        super(ci.type, ci.typeCode);
        mProjection = null;
    }

    @Override
    public TupleType nullable() {
        return isNullable() ? this : new TupleType(this, TYPE_REFERENCE | TYPE_NULLABLE);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeReference(clazz());
            enc.encodeInt(typeCode());
            if (mProjection == null) {
                enc.encodeByte(0);
            } else {
                enc.encodeUnsignedVarInt(mProjection.size() + 1);
                for (String s : mProjection) {
                    enc.encodeString(s);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return clazz().hashCode() * 31 + Objects.hashCode(mProjection);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof TupleType tt
            && clazz() == tt.clazz() && typeCode() == tt.typeCode()
            && Objects.equals(mProjection, tt.mProjection);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b, boolean simple) {
        b.append('{');

        int i = 0;
        for (Column column : this) {
            if (i > 0) {
                b.append(", ");
            }
            column.type().appendTo(b, simple);
            b.append(' ').append(column.name());
            i++;
        }

        b.append('}');
    }

    public int numColumns() {
        return columns().size();
    }

    public Iterator<Column> iterator() {
        return columns().values().iterator();
    }

    /**
     * Returns the column which has the given name.
     *
     * @throws QueryException if not found
     */
    public Column findColumn(String name) {
        Column c = tryFindColumn(name);
        if (c == null) {
            throw new QueryException("Unknown column: " + name);
        }
        return c;
    }

    /**
     * Returns the column which has the given name.
     *
     * @return null if not found
     */
    public Column tryFindColumn(String name) {
        return columns().get(name);
    }

    /**
     * Returns true if the given projection only consists of unordered columns, and refers to
     * each column of this tuple, and no more. Only the first column in a path is considered.
     */
    public boolean isFullProjection(Collection<ProjExpr> projection) {
        Map<String, Column> columns = columns();

        var accessed = new HashSet<String>();

        for (ProjExpr pe : projection) {
            if (pe.hasExclude() || pe.hasOrderBy()) {
                return false;
            }
            Expr e = pe.wrapped();
            if (!(e instanceof ColumnExpr ce)) {
                return false;
            }
            Column column = ce.firstColumn();
            String name = column.name();
            if (!column.equals(columns.get(name))) {
                return false;
            }
            accessed.add(name);
        }

        return accessed.size() == columns.size();
    }

    /**
     * Returns a TupleType subset consisting of the given projected columns.
     *
     * @param projExprs if null, is treated as full projection
     * @throws IllegalArgumentException if projExprs refers to columns not in this TupleType
     */
    public TupleType withProjection(Collection<ProjExpr> projExprs) {
        if (projExprs == null) {
            return this;
        }

        Map<String, Column> columns = columns();

        var projColumns = new TreeMap<String, Column>();

        for (ProjExpr pe : projExprs) {
            if (!pe.hasExclude()) {
                String name;
                if (!(pe.wrapped() instanceof ColumnExpr ce)) {
                    name = pe.name();
                } else {
                    Column first = ce.firstColumn();
                    if (first == null) {
                        throw new IllegalArgumentException();
                    }
                    name = first.name();
                }

                Column column = columns.get(name);
                if (column == null) {
                    throw new IllegalArgumentException();
                }

                projColumns.put(name, column);
            }
        }

        if (projColumns.equals(columns)) {
            return this;
        }

        return new TupleType(clazz(), typeCode(), projColumns.navigableKeySet(), projColumns);
    }

    /**
     * Returns true if the given projection only consists of columns which are found in this
     * tuple.
     */
    public boolean canRepresent(Collection<ProjExpr> projExprs) {
        Map<String, Column> columns = columns();
        for (ProjExpr pe : projExprs) {
            if (!pe.hasExclude() && !pe.matches(columns)) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Column> columns() {
        Map<String, Column> columns = mColumns;
        if (columns == null) {
            mColumns = columns = buildColumns();
        }
        return columns;
    }

    private Map<String, Column> buildColumns() {
        if (mProjection != null && mProjection.isEmpty()) {
            return Map.of();
        }

        RowInfo info = RowInfo.find(clazz());

        var columns = new TreeMap<String, Column>();

        if (mProjection == null) {
            for (ColumnInfo ci : info.allColumns.values()) {
                addColumn(columns, ci);
            }
        } else {
            for (String name : mProjection) {
                ColumnInfo ci = info.allColumns.get(name);
                if (ci == null) {
                    name = RowMethodsMaker.unescape(name);
                    throw new QueryException("Unknown column: " + name);
                }
                addColumn(columns, ci);
            }
        }

        return columns;
    }

    private static Column toColumn(ColumnInfo ci) {
        Type type = isTupleType(ci) ? new TupleType(ci) : BasicType.make(ci);
        return Column.make(type, ci.name, ci.isHidden());
    }

    private static void addColumn(Map<String, Column> columns, ColumnInfo ci) {
        addColumn(columns, toColumn(ci));
    }

    private static void addColumn(Map<String, Column> columns, Column column) {
        String name = column.name();
        if (columns.putIfAbsent(name, column) != null) {
            name = RowMethodsMaker.unescape(name);
            throw new QueryException("Duplicate column: " + name);
        }
    }

    private static boolean isTupleType(ColumnInfo ci) {
        if (ci.isScalarType()) {
            return false;
        }
        try {
            RowInfo.find(ci.type);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    private static final WeakCache<Object, Class<?>, Map<String, Column>> cGeneratedCache;

    static {
        cGeneratedCache = new WeakCache<>() {
            @Override
            public Class<?> newValue(Object key, Map<String, Column> columns) {
                return makeRowTypeClass(columns);
            }
        };
    }

    private static Class<?> makeRowTypeClass(Map<String, Column> columns) {
        ClassMaker cm = RowGen.beginClassMakerForRowType(TupleType.class.getPackageName(), "Type");
        cm.implement(Row.class);
        cm.sourceFile(TupleType.class.getSimpleName());
        cm.addAnnotation(Unpersisted.class, true);

        for (Column c : columns.values()) {
            Type type = c.type();
            Class<?> clazz = type.clazz();
            String name = c.name();

            MethodMaker mm = cm.addMethod(clazz, name).public_().abstract_();

            if (c.type().isNullable()) {
                mm.addAnnotation(Nullable.class, true);
            }

            if (c.isHidden()) {
                mm.addAnnotation(Hidden.class, true);
            }

            cm.addMethod(null, name, clazz).public_().abstract_();
        }

        return cm.finish();
    }
}
