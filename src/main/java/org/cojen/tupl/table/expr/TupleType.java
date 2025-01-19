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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.cojen.tupl.core.TupleKey;

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
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.QueryException;
import org.cojen.tupl.Row;
import org.cojen.tupl.Unsigned;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class TupleType extends Type implements Iterable<Column> {
    /**
     * Makes a type which has a generated row type class. Columns aren't defined for excluded
     * projections, unless they're part of the primary key.
     *
     * @param pkNum number of projected columns which should be part of the primary key
     * @throws QueryException if any names are duplicated
     */
    public static TupleType make(List<ProjExpr> projExprs, int pkNum) {
        String[] primaryKey;
        if (pkNum <= 0) {
            primaryKey = null;
        } else {
            primaryKey = new String[pkNum];

            for (int i=0; i<pkNum; i++) {
                ProjExpr pe = projExprs.get(i);
                String name = pe.name();

                if (pe.hasOrderBy()) {
                    var b = new StringBuilder(2 + name.length());
                    b.append(pe.hasDescending() ? '-' : '+');
                    if (pe.hasNullLow()) {
                        b.append('!');
                    }
                    name = b.append(name).toString();
                }

                primaryKey[i] = name;
            }
        }

        return make(projExprs, pkNum, primaryKey);
    }

    private static TupleType make(List<ProjExpr> projExprs, int pkNum, String[] primaryKey) {
        var columns = new TreeMap<String, Column>();

        for (int i=0; i<projExprs.size(); i++) {
            ProjExpr pe = projExprs.get(i);
            if (i < pkNum || !pe.shouldExclude()) {
                boolean hidden = pe.wrapped() instanceof ColumnExpr ce && ce.isHidden();
                Column column = Column.make(pe.type(), pe.name(), hidden);
                addColumn(columns, column);
            }
        }

        Class rowType = findRowTypeClass(columns, primaryKey);

        return new TupleType(rowType, TYPE_REFERENCE, null, columns);
    }

    /**
     * Makes a type which has a generated row type class.
     *
     * @throws QueryException if any names are duplicated
     */
    public static TupleType makeForColumns(Collection<ColumnInfo> columnList) {
        return makeForColumns(columnList, null);
    }

    /**
     * Makes a type which has a generated row type class.
     *
     * @param primaryKey can be null if none; can use +/- prefixes; not validated
     * @throws QueryException if any names are duplicated
     */
    public static TupleType makeForColumns(Collection<ColumnInfo> columnList, String[] primaryKey) {
        var columns = new TreeMap<String, Column>();

        for (ColumnInfo ci : columnList) {
            Column column = toColumn(ci);
            addColumn(columns, column);
        }

        Class rowType = findRowTypeClass(columns, primaryKey);

        return new TupleType(rowType, TYPE_REFERENCE, null, columns);
    }

    private static Class findRowTypeClass(SortedMap<String, Column> columns, String[] primaryKey) {
        Object helper;
        if (primaryKey == null || primaryKey.length == 0) {
            helper = columns;
        } else {
            helper = TupleKey.make.with(primaryKey, columns);
        }

        var enc = new KeyEncoder();
        enc.encodeStrings(primaryKey);
        for (Column column : columns.values()) {
            column.encodeKey(enc);
        }

        return cGeneratedCache.obtain(enc.finish(), helper);
    }

    /**
     * Makes a type which uses the given row type class.
     *
     * @param projection consists of column names; can pass null to project all columns
     * @throws QueryException if projection refers to a non-existent column
     */
    public static TupleType forClass(Class rowType, Set<String> projection) {
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

    @Override
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
     * each column of this tuple, and no more. Wildcards aren't supported.
     */
    public boolean isFullProjection(Collection<ProjExpr> projection) {
        Map<String, Column> columns = columns();

        var accessed = new HashSet<String>();

        for (ProjExpr pe : projection) {
            if (pe.flags() != 0 || !(pe.wrapped() instanceof ColumnExpr ce) || ce.isPath()) {
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
            if (!pe.shouldExclude()) {
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
                    throw new IllegalArgumentException("Unknown column: " + name);
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
     * Returns a TupleType which has generated row type class with the given primary key, if
     * provided. The primary key elements can be prefixed to control column sort order.
     *
     * <p>This method shouldn't be called if the row type class already implements Row and
     * conforms to the given primary key specification.
     *
     * @param primaryKey can be null if none
     */
    public TupleType withPrimaryKey(String[] primaryKey) {
        SortedMap<String, Column> columns;
        if (mColumns instanceof SortedMap<String, Column> sm) {
            columns = sm;
        } else {
            columns = new TreeMap<>();
            columns.putAll(mColumns);
        }

        Class rowType = findRowTypeClass(columns, primaryKey);

        return new TupleType(rowType, typeCode, mProjection, mColumns);
    }

    /**
     * Returns true if the given projection consists of columns which are found in this tuple,
     * possibly requiring a safe conversion.
     *
     * @param exact when true, the column types must exactly match
     */
    public boolean canRepresent(Collection<ProjExpr> projExprs, boolean exact) {
        Map<String, Column> columns = columns();
        for (ProjExpr pe : projExprs) {
            if (!pe.shouldExclude() && !pe.canRepresent(columns, exact)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the given tuple consists of columns which are found in this tuple,
     * possibly requiring a safe conversion.
     *
     * @param exact when true, the column types must exactly match
     */
    public boolean canRepresent(TupleType other, boolean exact) {
        Map<String, Column> columns = columns();
        for (Column otherColumn : other.columns().values()) {
            Column thisColumn = columns.get(otherColumn.name());
            if (thisColumn == null || !thisColumn.type().canRepresent(otherColumn.type(), exact)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Call when canRepresent returns false to get a detailed message.
     */
    public String notRepresentable(Iterable<? extends Attr> columns, boolean exact) {
        var b = new StringBuilder().append("Query derives new or mismatched columns: ");
        final int originalLength = b.length();

        for (Attr column : columns) {
            String name = column.name();
            Column thisColumn = tryFindColumn(name);
            if (thisColumn == null || !thisColumn.type().canRepresent(column.type(), exact)) {
                if (b.length() != originalLength) {
                    b.append(", ");
                }
                column.type().appendTo(b, true);
                b.append(' ').append(RowMethodsMaker.unescape(name));
                if (thisColumn != null) {
                    b.append(" cannot be converted to ");
                    thisColumn.type().appendTo(b, true);
                }
            }
        }

        if (b.length() == originalLength) {
            // Shouldn't happen if canRepresent returned false.
            b.append('?');
        }

        return b.toString();
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

    private static final WeakCache<Object, Class<?>, Object> cGeneratedCache;

    static {
        cGeneratedCache = new WeakCache<>() {
            @Override
            @SuppressWarnings("unchecked")
            public Class<?> newValue(Object key, Object helper) {
                String[] primaryKey;
                Map<String, Column> columns;                
                if (helper instanceof TupleKey tk) {
                    primaryKey = (String[]) tk.get(0);
                    columns = (Map<String, Column>) tk.get(1);
                } else {
                    primaryKey = null;
                    columns = (Map<String, Column>) helper;
                }

                return columns.isEmpty() ? Row.class : makeRowTypeClass(primaryKey, columns);
            }
        };
    }

    private static Class<?> makeRowTypeClass(String[] primaryKey, Map<String, Column> columns) {
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

            if (c.type().isUnsignedInteger()) {
                mm.addAnnotation(Unsigned.class, true);
            }

            if (c.isHidden()) {
                mm.addAnnotation(Hidden.class, true);
            }

            cm.addMethod(null, name, clazz).public_().abstract_();
        }

        if (primaryKey != null) {
            cm.addAnnotation(PrimaryKey.class, true).put("value", primaryKey);
        }

        return cm.finish();
    }
}
