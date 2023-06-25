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

package org.cojen.tupl.sql;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.math.BigDecimal;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.function.BiFunction;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.ConversionException;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.ConvertCallSite;
import org.cojen.tupl.rows.Converter;
import org.cojen.tupl.rows.ConvertUtils;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.RowMaker;
import org.cojen.tupl.rows.WeakCache;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * Makes classes which extend BaseResultSet and wrap a row object. For supporting queries, the
 * generated classes need to be extended and the necessary BaseResultSet methods need to be
 * overridden. At a minimum, the `next` method needs to be overridden.
 *
 * @author Brian S O'Neill
 */
public final class ResultSetMaker {
    /**
     * Finds or makes a ResultSet implementation class.
     *
     * @param rowType interface consisting of column methods
     * @param projection maps original column names to target names; the order of the elements
     * determines the ResultSet column numbers; pass null to project all non-hidden columns
     * @throws SQLDataException if a requested column doesn't exist
     */
    static Class<?> find(Class<?> rowType, LinkedHashMap<String, String> projection)
        throws SQLDataException
    {
        RowInfo info = RowInfo.find(rowType);

        var columns = new LinkedHashMap<String, ColumnInfo>();

        if (projection == null) {
            putAllNonHidden(columns, info.keyColumns);
            putAllNonHidden(columns, info.valueColumns);
        } else {
            Map<String, ColumnInfo> all = info.allColumns;

            for (Map.Entry<String, String> e : projection.entrySet()) {
                String originalName = e.getKey();

                ColumnInfo colInfo = all.get(originalName);
                if (colInfo == null) {
                    throw new SQLDataException
                        ("Column \"" + originalName + "\" doesn't exist in \"" +
                         rowType.getSimpleName() + '"');
                }

                String targetName = e.getValue();
                if (targetName == null) {
                    targetName = originalName;
                }

                columns.put(targetName, colInfo);
            }
        }

        var maker = new ResultSetMaker(rowType, info, columns);

        return cCache.obtain(new Key(maker.mRowType, maker.mColumnPairs), maker);
    }

    private static void putAllNonHidden(Map<String, ColumnInfo> dst, Map<String, ColumnInfo> src) {
        for (Map.Entry<String, ColumnInfo> e : src.entrySet()) {
            ColumnInfo colInfo = e.getValue();
            if (!colInfo.isHidden()) {
                dst.put(e.getKey(), colInfo);
            }
        }
    }

    private static class Key {
        private final Class<?> mRowType;
        private final String[] mColumnPairs;

        Key(Class<?> rowType, String[] columnPairs) {
            mRowType = rowType;
            mColumnPairs = columnPairs;
        }

        @Override
        public int hashCode() {
            return mRowType.hashCode() * 31 + Arrays.hashCode(mColumnPairs);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof Key other
                && mRowType == other.mRowType && Arrays.equals(mColumnPairs, other.mColumnPairs);
        }
    }

    private static final WeakCache<Key, Class<?>, ResultSetMaker> cCache = new WeakCache<>() {
        protected Class<?> newValue(Key key, ResultSetMaker maker) {
            return maker.finish();
        }
    };

    private static int TYPE_OBJECT = 0b11111 | TYPE_NULLABLE;

    private final Class<?> mRowType;
    private final Class<?> mRowClass;
    private final LinkedHashMap<String, ColumnInfo> mColumns;
    private final String[] mColumnPairs;

    private boolean mHasWasNull;

    private ClassMaker mClassMaker;

    /**
     * @param columns maps target names to the original columns
     */
    private ResultSetMaker(Class<?> rowType, RowInfo info,
                           LinkedHashMap<String, ColumnInfo> columns)
    {
        mRowType = rowType;
        mRowClass = RowMaker.find(rowType);
        mColumns = columns;

        var pairs = new String[mColumns.size() << 1];
        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : mColumns.entrySet()) {
            pairs[i++] = entry.getKey().intern();
            pairs[i++] = entry.getValue().name.intern();
        }
        mColumnPairs = pairs;
    }

    /**
     * @param columnPairs maps target names to the original columns
     */
    private ResultSetMaker(Class<?> rowType, Class<?> rowClass, String[] columnPairs,
                           boolean hasWasNull)
    {
        mRowType = rowType;
        mRowClass = rowClass;

        Map<String, ColumnInfo> allColumns = RowInfo.find(rowType).allColumns;
        mColumns = new LinkedHashMap<>();

        for (int i = 0; i < columnPairs.length; i += 2) {
            mColumns.put(columnPairs[i], allColumns.get(columnPairs[i + 1]));
        }

        mColumnPairs = columnPairs;
        mHasWasNull = hasWasNull;
    }

    /**
     * Returns the generated class which has a no-arg constructor and an init method which
     * accepts a row object. The init method parameter type is the generated row implementation
     * class and not the row interface.
     *
     * @see RowMaker#find
     */
    private Class<?> finish() {
        // Note that the generated mRowClass is used as the peer in order to be accessible.
        mClassMaker = CodeUtils.beginClassMaker(ResultSetMaker.class, mRowClass, null, "rs");
        mClassMaker.public_().implement(BaseResultSet.class);

        mClassMaker.addConstructor().public_();

        mClassMaker.addField(mRowClass, "row").private_();

        // 0: not ready, 1: ready, 2: closed
        mClassMaker.addField(int.class, "state").private_();

        addInitMethod();
        addRowMethod();
        addFirstMethod();
        addToStringMethod();
        addCloseMethod();
        addIsClosedMethod();
        addMetaDataMethod();
        addFindColumnMethod();
        addWasNullMethod();

        // Get methods...

        addGetMethod(Object.class, TYPE_OBJECT, "getObject");
        addGetMethod(String.class, TYPE_UTF8 | TYPE_NULLABLE, "getString");
        addGetMethod(boolean.class, TYPE_BOOLEAN, "getBoolean");
        addGetMethod(byte.class, TYPE_BYTE, "getByte");
        addGetMethod(short.class, TYPE_SHORT, "getShort");
        addGetMethod(int.class, TYPE_INT, "getInt");
        addGetMethod(long.class, TYPE_LONG, "getLong");
        addGetMethod(float.class, TYPE_FLOAT, "getFloat");
        addGetMethod(double.class, TYPE_DOUBLE, "getDouble");
        addGetMethod(BigDecimal.class, TYPE_BIG_DECIMAL | TYPE_NULLABLE, "getBigDecimal");
        addGetMethod(byte[].class, TYPE_BYTE | TYPE_NULLABLE | TYPE_ARRAY, "getBytes");

        // Update methods...

        addUpdateMethod("updateObject", Object.class, TYPE_OBJECT);
        addUpdateMethod("updateString", String.class, TYPE_UTF8 | TYPE_NULLABLE);

        addUpdateMethod("updateByte", byte.class, TYPE_BYTE);
        addUpdateMethod("updateShort", short.class, TYPE_SHORT);
        addUpdateMethod("updateInt", int.class, TYPE_INT);
        addUpdateMethod("updateLong", long.class, TYPE_LONG);
        addUpdateMethod("updateFloat", float.class, TYPE_FLOAT);
        addUpdateMethod("updateDouble", double.class, TYPE_DOUBLE);
        addUpdateMethod("updateBigDecimal", BigDecimal.class, TYPE_BIG_DECIMAL | TYPE_NULLABLE);
        addUpdateMethod("updateBytes", byte[].class, TYPE_BYTE | TYPE_NULLABLE | TYPE_ARRAY);

        return mClassMaker.finish();
    }

    private void addInitMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "init", mRowClass).public_();
        mm.field("row").set(mm.param(0));
        mm.field("state").set(1);
    }

    private void addRowMethod() {
        MethodMaker mm = mClassMaker.addMethod(mRowClass, "row").private_();
        var rowVar = mm.field("row").get();
        Label ready = mm.label();
        rowVar.ifNe(null, ready);
        mm.var(ResultSetMaker.class).invoke("notReady", mm.field("state")).throw_();
        ready.here();
        mm.return_(rowVar);
    }

    private void addFirstMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "first").public_();
        mm.return_(mm.var(ResultSetMaker.class).invoke("first", mm.this_(), mm.field("state")));
    }

    private void addToStringMethod() {
        // TODO: Should show the projected column names, not the actual column names.
        MethodMaker mm = mClassMaker.addMethod(String.class, "toString").public_();
        mm.return_(mm.var(ResultSetMaker.class)
                   .invoke("toString", mm.this_(), mm.field("row")));
    }

    private void addCloseMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "close").public_();
        mm.field("state").set(2); // closed state
        mm.field("row").set(null);
    }

    private void addIsClosedMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "isClosed").public_();
        mm.return_(mm.field("state").ge(2));
    }

    private void addMetaDataMethod() {
        MethodMaker mm = mClassMaker.addMethod(ResultSetMetaData.class, "getMetaData").public_();
        var bootstrap = mm.var(ResultSetMaker.class).condy("condyMD", mRowType, mColumnPairs);
        mm.return_(bootstrap.invoke(ResultSetMetaData.class, "_"));
    }

    public static ResultSetMetaData condyMD(MethodHandles.Lookup lookup, String name, Class<?> type,
                                            Class<?> rowType, String[] columnPairs)
    {
        ClassMaker cm = ClassMaker.begin(null, lookup).implement(BaseResultSetMetaData.class);
        cm.addConstructor().private_();

        var maker = new ResultSetMaker(rowType, null, columnPairs, false);
        maker.mClassMaker = cm;

        cm.addMethod(int.class, "getColumnCount").public_().return_(maker.mColumns.size());

        maker.addMetaDataMethod(boolean.class, "isAutoIncrement", (n, info) -> info.isAutomatic());

        maker.addMetaDataMethod(int.class, "isNullable", (n, info) -> {
            return info.isNullable() ?
                ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
        });

        maker.addMetaDataMethod(boolean.class, "isSigned", (n, info) -> info.isSigned());

        maker.addMetaDataMethod(int.class, "getColumnDisplaySize", (n, info) -> {
            if (info.isArray()) {
                return Integer.MAX_VALUE;
            } else {
                return switch (info.plainTypeCode()) {
                case TYPE_BOOLEAN -> 5;
                case TYPE_UBYTE -> 3;
                case TYPE_USHORT -> 5;
                case TYPE_UINT -> 10;
                case TYPE_ULONG -> 20;
                case TYPE_BYTE -> 4;
                case TYPE_SHORT -> 5;
                case TYPE_INT -> 11;
                case TYPE_LONG -> 20;
                case TYPE_FLOAT -> 15;
                case TYPE_DOUBLE -> 24;
                case TYPE_CHAR -> 1;
                default -> Integer.MAX_VALUE;
                };
            }
        });

        maker.addMetaDataMethod(String.class, "getColumnName", (n, info) -> n);

        maker.addMetaDataMethod(int.class, "getPrecision", (n, info) -> {
            if (info.isArray()) {
                return 0;
            } else {
                return switch (info.plainTypeCode()) {
                case TYPE_BOOLEAN -> 0;
                case TYPE_UBYTE, TYPE_BYTE -> 3;
                case TYPE_USHORT, TYPE_SHORT -> 5;
                case TYPE_UINT, TYPE_INT -> 10;
                case TYPE_ULONG, TYPE_LONG -> 20;
                case TYPE_FLOAT -> 9;
                case TYPE_DOUBLE -> 17;
                case TYPE_CHAR -> 1;
                default -> Integer.MAX_VALUE;
                };
            }
        });

        maker.addMetaDataMethod(int.class, "getScale", (n, info) -> {
            if (info.isArray()) {
                return 0;
            } else {
                return switch (info.plainTypeCode()) {
                case TYPE_FLOAT -> 8;
                case TYPE_DOUBLE -> 16;
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> Integer.MAX_VALUE;
                default -> 0;
                };
            }
        });

        maker.addMetaDataMethod(int.class, "getColumnType", (n, info) -> {
            int tc = info.plainTypeCode();

            if (info.isArray()) {
                return switch (tc) {
                case TYPE_UBYTE, TYPE_BYTE -> Types.VARBINARY;
                default -> Types.ARRAY;
                };
            } else {
                return switch (tc) {
                case TYPE_BOOLEAN -> Types.BOOLEAN;
                case TYPE_UBYTE, TYPE_BYTE -> Types.TINYINT;
                case TYPE_USHORT, TYPE_SHORT -> Types.SMALLINT;
                case TYPE_UINT, TYPE_INT -> Types.INTEGER;
                case TYPE_ULONG, TYPE_LONG, TYPE_BIG_INTEGER -> Types.BIGINT;
                case TYPE_FLOAT -> Types.FLOAT;
                case TYPE_DOUBLE -> Types.DOUBLE;
                case TYPE_CHAR -> Types.CHAR;
                case TYPE_UTF8 -> Types.VARCHAR;
                case TYPE_BIG_DECIMAL -> Types.DECIMAL;
                default -> Types.OTHER;
                };
            }
        });

        lookup = cm.finishLookup();

        try {
            MethodHandle ctor = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (ResultSetMetaData) ctor.invoke();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param impl function accepts the target name and ColumnInfo, and provides a value
     */
    private void addMetaDataMethod(Class<?> type, String methodName,
                                   BiFunction<String, ColumnInfo, Object> impl)
    {
        MethodMaker mm = mClassMaker.addMethod(type, methodName, int.class).public_();
        var indexVar = mm.param(0);

        var cases = new int[mColumns.size()];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        Label defaultLabel = mm.label();
        indexVar.switch_(defaultLabel, cases, labels);

        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : mColumns.entrySet()) {
            labels[i++].here();
            mm.return_(impl.apply(entry.getKey(), entry.getValue()));
        }

        defaultLabel.here();
        mm.var(ResultSetMaker.class).invoke("notFound", indexVar).throw_();
    }

    @SuppressWarnings("unchecked")
    private void addFindColumnMethod() {
        MethodMaker mm = mClassMaker.addMethod(int.class, "findColumn", String.class).public_();
        var columnNameVar = mm.param(0);

        var cases = new String[mColumns.size()];
        var labels = new Label[cases.length];

        int ix = 0;
        for (String name : mColumns.keySet()) {
            cases[ix] = name;
            labels[ix] = mm.label();
            ix++;
        }

        Label defaultLabel = mm.label();

        columnNameVar.switch_(defaultLabel, cases, labels);

        for (int i=0; i<labels.length; i++) {
            labels[i].here();
            mm.return_(i + 1);
        }

        defaultLabel.here();
        mm.var(ResultSetMaker.class).invoke("notFound", columnNameVar).throw_();
    }

    private void addWasNullMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "wasNull").public_();

        for (ColumnInfo ci : mColumns.values()) {
            if (ci.isNullable()) {
                mClassMaker.addField(boolean.class, "wasNull").private_();
                mm.return_(mm.field("wasNull"));
                mHasWasNull = true;
                return;
            }
        }

        mm.return_(false);
    }

    private void addGetMethod(Class<?> returnType, int returnTypeCode, String name) {
        MethodMaker mm = mClassMaker.addMethod(returnType, name, int.class).public_();

        if (mHasWasNull) {
            returnTypeCode |= (1 << 31);
        }

        var bootstrap = mm.var(ResultSetMaker.class)
            .indy("indyGet", mRowType, mRowClass, mColumnPairs, returnTypeCode);

        mm.return_(bootstrap.invoke(returnType, "_", null, mm.this_(), mm.param(0)));
    }

    /**
     * @param returnTypeCode bit 31 set indicates that a wasNull field exists
     */
    public static CallSite indyGet(MethodHandles.Lookup lookup, String name, MethodType mt,
                                   Class<?> rowType, Class<?> rowClass,
                                   String[] columnPairs, int returnTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rsVar = mm.param(0);
        var indexVar = mm.param(1);
        var rowVar = rsVar.invoke("row");

        Field wasNullField = null;
        if (returnTypeCode < 0) {
            returnTypeCode &= Integer.MAX_VALUE;
            wasNullField = rsVar.field("wasNull");
        }

        var maker = new ResultSetMaker(rowType, rowClass, columnPairs, wasNullField != null);
        maker.makeGetMethod(mm, wasNullField, returnTypeCode, rowVar, indexVar);

        return new ConstantCallSite(mm.finish());
    }

    /**
     * @param wasNullField is null if not defined
     */
    private void makeGetMethod(MethodMaker mm, Field wasNullField, int returnTypeCode,
                               Variable rowVar, Variable indexVar)
    {
        var returnInfo = new ColumnInfo();
        returnInfo.typeCode = returnTypeCode;
        returnInfo.assignType();

        Variable returnVar = mm.var(returnInfo.type);

        var cases = new int[mColumns.size()];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        Label tryStart = mm.label().here();

        Label defaultLabel = mm.label();
        indexVar.switch_(defaultLabel, cases, labels);

        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : mColumns.entrySet()) {
            labels[i++].here();

            ColumnInfo colInfo = entry.getValue();
            Variable columnVar = rowVar.field(colInfo.name);

            if (wasNullField != null && colInfo.isNullable() && !returnInfo.isNullable()) {
                // Copy to a local variable because it will be accessed twice.
                columnVar = columnVar.get();
                Label notNull = mm.label();
                columnVar.ifNe(null, notNull);
                wasNullField.set(true);
                returnVar.clear();
                mm.return_(returnVar);
                notNull.here();
            }

            if (returnTypeCode != TYPE_OBJECT) {
                Converter.convertExact(mm, entry.getKey(),
                                       colInfo, columnVar, returnInfo, returnVar);
            } else {
                toObject: {
                    if (colInfo.isUnsignedInteger() && !colInfo.isArray()) {
                        switch (colInfo.plainTypeCode()) {
                        case TYPE_UBYTE:
                            returnVar.set(mm.var(Short.class).set(columnVar));
                            break toObject;
                        case TYPE_USHORT:
                            returnVar.set(mm.var(Integer.class).set(columnVar));
                            break toObject;
                        case TYPE_UINT:
                            returnVar.set(mm.var(Long.class).set(columnVar));
                            break toObject;
                        case TYPE_ULONG:
                            returnVar.set(mm.var(ConvertUtils.class).invoke
                                          ("unsignedLongToBigIntegerExact", columnVar));
                            break toObject;
                        }
                    }

                    returnVar.set(columnVar);
                }
            }

            if (wasNullField != null) {
                if (returnInfo.isNullable()) {
                    wasNullField.set(returnVar.eq(null));
                } else {
                    wasNullField.set(false);
                }
            }

            mm.return_(returnVar);
        }

        var rsmVar = mm.var(ResultSetMaker.class);

        if (returnTypeCode != TYPE_OBJECT) {
            mm.catch_(tryStart, ConversionException.class, exVar -> {
                rsmVar.invoke("failed", exVar).throw_();
            });
        }

        defaultLabel.here();
        rsmVar.invoke("notFound", indexVar).throw_();
    }

    private void addUpdateMethod(String name, Class<?> paramType, int paramTypeCode) {
        MethodMaker mm = mClassMaker.addMethod(null, name, int.class, paramType).public_();

        var bootstrap = mm.var(ResultSetMaker.class)
            .indy("indyUpdate", mRowType, mRowClass, mColumnPairs, paramTypeCode);

        bootstrap.invoke(null, "_", null, mm.this_(), mm.param(0), mm.param(1));
    }

    /**
     * @param returnTypeCode bit 31 set indicates that a wasNull field exists
     */
    public static CallSite indyUpdate(MethodHandles.Lookup lookup, String name, MethodType mt,
                                      Class<?> rowType, Class<?> rowClass,
                                      String[] columnPairs, int paramTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rsVar = mm.param(0);
        var indexVar = mm.param(1);
        var paramVar = mm.param(2);
        var rowVar = rsVar.invoke("row");

        var maker = new ResultSetMaker(rowType, rowClass, columnPairs, false); // not needed
        maker.makeUpdateMethod(mm, paramTypeCode, rowVar, indexVar, paramVar);

        return new ConstantCallSite(mm.finish());
    }

    private void makeUpdateMethod(MethodMaker mm, int paramTypeCode,
                                  Variable rowVar, Variable indexVar, Variable paramVar)
    {
        var paramInfo = new ColumnInfo();
        paramInfo.typeCode = paramTypeCode;
        paramInfo.assignType();

        var cases = new int[mColumns.size()];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        var rsmVar = mm.var(ResultSetMaker.class);

        Label tryStart = mm.label().here();

        Label defaultLabel = mm.label();
        indexVar.switch_(defaultLabel, cases, labels);

        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : mColumns.entrySet()) {
            labels[i++].here();

            ColumnInfo colInfo = entry.getValue();

            if (!colInfo.isNullable() && !paramInfo.type.isPrimitive()) {
                Label notNull = mm.label();
                paramVar.ifNe(null, notNull);
                rsmVar.invoke("notNull", entry.getKey()).throw_();
                notNull.here();
            }

            if (paramTypeCode != TYPE_OBJECT) {
                var columnVar = mm.var(colInfo.type);
                Converter.convertExact(mm, entry.getKey(),
                                       paramInfo, paramVar, colInfo, columnVar);
                rowVar.invoke(colInfo.name, columnVar);
                mm.return_();
            } else {
                Label convertStart = mm.label().here();

                var columnVar = ConvertCallSite.make(mm, colInfo.type, paramVar);
                rowVar.invoke(colInfo.name, columnVar);
                mm.return_();

                mm.catch_(tryStart, RuntimeException.class, exVar -> {
                    rsmVar.invoke("failed", entry.getKey(), exVar).throw_();
                });
            }
        }

        mm.catch_(tryStart, ConversionException.class, exVar -> {
            rsmVar.invoke("failed", exVar).throw_();
        });

        defaultLabel.here();
        rsmVar.invoke("notFound", indexVar).throw_();
    }

    // Called by generated code.
    public static String toString(Object rs, Object row) {
        var bob = new StringBuilder(ResultSet.class.getName()).append('@')
            .append(Integer.toHexString(System.identityHashCode(rs)));

        if (row != null) {
            String rowStr = row.toString();
            int ix = rowStr.indexOf('{');
            if (ix >= 0) {
                bob.append(rowStr.substring(ix));
            }
        }

        return bob.toString();
    }

    // Called by generated code.
    public static SQLNonTransientException notReady(int state) {
        String message = state == 0 ? "ResultSet isn't positioned; call next() or first()"
            : "ResultSet is closed";
        return new SQLNonTransientException(message);
    }

    // Called by generated code.
    public static boolean first(BaseResultSet rs, int state) throws SQLException {
        if (state == 0) {
            return rs.next();
        }
        String message = state == 1 ? "ResultSet is already positioned" : "ResultSet is closed";
        throw new SQLNonTransientException(message);
    }

    // Called by generated code.
    public static SQLDataException notFound(int columnIndex) {
        return new SQLDataException
            ("Column index " + columnIndex + " doesn't exist in the ResultSet");
    }

    // Called by generated code.
    public static SQLDataException notFound(String columnName) {
        return new SQLDataException
            ("Column \"" + columnName + "\" doesn't exist in the ResultSet");
    }

    // Called by generated code.
    public static SQLDataException notNull(String columnName) {
        return new SQLDataException
            ("Column \"" + columnName + "\" cannot be set to null");
    }

    // Called by generated code.
    public static SQLDataException failed(ConversionException ex) {
        return new SQLDataException
            ("Column conversion failed: " + ex.getMessage());
    }

    // Called by generated code.
    public static SQLDataException failed(String columnName, RuntimeException ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            message = ex.toString();
        }
        return new SQLDataException
            ("Column conversion failed for column \"" + columnName + "\": " + message);
    }
}
