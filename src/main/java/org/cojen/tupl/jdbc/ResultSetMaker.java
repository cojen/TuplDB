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

package org.cojen.tupl.jdbc;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import java.util.function.BiFunction;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import org.cojen.tupl.rows.ColumnSet;
import org.cojen.tupl.rows.ConvertCallSite;
import org.cojen.tupl.rows.Converter;
import org.cojen.tupl.rows.ConvertUtils;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.RowMaker;
import org.cojen.tupl.rows.WeakCache;

import org.cojen.tupl.rows.join.JoinRowMaker;

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
     * Finds or makes a ResultSet implementation class. The class has a public no-arg
     * constructor and a public init method which accepts a row object.
     *
     * @param rowType interface consisting of column methods
     * @param projection maps original column names to fully qualified target names; the order
     * of the elements determines the ResultSet column numbers; pass null to project all
     * non-hidden columns
     * @throws SQLNonTransientException if a requested column doesn't exist
     */
    static Class<?> find(Class<?> rowType, LinkedHashMap<String, String> projection)
        throws SQLNonTransientException
    {
        RowInfo info = RowInfo.find(rowType);

        var columns = new LinkedHashMap<String, ColumnInfo>();
        boolean joinType = false;

        if (projection == null) {
            gatherAllScalarColumns(columns, info.keyColumns);
            gatherAllScalarColumns(columns, info.valueColumns);

            Iterator<ColumnInfo> it = columns.values().iterator();
            while (it.hasNext()) {
                ColumnInfo colInfo = it.next();
                joinType |= colInfo.prefix() != null;
                if (colInfo.isHidden()) {
                    // Need to remove all of the hidden columns.
                    it.remove();
                }
            }
        } else {
            for (Map.Entry<String, String> e : projection.entrySet()) {
                String originalName = e.getKey();

                ColumnInfo colInfo = findColumn(rowType, info.allColumns, originalName);

                String targetName = e.getValue();
                if (targetName == null) {
                    targetName = originalName;
                }

                columns.put(targetName, colInfo);
                joinType |= colInfo.prefix() != null;
            }
        }

        Class<?> rowClass;
        if (!joinType) {
            rowClass = RowMaker.find(rowType);
        } else {
            rowClass = JoinRowMaker.find(rowType);
        }

        var maker = new ResultSetMaker(rowType, rowClass, info, columns);

        return cCache.obtain(new Key(maker.mRowType, maker.mColumnPairs), maker);
    }

    private static ColumnInfo findColumn(Class<?> rowType,
                                         Map<String, ColumnInfo> allColumns, String path)
        throws SQLNonTransientException
    {
        ColumnInfo colInfo = ColumnSet.findColumn(allColumns, path);

        if (colInfo != null) {
            return colInfo;
        }

        throw new SQLNonTransientException
            ("Column \"" + path + "\" doesn't exist in \"" +
             rowType.getSimpleName() + '"');
    }

    private static void gatherAllScalarColumns(Map<String, ColumnInfo> dst,
                                               Map<String, ColumnInfo> src)
    {
        for (Map.Entry<String, ColumnInfo> e : src.entrySet()) {
            e.getValue().gatherScalarColumns(dst);
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

    private static final WeakCache<Object, Class<?>, ResultSetMaker> cCache = new WeakCache<>() {
        protected Class<?> newValue(Object key, ResultSetMaker maker) {
            try {
                return maker.finish();
            } catch (SQLNonTransientException e) {
                throw Utils.rethrow(e);
            }
        }
    };

    private static final int TYPE_OBJECT = TYPE_REFERENCE | TYPE_NULLABLE;

    private final Class<?> mRowType;
    private final Class<?> mRowClass;
    private final String[] mColumnPairs;
    private final boolean mHasNullableColumns;

    private LinkedHashMap<String, ColumnInfo> mColumns;

    private ClassMaker mClassMaker;

    /**
     * @param columns maps target names to the original columns
     */
    private ResultSetMaker(Class<?> rowType, Class<?> rowClass, RowInfo info,
                           LinkedHashMap<String, ColumnInfo> columns)
    {
        mRowType = rowType;
        mRowClass = rowClass;
        mColumns = columns;

        boolean hasNullableColumns = false;

        var pairs = new String[mColumns.size() << 1];
        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : mColumns.entrySet()) {
            pairs[i++] = entry.getKey().intern();
            ColumnInfo ci = entry.getValue();
            pairs[i++] = ci.name.intern();
            hasNullableColumns |= ci.isNullable();
        }
        mColumnPairs = pairs;

        mHasNullableColumns = hasNullableColumns;
    }

    /**
     * Constructor used by indy and condy methods.
     *
     * @param columnPairs maps target names to the original columns
     */
    private ResultSetMaker(Class<?> rowType, String[] columnPairs, boolean hasNullableColumns) {
        mRowType = rowType;
        mRowClass = null; // not needed
        mColumnPairs = columnPairs;
        mHasNullableColumns = hasNullableColumns;
    }

    /**
     * Copy constructor which can only make the abstract parent class. It only defines methods
     * which don't depend on columns.
     */
    private ResultSetMaker(ResultSetMaker maker) {
        mRowType = maker.mRowType;
        mRowClass = maker.mRowClass;
        mColumns = maker.mColumns;
        mColumnPairs = null; // parent class indicator
        mHasNullableColumns = maker.mHasNullableColumns;
    }

    private Map<String, ColumnInfo> columns() throws SQLNonTransientException {
        Map<String, ColumnInfo> columns = mColumns;
        if (columns == null) {
            columns = buildColumnMap();
        }
        return columns;
    }

    private Map<String, ColumnInfo> buildColumnMap() throws SQLNonTransientException {
        Map<String, ColumnInfo> allColumns = RowInfo.find(mRowType).allColumns;
        var columns = new LinkedHashMap<String, ColumnInfo>();
        String[] columnPairs = mColumnPairs;
        for (int i = 0; i < columnPairs.length; i += 2) {
            columns.put(columnPairs[i], findColumn(mRowType, allColumns, columnPairs[i + 1]));
        }
        mColumns = columns;
        return columns;
    }

    /**
     * Returns the generated class which has a no-arg constructor and an init method which
     * accepts a row object. The init method parameter type is the generated row implementation
     * class and not the row interface.
     *
     * @see RowMaker#find
     */
    private Class<?> finish() throws SQLNonTransientException {
        // Note that the generated mRowClass is used as the peer in order to be accessible.
        mClassMaker = CodeUtils.beginClassMaker(ResultSetMaker.class, mRowClass, null, "rs");
        mClassMaker.public_();

        if (mColumnPairs == null) {
            // This is the parent abstract class.
            mClassMaker.extend(BaseResultSet.class).abstract_();
        } else {
            mClassMaker.extend(cCache.obtain(mRowType, new ResultSetMaker(this)));
        }

        mClassMaker.addConstructor().public_();

        if (mColumnPairs == null) {
            // This is the parent abstract class.

            mClassMaker.addField(mRowClass, "row").protected_();

            addInitMethod();
            addRowMethod();
            addCloseMethod();

            return mClassMaker.finish();
        }

        // The remaining methods depend on columns.

        addWasNullMethod();
        addToStringMethod();
        addMetaDataMethod();
        addFindColumnMethod();

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
        addUpdateMethod("updateBoolean", boolean.class, TYPE_BOOLEAN);
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
        MethodMaker mm = mClassMaker.addMethod(null, "init", mRowClass).public_().final_();
        mm.field("row").set(mm.param(0));
        mm.field("state").set(1);
    }

    private void addRowMethod() {
        MethodMaker mm = mClassMaker.addMethod(mRowClass, "row").protected_().final_();
        var rowVar = mm.field("row").get();
        Label ready = mm.label();
        rowVar.ifNe(null, ready);
        mm.var(ResultSetMaker.class).invoke("notReady", mm.field("state")).throw_();
        ready.here();
        mm.return_(rowVar);
    }

    private void addToStringMethod() {
        MethodMaker mm = mClassMaker.addMethod(String.class, "toString").public_().final_();
        var bootstrap = mm.var(ResultSetMaker.class).indy("indyToString", mRowType, mColumnPairs);
        mm.return_(bootstrap.invoke(String.class, "toString", null, mm.this_()));
    }

    public static CallSite indyToString(MethodHandles.Lookup lookup, String name, MethodType mt,
                                        Class<?> rowType, String[] columnPairs)
        throws SQLNonTransientException
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);
        var rsVar = mm.param(0);
        var rowVar = rsVar.field("row").get();

        var maker = new ResultSetMaker(rowType, columnPairs, false);
        maker.makeToStringMethod(mm, rsVar, rowVar);

        return new ConstantCallSite(mm.finish());
    }

    private void makeToStringMethod(MethodMaker mm, Variable rsVar, Variable rowVar)
        throws SQLNonTransientException
    {
        var bob = mm.var(ResultSetMaker.class).invoke("beginToString", rsVar);

        Label done = mm.label();
        rowVar.ifEq(null, done);

        var initSize = bob.invoke("append", '{').invoke("length");

        for (Map.Entry<String, ColumnInfo> e : columns().entrySet()) {
            ColumnInfo info = e.getValue();
            if (!info.isHidden()) {
                Label sep = mm.label();
                bob.invoke("length").ifEq(initSize, sep);
                bob.invoke("append", ", ");
                sep.here();
                bob.invoke("append", e.getKey()).invoke("append", '=');
                CodeUtils.appendValue(bob, info, CodeUtils.getColumnValue(rowVar, info, true));
            }
        }

        bob.invoke("append", '}');

        done.here();
        mm.return_(bob.invoke("toString"));
    }

    private void addCloseMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "close").public_().final_();
        mm.field("state").set(2); // closed state
        mm.field("row").set(null);
    }

    private void addMetaDataMethod() {
        MethodMaker mm = mClassMaker.addMethod(ResultSetMetaData.class, "getMetaData");
        mm.public_().final_();
        var bootstrap = mm.var(ResultSetMaker.class).condy("condyMD", mRowType, mColumnPairs);
        mm.return_(bootstrap.invoke(ResultSetMetaData.class, "_"));
    }

    public static ResultSetMetaData condyMD(MethodHandles.Lookup lookup, String name, Class<?> type,
                                            Class<?> rowType, String[] columnPairs)
        throws SQLNonTransientException
    {
        ClassMaker cm = ClassMaker.begin(null, lookup).extend(BaseResultSetMetaData.class).final_();
        cm.addConstructor().private_();

        var maker = new ResultSetMaker(rowType, columnPairs, false);
        maker.mClassMaker = cm;

        cm.addMethod(int.class, "getColumnCount").public_().return_(maker.columns().size());

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
        throws SQLNonTransientException
    {
        MethodMaker mm = mClassMaker.addMethod(type, methodName, int.class).public_();
        var indexVar = mm.param(0);

        var cases = new int[columns().size()];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        Label defaultLabel = mm.label();
        indexVar.switch_(defaultLabel, cases, labels);

        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : columns().entrySet()) {
            labels[i++].here();
            mm.return_(impl.apply(entry.getKey(), entry.getValue()));
        }

        defaultLabel.here();
        mm.var(ResultSetMaker.class).invoke("notFound", indexVar).throw_();
    }

    @SuppressWarnings("unchecked")
    private void addFindColumnMethod() throws SQLNonTransientException {
        MethodMaker mm = mClassMaker.addMethod(int.class, "findColumn", String.class);
        mm.public_().final_();
        var columnNameVar = mm.param(0);

        var rsMakerVar = mm.var(ResultSetMaker.class);
        rsMakerVar.invoke("nullCheck", columnNameVar);

        var caseMap = new HashMap<String, Label>(columns().size() * 3);
        var uniqueLabels = new ArrayList<Label>(caseMap.size());

        for (String name : columns().keySet()) {
            Label label = mm.label();
            uniqueLabels.add(label);
            caseMap.put(name, label);
            caseMap.put(name.toLowerCase(Locale.ROOT), label);
            caseMap.put(name.toUpperCase(Locale.ROOT), label);
        }

        var cases = new String[caseMap.size()];
        var labels = new Label[cases.length];

        {
            int i = 0;
            for (Map.Entry<String, Label> e : caseMap.entrySet()) {
                cases[i] = e.getKey();
                labels[i] = e.getValue();
                i++;
            }
        }

        Label defaultLabel = mm.label();

        Label start = mm.label().here();

        columnNameVar.switch_(defaultLabel, cases, labels);

        for (int i=0; i<uniqueLabels.size(); i++) {
            uniqueLabels.get(i).here();
            mm.return_(i + 1);
        }

        defaultLabel.here();

        columnNameVar.set(rsMakerVar.invoke("notFound", columnNameVar));
        start.goto_();
    }

    private void addWasNullMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "wasNull").public_().final_();
        if (mHasNullableColumns) {
            mClassMaker.addField(Object.class, "lastGet").private_();
            mm.return_(mm.field("lastGet").eq(null));
        } else {
            mm.return_(false);
        }
    }

    private void addGetMethod(Class<?> returnType, int returnTypeCode, String name) {
        MethodMaker mm = mClassMaker.addMethod(returnType, name, int.class).public_().final_();

        if (mHasNullableColumns) {
            returnTypeCode |= (1 << 31);
        }

        var bootstrap = mm.var(ResultSetMaker.class)
            .indy("indyGet", mRowType, mColumnPairs, returnTypeCode);

        mm.return_(bootstrap.invoke(returnType, "_", null, mm.this_(), mm.param(0)));
    }

    /**
     * @param returnTypeCode bit 31 set indicates that nullable columns exist
     */
    public static CallSite indyGet(MethodHandles.Lookup lookup, String name, MethodType mt,
                                   Class<?> rowType, String[] columnPairs, int returnTypeCode)
        throws SQLNonTransientException
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rsVar = mm.param(0);
        var indexVar = mm.param(1);
        var rowVar = rsVar.invoke("row");

        Field lastGetField = null;
        if (returnTypeCode < 0) {
            returnTypeCode &= Integer.MAX_VALUE;
            lastGetField = rsVar.field("lastGet");
        }

        var maker = new ResultSetMaker(rowType, columnPairs, lastGetField != null);
        maker.makeGetMethod(mm, lastGetField, returnTypeCode, rowVar, indexVar);

        return new ConstantCallSite(mm.finish());
    }

    /**
     * @param lastGetField is null if not defined
     */
    private void makeGetMethod(MethodMaker mm, Field lastGetField, int returnTypeCode,
                               Variable rowVar, Variable indexVar)
        throws SQLNonTransientException
    {
        var returnInfo = new ColumnInfo();
        returnInfo.typeCode = returnTypeCode;
        returnInfo.assignType();

        Variable returnVar = mm.var(returnInfo.type);

        var cases = new int[columns().size()];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        Label tryStart = mm.label().here();

        Label defaultLabel = mm.label();
        indexVar.switch_(defaultLabel, cases, labels);

        int i = 0;
        for (Map.Entry<String, ColumnInfo> entry : columns().entrySet()) {
            labels[i++].here();

            ColumnInfo colInfo = entry.getValue();
            var columnVar = CodeUtils.getColumnValue(rowVar, colInfo, true);

            if (lastGetField != null && colInfo.isNullable() && !returnInfo.isNullable()) {
                // Copy to a local variable because it will be accessed twice.
                columnVar = columnVar.get();
                Label notNull = mm.label();
                columnVar.ifNe(null, notNull);
                lastGetField.set(null);
                returnVar.clear();
                mm.return_(returnVar);
                notNull.here();

                if (returnTypeCode != TYPE_OBJECT) {
                    // The converter doesn't need to check null again.
                    colInfo = colInfo.copy();
                    colInfo.typeCode &= ~TYPE_NULLABLE;
                }
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

            if (lastGetField != null) {
                if (returnInfo.isNullable()) {
                    lastGetField.set(returnVar);
                } else {
                    lastGetField.set(Boolean.TRUE);
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
        MethodMaker mm = mClassMaker.addMethod(null, name, int.class, paramType).public_().final_();

        var bootstrap = mm.var(ResultSetMaker.class)
            .indy("indyUpdate", mRowType, mColumnPairs, paramTypeCode);

        bootstrap.invoke(null, "_", null, mm.this_(), mm.param(0), mm.param(1));
    }

    public static CallSite indyUpdate(MethodHandles.Lookup lookup, String name, MethodType mt,
                                      Class<?> rowType, String[] columnPairs, int paramTypeCode)
        throws SQLNonTransientException
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rsVar = mm.param(0);
        var indexVar = mm.param(1);
        var paramVar = mm.param(2);
        var rowVar = rsVar.invoke("row");

        var maker = new ResultSetMaker(rowType, columnPairs, false);
        maker.makeUpdateMethod(mm, paramTypeCode, rowVar, indexVar, paramVar);

        return new ConstantCallSite(mm.finish());
    }

    private void makeUpdateMethod(MethodMaker mm, int paramTypeCode,
                                  Variable rowVar, Variable indexVar, Variable paramVar)
        throws SQLNonTransientException
    {
        var paramInfo = new ColumnInfo();
        paramInfo.typeCode = paramTypeCode;
        paramInfo.assignType();

        var cases = new int[columns().size()];
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
        for (Map.Entry<String, ColumnInfo> entry : columns().entrySet()) {
            labels[i++].here();

            ColumnInfo colInfo = entry.getValue();

            if (!colInfo.isNullable() && !paramInfo.type.isPrimitive()) {
                Label notNull = mm.label();
                paramVar.ifNe(null, notNull);
                rsmVar.invoke("notNull", entry.getKey()).throw_();
                notNull.here();

                if (paramTypeCode != TYPE_OBJECT) {
                    // The converter doesn't need to check null again.
                    paramInfo = paramInfo.copy();
                    paramInfo.typeCode &= ~TYPE_NULLABLE;
                }
            }

            if (paramTypeCode != TYPE_OBJECT) {
                var columnVar = mm.var(colInfo.type);
                Converter.convertExact(mm, entry.getKey(),
                                       paramInfo, paramVar, colInfo, columnVar);
                CodeUtils.setColumnValue(rowVar, colInfo, columnVar);
                mm.return_();
            } else {
                Label convertStart = mm.label().here();

                var columnVar = ConvertCallSite.make(mm, colInfo.type, paramVar);
                CodeUtils.setColumnValue(rowVar, colInfo, columnVar);
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
    public static StringBuilder beginToString(Object rs) {
        return new StringBuilder(ResultSet.class.getName()).append('@')
            .append(Integer.toHexString(System.identityHashCode(rs)));
    }

    // Called by generated code.
    public static Exception notReady(int state) {
        String message = state == 0 ? "ResultSet isn't positioned; call next() or first()"
            : "ResultSet is closed";
        return new SQLNonTransientException(message);
    }

    // Called by generated code.
    public static Exception notFound(int columnIndex) {
        return new SQLNonTransientException
            ("Column index " + columnIndex + " doesn't exist in the ResultSet");
    }

    // Called by generated code. Returns a string for trying to find the column again.
    public static String notFound(String columnName) throws SQLException {
        String lowercase = columnName.toLowerCase(Locale.ROOT);
        if (lowercase.equals(columnName)) {
            throw new SQLNonTransientException
                ("Column \"" + columnName + "\" doesn't exist in the ResultSet");
        } else {
            return lowercase;
        }
    }

    // Called by generated code.
    public static void nullCheck(String columnName) throws SQLException {
        if (columnName == null) {
            throw new SQLNonTransientException("Null column name");
        }
    }

    // Called by generated code.
    public static Exception notNull(String columnName) {
        return new SQLNonTransientException
            ("Column \"" + columnName + "\" cannot be set to null");
    }

    // Called by generated code.
    public static Exception failed(ConversionException ex) {
        return new SQLNonTransientException
            ("Column conversion failed: " + ex.getMessage());
    }

    // Called by generated code.
    public static Exception failed(String columnName, RuntimeException ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            message = ex.toString();
        }
        return new SQLNonTransientException
            ("Column conversion failed for column \"" + columnName + "\": " + message);
    }
}
