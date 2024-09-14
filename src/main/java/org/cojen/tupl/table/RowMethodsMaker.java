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

package org.cojen.tupl.table;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import java.util.function.Consumer;

import org.cojen.maker.Bootstrap;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.ConversionException;
import org.cojen.tupl.Row;

import org.cojen.tupl.table.codec.ColumnCodec;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Makes methods to implement the Row interface.
 *
 * @author Brian S. O'Neill
 */
public final class RowMethodsMaker {
    /**
     * Given an arbitrary column name, returns a valid method name by possibly introducing
     * escape characters.
     */
    public static String escape(String name) {
        for (int i=0; i<name.length(); i++) {
            char c = name.charAt(i);
            char e = escape(c);
            if (e != '\0') {
                var b = new StringBuilder(name.length() + 1);
                b.append(name, 0, i).append('\\').append(e);
                i++;
                for (; i<name.length(); i++) {
                    c = name.charAt(i);
                    e = escape(c);
                    if (e == '\0') {
                        b.append(c);
                    } else {
                        b.append('\\').append(e);
                    }
                }
                return b.toString();
            }
        }

        // Also need to handle conflicts with inherited method names, and the empty string.

        switch (name) {
            case "hashCode", "equals", "toString", "clone", "getClass", "wait", "" -> name += '\\';
        }

        return name;
    }

    /**
     * @return 0 if no need to escape
     */
    private static char escape(char c) {
        return switch (c) {
            case '\\' -> '\\';
            case '.'  -> '_';
            case ';'  -> ':';
            case '['  -> '(';
            case '/'  -> '-';
            case '<'  -> '{';
            case '>'  -> '}';
            default   -> '\0';
        };
    }

    /**
     * Converts an escaped string back to its original form.
     */
    public static String unescape(String name) {
        int length = name.length();

        for (int i=0; i<length; i++) {
            char c = name.charAt(i);
            if (c == '\\') {
                i++;
                if (i >= length) {
                    return name.substring(0, length - 1);
                }
                var b = new StringBuilder(name.length() - 1);
                b.append(name, 0, i - 1).append(unescape(name.charAt(i)));
                i++;
                for (; i<name.length(); i++) {
                    c = name.charAt(i);
                    if (c != '\\') {
                        b.append(c);
                    } else {
                        i++;
                        if (i >= length) {
                            break;
                        }
                        b.append(unescape(name.charAt(i)));
                    }
                }
                return b.toString();
            }
        }

        return name;
    }

    private static char unescape(char c) {
        return switch (c) {
            case '\\' -> '\\';
            case '_'  -> '.';
            case ':'  -> ';';
            case '('  -> '[';
            case '-'  -> '/';
            case '{'  -> '<';
            case '}'  -> '>';
            default   -> c;
        };
    }

    // Used to cache generated classes which contain only static methods.
    private static volatile WeakClassCache<Class<?>> cRowMethodsClasses;

    /**
     * Returns a class which contains methods which correspond to all the Row methods, except
     * they're static and the last parameter is the row instance.
     */
    private static Class<?> rowMethodsClass(Class<?> rowType) {
        WeakClassCache<Class<?>> cache = cRowMethodsClasses;

        if (cache == null) {
            synchronized (RowMethodsMaker.class) {
                cache = cRowMethodsClasses;
                if (cache == null) {
                    cRowMethodsClasses = cache = new WeakClassCache<>() {
                        @Override
                        protected Class<?> newValue(Class<?> rowType, Object unused) {
                            return makeRowMethodsClass(rowType);
                        }
                    };
                }
            }
        }

        return cache.obtain(rowType, null);
    }

    private static Class<?> makeRowMethodsClass(Class<?> rowType) {
        ClassMaker cm = ClassMaker.begin(rowType.getName()).public_().final_();
        new RowMethodsMaker(cm, rowType, RowInfo.find(rowType).rowGen(), true).addMethods();
        return cm.finishHidden().lookupClass();
    }

    private final ClassMaker mClassMaker;
    private final Class<?> mRowType;
    private final boolean mStaticMethods;
    private final ColumnInfo[] mColumns;
    private final boolean mHasReferences;
    private final Map<String, Integer> mUnescapedMap; // maps unescaped names to column indexes

    public RowMethodsMaker(ClassMaker cm, Class<?> rowType, RowInfo rowInfo) {
        this(cm, rowType, rowInfo.rowGen(), false);
    }

    public RowMethodsMaker(ClassMaker cm, Class<?> rowType, RowGen rowGen) {
        this(cm, rowType, rowGen, false);
    }

    /**
     * @param staticMethods when true, define static methods which accept a row object as the
     * last parameter
     */
    private RowMethodsMaker(ClassMaker cm, Class<?> rowType, RowGen rowGen, boolean staticMethods) {
        mClassMaker = cm;
        mRowType = rowType;
        mStaticMethods = staticMethods;

        RowInfo rowInfo = rowGen.info;
        var columns = new ColumnInfo[rowInfo.allColumns.size()];

        boolean hasReferences = false;

        int i = 0;
        for (ColumnInfo info : rowInfo.keyColumns.values()) {
            columns[i++] = info;
            hasReferences |= !info.isScalarType();
        }
        for (ColumnCodec codec : rowGen.valueCodecs()) { // use encoding order
            ColumnInfo info = codec.info;
            columns[i++] = info;
            hasReferences |= !info.isScalarType();
        }

        assert i == columns.length;

        mColumns = columns;
        mHasReferences = hasReferences;

        Map<String, Integer> unescapedMap = Map.of();

        for (i=0; i<columns.length; i++) {
            String name = columns[i].name;
            String unescaped = unescape(name);
            if (!unescaped.equals(name)) {
                if (unescapedMap.isEmpty()) {
                    unescapedMap = new HashMap<>();
                }
                unescapedMap.put(unescaped, i);
            }
        }

        mUnescapedMap = unescapedMap;
    }

    private RowMethodsMaker(ClassMaker cm, Class<?> rowType) {
        this(cm, rowType, RowInfo.find(rowType));
    }

    public void addMethods() {
        if (mHasReferences) {
            // Returns a lazily initialized PathSplitter instance.
            MethodMaker mm = addPublicMethod(PathSplitter.class, "splitter").static_();
            Bootstrap condy = mm.var(RowMethodsMaker.class).condy("splitter", mRowType);
            mm.return_(condy.invoke(PathSplitter.class, "splitter"));
        }

        addColumnType();
        addColumnMethodName();

        addGetSetMethods(Object.class, TYPE_REFERENCE | TYPE_NULLABLE);
        addGetSetMethods(boolean.class, TYPE_BOOLEAN);
        addGetSetMethods(boolean[].class, TYPE_BOOLEAN | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(byte.class, TYPE_BYTE);
        addGetSetMethods(byte[].class, TYPE_BYTE | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(short.class, TYPE_SHORT);
        addGetSetMethods(short[].class, TYPE_SHORT | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(char.class, TYPE_CHAR);
        addGetSetMethods(char[].class, TYPE_CHAR | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(int.class, TYPE_INT);
        addGetSetMethods(int[].class, TYPE_INT | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(long.class, TYPE_LONG);
        addGetSetMethods(long[].class, TYPE_LONG | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(float.class, TYPE_FLOAT);
        addGetSetMethods(float[].class, TYPE_FLOAT | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(double.class, TYPE_DOUBLE);
        addGetSetMethods(double[].class, TYPE_DOUBLE | TYPE_ARRAY | TYPE_NULLABLE);
        addGetSetMethods(Boolean.class, TYPE_BOOLEAN | TYPE_NULLABLE);
        addGetSetMethods(Byte.class, TYPE_BYTE | TYPE_NULLABLE);
        addGetSetMethods(Short.class, TYPE_SHORT | TYPE_NULLABLE);
        addGetSetMethods(Character.class, TYPE_CHAR | TYPE_NULLABLE);
        addGetSetMethods(Integer.class, TYPE_INT | TYPE_NULLABLE);
        addGetSetMethods(Long.class, TYPE_LONG | TYPE_NULLABLE);
        addGetSetMethods(Float.class, TYPE_FLOAT | TYPE_NULLABLE);
        addGetSetMethods(Double.class, TYPE_DOUBLE | TYPE_NULLABLE);
        addGetSetMethods(String.class, TYPE_UTF8 | TYPE_NULLABLE);
        addGetSetMethods(BigInteger.class, TYPE_BIG_INTEGER | TYPE_NULLABLE);
        addGetSetMethods(BigDecimal.class, TYPE_BIG_DECIMAL | TYPE_NULLABLE);
    }

    private MethodMaker addPublicMethod(Object retType, String name, Object... paramTypes) {
        if (!mStaticMethods) {
            return mClassMaker.addMethod(retType, name, paramTypes).public_();
        } else {
            paramTypes = Arrays.copyOf(paramTypes, paramTypes.length + 1);
            paramTypes[paramTypes.length - 1] = mRowType;
            return mClassMaker.addMethod(retType, name, paramTypes).public_().static_();
        }
    }

    private Variable rowVar(MethodMaker mm) {
        return !mStaticMethods ? mm.this_() : mm.param(mm.paramCount() - 1);
    }

    /**
     * @param mm param(0) is the column name, param(1) is the row instance, and param(2) is the
     * value instance if making a "set" method
     * @param caseBody generates code which must return from the method
     */
    private void nameSwitch(Lookup lookup, MethodType type,
                            MethodMaker mm, Consumer<ColumnInfo> caseBody)
    {
        // Accepts escaped and unescaped forms.

        var nameVar = mm.param(0);

        {
            var cases = new String[mColumns.length + mUnescapedMap.size()];
            var labels = new Label[cases.length];

            {
                int i = 0;

                for (; i<mColumns.length; i++) {
                    cases[i] = mColumns[i].name;
                    labels[i] = mm.label();
                }

                if (!mUnescapedMap.isEmpty()) {
                    for (var entry : mUnescapedMap.entrySet()) {
                        cases[i] = entry.getKey();
                        labels[i] = labels[entry.getValue()];
                        i++;
                    }
                }

                assert i == cases.length;
            }

            Label default_ = mm.label();

            nameVar.switch_(default_, cases, labels);

            for (int i=0; i<mColumns.length; i++) {
                labels[i].here();
                caseBody.accept(mColumns[i]);
            }

            default_.here();
        }

        if (mHasReferences) {
            // Support accessing joined columns by path.

            var splitterVar = mm.var(lookup.lookupClass()).invoke("splitter");
            var entryVar = splitterVar.invoke("find", nameVar);
            var tailVar = entryVar.field("tail");

            Label tryStart = mm.label().here();

            var cases = new int[mColumns.length];
            var labels = new Label[cases.length];

            for (int i=0; i<cases.length; i++) {
                cases[i] = i;
                labels[i] = mm.label();
            }

            Label default_ = mm.label();

            entryVar.field("number").switch_(default_, cases, labels);

            var rowVar = mm.param(1);

            for (int i=0; i<labels.length; i++) {
                labels[i].here();
                var subVar = rowVar.invoke(mColumns[i].name);

                Label notNull = mm.label();
                subVar.ifNe(null, notNull);
                if (type.returnType().isPrimitive()) {
                    mm.var(RowMethodsMaker.class).invoke("nullJoin", nameVar, tailVar).throw_();
                } else {
                    mm.return_(null);
                }
                notNull.here();

                String name = mm.name();

                if (Row.class.isAssignableFrom(subVar.classType())) {
                    if (name.equals("set")) {
                        subVar.invoke(name, tailVar, mm.param(2));
                        mm.return_();
                    } else {
                        mm.return_(subVar.invoke(name, tailVar));
                    }
                } else {
                    var rowMethodsVar = mm.var(rowMethodsClass(subVar.classType()));
                    if (name.equals("set")) {
                        rowMethodsVar.invoke(name, tailVar, mm.param(2), subVar);
                        mm.return_();
                    } else {
                        mm.return_(rowMethodsVar.invoke(name, tailVar, subVar));
                    }
                }
            }

            mm.catch_(tryStart, IllegalArgumentException.class, exVar -> {
                // Remove a bogus entry from the PathSplitter to free up memory.
                splitterVar.invoke("remove", entryVar);
                exVar.throw_();
            });

            default_.here();
        }

        mm.var(RowMethodsMaker.class).invoke("notFound", nameVar).throw_();
    }

    private void addColumnType() {
        String name = "columnType";
        MethodMaker mm = addPublicMethod(Class.class, name, String.class);
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indyColumnType", mRowType);
        mm.return_(indy.invoke(Class.class, name, null, mm.param(0), rowVar(mm)));
    }

    public static CallSite indyColumnType(Lookup lookup, String name, MethodType type,
                                          Class<?> rowType)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var nameVar = mm.param(0); // nameSwitch will access it
        //var rowVar = mm.param(1); // nameSwitch might access it

        new RowMethodsMaker(mm.classMaker(), rowType)
            .nameSwitch(lookup, type, mm, ci -> mm.return_(ci.type));

        return new ConstantCallSite(mm.finish());
    }

    private void addColumnMethodName() {
        if (!mStaticMethods && !mHasReferences && mUnescapedMap.isEmpty()) {
            // Rely on the default implementation.
            return;
        }
        String name = "columnMethodName";
        MethodMaker mm = addPublicMethod(String.class, name, String.class);
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indyColumnMethodName", mRowType);
        mm.return_(indy.invoke(String.class, name, null, mm.param(0), rowVar(mm)));
    }

    public static CallSite indyColumnMethodName(Lookup lookup, String name, MethodType type,
                                                Class<?> rowType)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var nameVar = mm.param(0); // nameSwitch will access it
        // var rowVar = mm.param(1); // nameSwitch might access it

        new RowMethodsMaker(mm.classMaker(), rowType)
            .nameSwitch(lookup, type, mm, ci -> mm.return_(ci.name));

        return new ConstantCallSite(mm.finish());
    }

    private void addGetSetMethods(Class<?> type, int typeCode) {
        addGetMethod(type, typeCode);
        addSetMethod(type, typeCode);
    }

    private void addGetMethod(Class<?> type, int typeCode) {
        String name = "get";

        if (type != Object.class) {
            String suffix;

            Class<?> comp;
            if (type.isPrimitive()) {
                suffix = '_' + type.getName();
            } else if ((comp = type.componentType()) != null && comp.isPrimitive()) {
                suffix = '_' + comp.getName() + '_' + "array";
            } else {
                suffix = type.getSimpleName();
            }

            name += suffix;
        }

        MethodMaker mm = addPublicMethod(type, name, String.class);
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indyGet", mRowType, typeCode);
        mm.return_(indy.invoke(type, name, null, mm.param(0), rowVar(mm)));
    }

    public static CallSite indyGet(Lookup lookup, String name, MethodType type,
                                   Class<?> rowType, int returnTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var nameVar = mm.param(0); // nameSwitch will access it
        var rowVar = mm.param(1);

        Class<?> returnType = type.returnType();
        var returnVar = mm.var(returnType);

        var returnInfo = new ColumnInfo();
        returnInfo.type = returnType;
        returnInfo.typeCode = returnTypeCode;

        new RowMethodsMaker(mm.classMaker(), rowType).nameSwitch(lookup, type, mm, columnInfo -> {
            var columnVar = rowVar.invoke(columnInfo.name);

            if (returnType != Object.class) {
                Converter.convertExact(mm, columnInfo.name,
                                       columnInfo, columnVar, returnInfo, returnVar);
            } else toObject: {
                if (columnInfo.isUnsignedInteger() && !columnInfo.isArray()) {
                    switch (columnInfo.plainTypeCode()) {
                    case TYPE_UBYTE:
                        returnVar.set(mm.var(Short.class)
                                      .set(columnVar.cast(short.class).and(0xff)));
                        break toObject;
                    case TYPE_USHORT:
                        returnVar.set(mm.var(Integer.class)
                                      .set(columnVar.cast(int.class).and(0xffff)));
                        break toObject;
                    case TYPE_UINT:
                        returnVar.set(mm.var(Long.class)
                                      .set(columnVar.cast(long.class).and(0xffff_ffffL)));
                        break toObject;
                    case TYPE_ULONG:
                        returnVar.set(mm.var(ConvertUtils.class).invoke
                                      ("unsignedLongToBigIntegerExact", columnVar));
                        break toObject;
                    }
                }

                returnVar.set(columnVar);
            }

            mm.return_(returnVar);
        });

        return new ConstantCallSite(mm.finish());
    }

    private void addSetMethod(Class<?> type, int typeCode) {
        String name = "set";
        MethodMaker mm = addPublicMethod(null, name, String.class, type);
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indySet", mRowType, typeCode);
        indy.invoke(null, name, null, mm.param(0), rowVar(mm), mm.param(1));
    }

    public static CallSite indySet(Lookup lookup, String name, MethodType type,
                                   Class<?> rowType, int valueTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var nameVar = mm.param(0); // nameSwitch will access it
        var rowVar = mm.param(1);
        var valueVar = mm.param(2);

        Class<?> valueType = valueVar.classType();
        var valueInfo = new ColumnInfo();
        valueInfo.type = valueType;
        valueInfo.typeCode = valueTypeCode;

        var rmmVar = mm.var(RowMethodsMaker.class);

        new RowMethodsMaker(mm.classMaker(), rowType).nameSwitch(lookup, type, mm, columnInfo -> {
            boolean noNullCheck = false;

            if (!columnInfo.isNullable() && !valueInfo.type.isPrimitive()) {
                Label notNull = mm.label();
                valueVar.ifNe(null, notNull);
                rmmVar.invoke("notNull", columnInfo.name).throw_();
                notNull.here();

                if (valueType != Object.class) {
                    // The Converter doesn't need to check null again.
                    noNullCheck = true;
                }
            }

            Variable columnVar;

            if (valueType != Object.class) {
                ColumnInfo srcValueInfo = valueInfo;
                if (noNullCheck) {
                    srcValueInfo = srcValueInfo.copy();
                    srcValueInfo.typeCode &= ~TYPE_NULLABLE;
                }
                columnVar = mm.var(columnInfo.type);
                Converter.convertExact(mm, columnInfo.name,
                                       srcValueInfo, valueVar, columnInfo, columnVar);
            } else {
                columnVar = ConvertCallSite.make(mm, columnInfo.type, valueVar);
            }

            rowVar.invoke(columnInfo.name, columnVar);
            mm.return_();
        });

        return new ConstantCallSite(mm.finish());
    }

    // Called by generated code.
    public static PathSplitter splitter(Lookup lookup, String name, Class type, Class rowType) {
        return new PathSplitter(rowType);
    }

    // Called by generated code.
    public static IllegalArgumentException notFound(String name) {
        return new IllegalArgumentException("Column name isn't found: " + name);
    }

    // Called by generated code.
    public static ConversionException notNull(String columnName) {
        return new ConversionException("Column cannot be set to null: " + columnName);
    }

    // Called by generated code.
    public static ConversionException nullJoin(String path, String tail) {
        String head = path.substring(0, path.length() - tail.length() - 1);
        return new ConversionException("Column path joins to a null row: " + head);
    }
}
