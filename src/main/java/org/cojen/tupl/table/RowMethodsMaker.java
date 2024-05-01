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

import java.util.function.Consumer;

import org.cojen.maker.Bootstrap;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.ConversionException;

import org.cojen.tupl.table.codec.ColumnCodec;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Makes methods to implement the Row interface.
 *
 * @author Brian S. O'Neill
 */
public final class RowMethodsMaker {
    private final ClassMaker mClassMaker;
    private final Class<?> mRowType;
    private final ColumnInfo[] mColumns;

    RowMethodsMaker(ClassMaker cm, Class<?> rowType, RowGen rowGen) {
        mClassMaker = cm;
        mRowType = rowType;

        RowInfo rowInfo = rowGen.info;
        var columns = new ColumnInfo[rowInfo.allColumns.size()];

        int i = 0;
        for (ColumnInfo info : rowInfo.keyColumns.values()) {
            columns[i++] = info;
        }
        for (ColumnCodec codec : rowGen.valueCodecs()) { // use encoding order
            columns[i++] = codec.info;
        }

        assert i == columns.length;

        mColumns = columns;
    }

    private RowMethodsMaker(ClassMaker cm, Class<?> rowType) {
        this(cm, rowType, RowInfo.find(rowType).rowGen());
    }

    void addMethods() {
        addColumnCount();
        addColumnNumber("columnNumber");
        addColumnNumber("findColumnNumber");
        addColumnName();
        addColumnMethodName();
        addColumnType();

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

    private void addColumnCount() {
        MethodMaker mm = mClassMaker.addMethod(int.class, "columnCount").public_();
        mm.return_(mColumns.length);
    }

    private void addColumnNumber(String name) {
        MethodMaker mm = mClassMaker.addMethod(int.class, name, String.class).public_();

        var cases = new String[mColumns.length];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = mColumns[i].name;
            labels[i] = mm.label();
        }

        Label notFound = mm.label();

        mm.param(0).switch_(notFound, cases, labels);

        for (int i=0; i<cases.length; i++) {
            labels[i].here();
            mm.return_(i + 1);
        }

        notFound.here();

        if (name == "columnNumber") {
            mm.var(RowMethodsMaker.class).invoke("notFound", mm.param(0)).throw_();
        } else {
            mm.return_(0);
        }
    }

    private void numSwitch(MethodMaker mm, Consumer<ColumnInfo> caseBody) {
        var cases = new int[mColumns.length];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i + 1;
            labels[i] = mm.label();
        }

        Label notFound = mm.label();

        mm.param(0).switch_(notFound, cases, labels);

        for (int i=0; i<cases.length; i++) {
            labels[i].here();
            caseBody.accept(mColumns[i]);
        }

        notFound.here();

        mm.var(RowMethodsMaker.class).invoke("notFound", mm.param(0)).throw_();
    }

    private void addColumnName() {
        MethodMaker mm = mClassMaker.addMethod(String.class, "columnName", int.class).public_();
        numSwitch(mm, ci -> mm.return_(ci.name));
    }

    private void addColumnMethodName() {
        // FIXME: Only if @Name annotation is used anywhere and it actually differs.
    }

    private void addColumnType() {
        MethodMaker mm = mClassMaker.addMethod(Class.class, "columnType", int.class).public_();
        numSwitch(mm, ci -> mm.return_(ci.type));
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

        MethodMaker mm = mClassMaker.addMethod(type, name, int.class).public_();
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indyGet", mRowType, typeCode);
        mm.return_(indy.invoke(type, "_", null, mm.param(0), mm.this_()));
    }

    public static CallSite indyGet(Lookup lookup, String name, MethodType type,
                                   Class<?> rowType, int returnTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var numVar = mm.param(0); // numSwitch will access it
        var rowVar = mm.param(1);

        Class<?> returnType = type.returnType();
        var returnVar = mm.var(returnType);

        var returnInfo = new ColumnInfo();
        returnInfo.type = returnType;
        returnInfo.typeCode = returnTypeCode;

        new RowMethodsMaker(mm.classMaker(), rowType).numSwitch(mm, columnInfo -> {
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
        MethodMaker mm = mClassMaker.addMethod(null, "set", int.class, type).public_();
        Bootstrap indy = mm.var(RowMethodsMaker.class).indy("indySet", mRowType, typeCode);
        indy.invoke(null, "_", null, mm.param(0), mm.param(1), mm.this_());
    }

    public static CallSite indySet(Lookup lookup, String name, MethodType type,
                                   Class<?> rowType, int valueTypeCode)
    {
        MethodMaker mm = MethodMaker.begin(lookup, name, type);
        //var numVar = mm.param(0); // numSwitch will access it
        var valueVar = mm.param(1);
        var rowVar = mm.param(2);

        Class<?> valueType = valueVar.classType();
        var valueInfo = new ColumnInfo();
        valueInfo.type = valueType;
        valueInfo.typeCode = valueTypeCode;

        var rmmVar = mm.var(RowMethodsMaker.class);

        new RowMethodsMaker(mm.classMaker(), rowType).numSwitch(mm, columnInfo -> {
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
    public static IndexOutOfBoundsException notFound(int number) {
        return new IndexOutOfBoundsException("Column number is out of bounds: " + number);
    }

    // Called by generated code.
    public static IllegalArgumentException notFound(String name) {
        return new IllegalArgumentException("Column name isn't found: " + name);
    }

    // Called by generated code.
    public static ConversionException notNull(String columnName) {
        return new ConversionException("Column cannot be set to null: " + columnName);
    }
}
