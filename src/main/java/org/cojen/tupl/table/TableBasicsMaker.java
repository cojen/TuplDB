/*
 *  Copyright (C) 2022 Cojen.org
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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Map;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.Table;

/**
 * Makes a Table interface which implements basic methods like "rowType" which can be inherited
 * by a primary table class and all of its secondary index table classes.
 *
 * @author Brian S O'Neill
 */
public class TableBasicsMaker {
    // Maps rowType interface classes to generated interface classes.
    private static final WeakClassCache<Class<?>> cCache = new WeakClassCache<>();

    /**
     * Returns an interface with a few default methods.
     */
    public static Class<?> find(Class<?> rowType) {
        Class clazz = cCache.get(rowType);

        if (clazz == null) {
            synchronized (cCache) {
                clazz = cCache.get(rowType);
                if (clazz == null) {
                    clazz = make(rowType, RowInfo.find(rowType).rowGen());
                    cCache.put(rowType, clazz);
                }
            }
        }

        return clazz;
    }

    private static Class<?> make(Class<?> rowType, RowGen rowGen) {
        Class<?> rowClass = RowMaker.find(rowType);

        ClassMaker cm = rowGen.beginClassMaker(TableBasicsMaker.class, rowType, "basics")
            .interface_().implement(Table.class);

        // Add the simple rowType method.
        cm.addMethod(Class.class, "rowType").public_().return_(rowType);

        // Add the newRow method and its bridge.
        {
            MethodMaker mm = cm.addMethod(rowType, "newRow").public_();
            mm.return_(mm.new_(rowClass));
            mm = cm.addMethod(Object.class, "newRow").public_().bridge();
            mm.return_(mm.this_().invoke(rowType, "newRow", null));
        }

        // Add the cloneRow method and its bridge.
        {
            MethodMaker mm = cm.addMethod(rowType, "cloneRow", Object.class).public_();
            mm.return_(mm.param(0).cast(rowClass).invoke("clone"));
            mm = cm.addMethod(Object.class, "cloneRow", Object.class).public_().bridge();
            mm.return_(mm.this_().invoke(rowType, "cloneRow", null, mm.param(0)));
        }

        // Add the unsetRow method.
        {
            MethodMaker mm = cm.addMethod(null, "unsetRow", Object.class).public_();
            Variable rowVar = mm.param(0).cast(rowClass);

            // Clear the column fields that refer to objects.
            for (ColumnInfo info : rowGen.info.allColumns.values()) {
                if (!info.type.isPrimitive()) {
                    rowVar.field(info.name).set(null);
                }
            }

            // Clear the column state fields.
            for (String name : rowGen.stateFields()) {
                rowVar.field(name).set(0);
            }
        }

        // Add the cleanRow method.
        {
            MethodMaker mm = cm.addMethod(null, "cleanRow", Object.class).public_();
            TableMaker.markAllUndirty(mm.param(0).cast(rowClass), rowGen.info);
        }

        // Add the copyRow method.
        {
            MethodMaker mm = cm.addMethod(null, "copyRow", Object.class, Object.class).public_();
            Variable srcRowVar = mm.param(0).cast(rowClass);
            Variable dstRowVar = mm.param(1).cast(rowClass);

            // Copy the column fields.
            for (ColumnInfo info : rowGen.info.allColumns.values()) {
                dstRowVar.field(info.name).set(srcRowVar.field(info.name));
            }

            // Copy the column state fields.
            for (String name : rowGen.stateFields()) {
                dstRowVar.field(name).set(srcRowVar.field(name));
            }
        }

        // Add the isSet method.
        {
            MethodMaker mm = cm.addMethod
                (boolean.class, "isSet", Object.class, String.class).public_();
            var indy = mm.var(TableBasicsMaker.class).indy("indyIsSet", rowType);
            mm.return_(indy.invoke(boolean.class, "isSet", null, mm.param(0), mm.param(1)));
        }

        // Add the forEach method.
        {
            MethodMaker mm = cm.addMethod
                (null, "forEach", Object.class, ColumnProcessor.class).public_();
            var indy = mm.var(TableBasicsMaker.class).indy("indyForEach", rowType);
            indy.invoke(null, "forEach", null, mm.param(0), mm.param(1));
        }

        return cm.finish();
    }

    public static CallSite indyIsSet(MethodHandles.Lookup lookup, String name, MethodType mt,
                                     Class<?> rowType)
    {
        RowInfo rowInfo = RowInfo.find(rowType);
        Class<?> rowClass = RowMaker.find(rowType);

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rowVar = mm.param(0).cast(rowClass);
        var colName = mm.param(1);

        String[] cases = rowInfo.allColumns.keySet().toArray(String[]::new);
        var labels = new Label[cases.length];

        for (int i=0; i<labels.length; i++) {
            labels[i] = mm.label();
        }

        var notFound = mm.label();
        colName.switch_(notFound, cases, labels);

        RowGen rowGen = rowInfo.rowGen();
        Map<String, Integer> colNums = rowGen.columnNumbers();

        final Variable stateVar = mm.var(int.class);
        final Variable stateMask = mm.var(int.class);

        Label check = mm.label();

        for (int i=0; i<cases.length; i++) {
            labels[i].here();
            int colNum = colNums.get(cases[i]);
            stateVar.set(rowVar.field(rowGen.stateField(colNum)));
            stateMask.set(RowGen.stateFieldMask(colNum));
            check.goto_();
        }

        check.here();
        mm.return_(stateVar.and(stateMask).ne(0));

        notFound.here();
        mm.new_(IllegalArgumentException.class, mm.concat("Unknown column: ", colName)).throw_();

        return new ConstantCallSite(mm.finish());
    }

    public static CallSite indyForEach(MethodHandles.Lookup lookup, String name, MethodType mt,
                                       Class<?> rowType)
    {
        RowInfo rowInfo = RowInfo.find(rowType);
        RowGen rowGen = rowInfo.rowGen();
        Class<?> rowClass = RowMaker.find(rowType);
        Map<String, Integer> colNums = rowGen.columnNumbers();

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rowVar = mm.param(0).cast(rowClass);
        var consumerVar = mm.param(1);

        final var stateVar = mm.var(int.class);
        int stateNum = -1;

        for (Map.Entry<String, Integer> e : colNums.entrySet()) {
            String colName = e.getKey();
            int colNum = e.getValue();

            int sn = RowGen.stateFieldNum(colNum);
            if (sn != stateNum) {
                stateVar.set(rowVar.field(rowGen.stateField(colNum)));
                stateNum = sn;
            }

            Label next = mm.label();
            stateVar.and(RowGen.stateFieldMask(colNum)).ifEq(0, next);

            String realName = RowMethodsMaker.unescape(colName);
            consumerVar.invoke("accept", rowVar, realName, rowVar.field(colName));

            next.here();
        }

        return new ConstantCallSite(mm.finish());
    }
}
