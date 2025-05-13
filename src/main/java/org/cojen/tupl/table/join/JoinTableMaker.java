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

package org.cojen.tupl.table.join;

import java.io.IOException;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Map;
import java.util.TreeMap;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.Database;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.Row;
import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowMethodsMaker;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.WeakCache;
import org.cojen.tupl.table.WeakClassCache;

import org.cojen.tupl.util.Canonicalizer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class JoinTableMaker {
    private static final WeakClassCache<Class<?>> cClassCache;
    private static final Canonicalizer cInstanceCache;
    private static final WeakCache<TupleKey, Class<Row>, Map<String, ColumnInfo>> cTypeCache;

    static {
        cClassCache = new WeakClassCache<>() {
            @Override
            protected Class<?> newValue(Class<?> joinType, Object unused) {
                return new JoinTableMaker(joinType).finish();
            }
        };

        cInstanceCache = new Canonicalizer();

        cTypeCache = new WeakCache<>() {
            @Override
            protected Class<Row> newValue(TupleKey key, Map<String, ColumnInfo> columns) {
                return makeJoinTypeClass(columns);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static Class<Row> makeJoinTypeClass(Map<String, ColumnInfo> columns) {
        ClassMaker cm = RowGen.beginClassMakerForRowType
            (JoinTableMaker.class.getPackageName(), "Join");
        cm.implement(Row.class);
        cm.sourceFile(JoinTableMaker.class.getSimpleName());

        for (ColumnInfo ci : columns.values()) {
            MethodMaker mm = cm.addMethod(ci.type, ci.name).public_().abstract_();

            if (ci.isNullable()) {
                mm.addAnnotation(Nullable.class, true);
            }

            cm.addMethod(null, ci.name, ci.type).public_().abstract_();
        }

        return (Class<Row>) cm.finish();
    }

    /**
     * @see Table#join
     */
    public static <J> JoinTable<J> join(Class<J> joinType, String specStr, Table<?>... tables) {
        JoinSpec spec = JoinSpec.parse(RowInfo.find(joinType), specStr, tables);
        return join(joinType, spec);
    }

    /**
     * @see Table#join
     */
    public static JoinTable<Row> join(String specStr, Table<?>... tables) {
        var definedColumns = new TreeMap<String, ColumnInfo>();
        JoinSpec spec = JoinSpec.parse(specStr, definedColumns, tables);

        TupleKey cacheKey;
        {
            var pairs = new Object[definedColumns.size() * 2];
            int i = 0;
            for (ColumnInfo column : definedColumns.values()) {
                String name = column.name.intern();
                if (column.isNullable()) {
                    pairs[i++] = name;
                    pairs[i++] = column.type;
                } else {
                    pairs[i++] = column.type;
                    pairs[i++] = name;
                }
            }
            assert i == pairs.length;
            cacheKey = TupleKey.make.with(pairs);
        }

        return join(cTypeCache.obtain(cacheKey, definedColumns), spec);
    }

    /**
     * @see Database#openJoinTable
     */
    public static <J> JoinTable<J> join(Class<J> joinType, String specStr, Database db)
        throws IOException
    {
        RowInfo joinInfo = RowInfo.find(joinType);
        JoinSpec spec = JoinSpec.parse(joinInfo, specStr, db);
        return join(joinType, spec);
    }

    @SuppressWarnings("unchecked")
    static <J> JoinTable<J> join(Class<J> joinType, JoinSpec spec) {
        Class<?> clazz = findClass(joinType);

        spec = spec.toLeftJoin();

        var tables = new Table[spec.numSources()];

        spec.root().accept(new JoinSpec.Visitor() {
            int mPos;

            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                tables[mPos++] = node.table();
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                tables[mPos++] = join(joinType, node.toSpec());
                return node;
            }
        });

        String specStr = spec.toString();

        MethodType mt = MethodType.methodType
            (void.class, String.class, JoinSpec.class, Table[].class);

        JoinTable<J> table;
        try {
            table = (JoinTable<J>) MethodHandles.lookup()
                .findConstructor(clazz, mt).invoke(specStr, spec, tables);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }

        return cInstanceCache.apply(table);
    }

    /**
     * Returns a class which is constructed with:
     *
     *   (String specStr, JoinSpec spec, Table... tables)
     */
    private static Class<?> findClass(Class<?> joinType) {
        return cClassCache.obtain(joinType, null);
    }

    private final Class<?> mJoinType;
    private final Class<?> mJoinClass;
    private final RowInfo mJoinInfo;
    private final ClassMaker mClassMaker;

    private JoinTableMaker(Class<?> joinType) {
        mJoinType = joinType;
        mJoinClass = JoinRowMaker.find(joinType);
        mJoinInfo = RowInfo.find(joinType);

        mClassMaker = RowGen.beginClassMaker
            (JoinTableMaker.class, joinType, mJoinInfo.name, null, "table")
            .extend(JoinTable.class).public_();
    }

    private Class<?> finish() {
        addConstructors();

        // Add the simple rowType method.
        mClassMaker.addMethod(Class.class, "rowType").public_().return_(mJoinType);

        // Add the newRow method and its bridge.
        {
            MethodMaker mm = mClassMaker.addMethod(mJoinType, "newRow").public_();
            mm.return_(mm.new_(mJoinClass));
            mm = mClassMaker.addMethod(Object.class, "newRow").public_().bridge();
            mm.return_(mm.this_().invoke(mJoinType, "newRow", null));
        }

        // Add the cloneRow method and its bridge.
        {
            MethodMaker mm = mClassMaker.addMethod(mJoinType, "cloneRow", Object.class).public_();
            mm.return_(mm.param(0).cast(mJoinClass).invoke("clone"));
            mm = mClassMaker.addMethod(Object.class, "cloneRow", Object.class).public_().bridge();
            mm.return_(mm.this_().invoke(mJoinType, "cloneRow", null, mm.param(0)));
        }

        // Add the unsetRow method.
        {
            MethodMaker mm = mClassMaker.addMethod(null, "unsetRow", Object.class).public_();
            var rowVar = mm.param(0).cast(mJoinType);
            for (ColumnInfo info : mJoinInfo.allColumns.values()) {
                rowVar.invoke(info.name, (Object) null);
            }
        }

        // Add the cleanRow method. It doesn't need to do anything because there's no such
        // thing as a dirty column in a join row.
        mClassMaker.addMethod(null, "cleanRow", Object.class).public_();

        // Add the copyRow method.
        {
            MethodMaker mm = mClassMaker.addMethod
                (null, "copyRow", Object.class, Object.class).public_();
            var srcRowVar = mm.param(0).cast(mJoinType);
            var dstRowVar = mm.param(1).cast(mJoinType);
            for (ColumnInfo info : mJoinInfo.allColumns.values()) {
                dstRowVar.invoke(info.name, srcRowVar.invoke(info.name));
            }
        }

        // Add the isSet method.
        {
            MethodMaker mm = mClassMaker.addMethod
                (boolean.class, "isSet", Object.class, String.class).public_();
            var indy = mm.var(JoinTableMaker.class).indy("indyIsSet", mJoinType);
            mm.return_(indy.invoke(boolean.class, "isSet", null, mm.param(0), mm.param(1)));
        }

        // Add the forEach method.
        {
            MethodMaker mm = mClassMaker.addMethod
                (null, "forEach", Object.class, ColumnProcessor.class).public_();
            var indy = mm.var(JoinTableMaker.class).indy("indyForEach", mJoinType);
            indy.invoke(null, "forEach", null, mm.param(0), mm.param(1));
        }

        // Add the withTables method.
        {
            MethodMaker mm = mClassMaker.addMethod(JoinTable.class, "withTables", Table[].class)
                .protected_().override();
            mm.return_(mm.new_(mClassMaker, mm.this_(), mm.param(0)));
        }

        return mClassMaker.finish();
    }

    private void addConstructors() {
        {
            MethodMaker mm = mClassMaker.addConstructor
                (String.class, JoinSpec.class, Table[].class).varargs().public_();

            var specStrVar = mm.param(0);
            var specVar = mm.param(1);
            var tablesVar = mm.param(2);

            mm.invokeSuperConstructor(specStrVar, specVar, tablesVar);
        }

        {
            MethodMaker mm = mClassMaker.addConstructor
                (mClassMaker, Table[].class).varargs().public_();

            var jtVar = mm.param(0);
            var tablesVar = mm.param(1);

            mm.invokeSuperConstructor(jtVar, tablesVar);
        }
    }

    public static CallSite indyIsSet(MethodHandles.Lookup lookup, String name, MethodType mt,
                                     Class<?> rowType)
    {
        RowInfo rowInfo = RowInfo.find(rowType);

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rowVar = mm.param(0).cast(rowType);
        var colName = mm.param(1);

        String[] cases = rowInfo.allColumns.keySet().toArray(String[]::new);
        var labels = new Label[cases.length];

        for (int i=0; i<labels.length; i++) {
            labels[i] = mm.label();
        }

        var notFound = mm.label();
        colName.switch_(notFound, cases, labels);

        Variable valueVar = mm.var(Object.class);
        Label check = mm.label();

        for (int i=0; i<cases.length; i++) {
            labels[i].here();
            valueVar.set(rowVar.invoke(cases[i]));
            check.goto_();
        }

        check.here();
        mm.return_(valueVar.ne(null));

        notFound.here();
        mm.new_(IllegalArgumentException.class, mm.concat("Unknown column: ", colName)).throw_();

        return new ConstantCallSite(mm.finish());
    }

    public static CallSite indyForEach(MethodHandles.Lookup lookup, String name, MethodType mt,
                                       Class<?> rowType)
    {
        RowInfo rowInfo = RowInfo.find(rowType);

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        var rowVar = mm.param(0).cast(rowType);
        var consumerVar = mm.param(1);

        for (String colName : rowInfo.allColumns.keySet()) {
            Label next = mm.label();
            var value = rowVar.invoke(colName);
            value.ifEq(null, next);
            String realName = RowMethodsMaker.unescape(colName);
            consumerVar.invoke("accept", rowVar, realName, value);
            next.here();
        }

        return new ConstantCallSite(mm.finish());
    }
}
