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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashMap;
import java.util.Map;

import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.ConvertCallSite;
import org.cojen.tupl.table.Converter;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.WeakCache;

import org.cojen.tupl.table.expr.Parser;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class JoinPredicateMaker implements Visitor {
    private static final WeakCache<TupleKey, Class<?>, RowFilter> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public Class<?> newValue(TupleKey key, RowFilter filter) {
                var joinType = (Class<?>) key.get(0);
                String queryStr = key.getString(1);
                var maker = new JoinPredicateMaker(joinType, queryStr, filter);
                RowFilter canonical = maker.mFilter;
                String canonicalStr = canonical.toString();
                if (queryStr.equals(canonicalStr)) {
                    return maker.finish();
                } else {
                    return obtain(TupleKey.make.with(joinType, canonicalStr), canonical);
                }
            }
        };
    }

    /**
     * @see Table#predicate
     */
    @SuppressWarnings("unchecked")
    public static <J> Predicate<J> newInstance(Class<J> joinType, String queryStr, Object... args) {
        try {
            return (Predicate<J>) find(joinType, queryStr, null).invoke(args);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Returns a predicate constructor which accepts an Object[] of arguments.
     */
    static MethodHandle find(Class<?> joinType, RowFilter filter) {
        return find(joinType, filter.toString(), filter);
    }

    /**
     * Returns a predicate constructor which accepts an Object[] of arguments.
     *
     * @param filter if is null, the filter might be parsed from queryStr
     */
    private static MethodHandle find(Class<?> joinType, String queryStr, RowFilter filter) {
        Class<?> clazz = cCache.obtain(TupleKey.make.with(joinType, queryStr), filter);
        MethodType mt = MethodType.methodType(void.class, Object[].class);
        try {
            return MethodHandles.lookup().findConstructor(clazz, mt);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private final Class<?> mJoinType;
    private final RowInfo mJoinInfo;
    private final RowFilter mFilter;

    private MethodMaker mCtorMaker, mTestMaker;
    private Variable mRowVar;
    private Map<TupleKey, String> mArgFieldMap;
    private Label mPass, mFail;

    JoinPredicateMaker(Class<?> joinType, RowFilter filter) {
        mJoinType = joinType;
        mJoinInfo = RowInfo.find(joinType);
        mFilter = filter;
    }

    /**
     * @param filter if is null, the filter is parsed from queryStr
     */
    private JoinPredicateMaker(Class<?> joinType, String queryStr, RowFilter filter) {
        mJoinType = joinType;
        mJoinInfo = RowInfo.find(joinType);

        if (filter == null) {
            filter = Parser.parseQuerySpec(joinType, queryStr).filter();
        }

        mFilter = filter;
    }

    /**
     * Generates predicate code into the given testMaker method. The generated code might need
     * to define and assign argument conversion fields, which is why ctorMaker is needed.
     *
     * @param ctorMaker constructor; last param must be an Object[] of arguments
     * @param testMaker predicate test method to write code into; must return a boolean
     * @param rowVar row parameter available to the test method
     */
    void generate(MethodMaker ctorMaker, MethodMaker testMaker, Variable rowVar) {
        mCtorMaker = ctorMaker;
        mTestMaker = testMaker;
        mRowVar = rowVar;

        mArgFieldMap = null;

        mPass = mTestMaker.label();
        mFail = mTestMaker.label();

        mFilter.accept(this);

        mFail.here();
        mTestMaker.return_(false);
        mPass.here();
        mTestMaker.return_(true);
    }

    Class<?> finish() {
        ClassMaker cm = RowGen.beginClassMaker
            (JoinPredicateMaker.class, mJoinType, mJoinInfo.name, null, null)
            .implement(Predicate.class).public_().final_();

        MethodMaker ctorMaker = cm.addConstructor(Object[].class).public_().varargs();
        ctorMaker.invokeSuperConstructor();

        MethodMaker testMaker = cm.addMethod(boolean.class, "test", Object.class).public_();

        generate(ctorMaker, testMaker, testMaker.param(0).cast(mJoinType));

        // FIXME: Add a toString method. See RowPredicateMaker.

        // Define as a hidden class to facilitate unloading.
        return cm.finishHidden().lookupClass();
    }

    @Override
    public void visit(OrFilter filter) {
        final Label originalFail = mFail;
        RowFilter[] subFilters = filter.subFilters();
        for (RowFilter subFilter : subFilters) {
            mFail = mTestMaker.label();
            subFilter.accept(this);
            mFail.here();
        }
        mTestMaker.goto_(originalFail);
        mFail = originalFail;
    }

    @Override
    public void visit(AndFilter filter) {
        final Label originalPass = mPass;
        RowFilter[] subFilters = filter.subFilters();
        for (RowFilter subFilter : subFilters) {
            mPass = mTestMaker.label();
            subFilter.accept(this);
            mPass.here();
        }
        mTestMaker.goto_(originalPass);
        mPass = originalPass;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        ColumnInfo ci = filter.column();

        if (filter.isIn(filter.operator())) {
            // FIXME: Sort and use binary search if large enough. Be sure to clone array if
            // it wasn't converted.
            // FIXME: Support other types of 'in' arguments: Predicate, IntPredicate, etc.
            ci = ci.asArray(true);
        } else if (!ci.isNullable()) {
            // Arguments can always be null at runtime.
            ci = ci.copy();
            ci.typeCode |= ColumnInfo.TYPE_NULLABLE;
            ci.assignType();
        }

        Class<?> argType = ci.type;

        var fieldKey = TupleKey.make.with(filter.argument(), argType);

        if (mArgFieldMap == null) {
            mArgFieldMap = new HashMap<>();
        }

        String fieldName = mArgFieldMap.get(fieldKey);

        if (fieldName == null) {
            fieldName = "f" + mArgFieldMap.size();
            mArgFieldMap.put(fieldKey, fieldName);
            mCtorMaker.classMaker().addField(argType, fieldName).private_().final_();
            var argsVar = mCtorMaker.param(mCtorMaker.paramCount() - 1);
            Variable argVar = argsVar.aget(filter.argument() - 1);
            mCtorMaker.field(fieldName).set(ConvertCallSite.make(mCtorMaker, argType, argVar));
        }

        var colVar = columnValue(ci);
        var argVar = mTestMaker.field(fieldName);

        CompareUtils.compare(mTestMaker, ci, colVar, ci, argVar, filter.operator(), mPass, mFail);
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        ColumnInfo c1 = filter.column();
        var c1Var = columnValue(c1);
        ColumnInfo c2 = filter.otherColumn();
        var c2Var = columnValue(c2);

        if (c1Var.classType() != c2Var.classType()) {
            ColumnInfo ci = filter.common();

            var c1ConvertedVar = mTestMaker.var(ci.type);
            Converter.convertExact(mTestMaker, null, c1, c1Var, ci, c1ConvertedVar);
            c1 = ci;
            c1Var = c1ConvertedVar;

            var c2ConvertedVar = mTestMaker.var(ci.type);
            Converter.convertExact(mTestMaker, null, c2, c2Var, ci, c2ConvertedVar);
            c2 = ci;
            c2Var = c2ConvertedVar;
        }

        CompareUtils.compare(mTestMaker, c1, c1Var, c2, c2Var, filter.operator(), mPass, mFail);
    }

    /**
     * Generates code which follows a path to obtain the column value, branching to the fail
     * label if any path component is null.
     */
    private Variable columnValue(ColumnInfo ci) {
        Variable rowVar = mRowVar;
        while (true) {
            String prefix = ci.prefix();
            if (prefix == null) {
                return rowVar.invoke(ci.name);
            }
            rowVar = rowVar.invoke(prefix);
            rowVar.ifEq(null, mFail);
            ci = ci.tail();
        }
    }
}
