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

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.cojen.tupl.Mapper;
import org.cojen.tupl.QueryException;
import org.cojen.tupl.Table;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.ViewedTable;
import org.cojen.tupl.table.WeakCache;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a QueryExpr which relies on a Mapper to perform custom transformations and
 * filtering.
 *
 * @author Brian S. O'Neill
 * @see QueryExpr#make
 */
final class MappedQueryExpr extends QueryExpr {
    /**
     * @param filter can be null if rowFilter is TrueFilter
     * @param projection can be null to project all columns
     * @param orderBy optional view ordering to apply after calling Table.map
     * @see QueryExpr#make
     */
    static MappedQueryExpr make(int startPos, int endPos, RelationType type,
                                RelationExpr from, RowFilter rowFilter, Expr filter,
                                List<ProjExpr> projection, int maxArgument, String orderBy)
    {
        List<ProjExpr> effectiveProjection;

        if (projection == null) {
            effectiveProjection = from.fullProjection();
        } else {
            effectiveProjection = projection;
            if (from.rowType().isFullProjection(projection)) {
                projection = null;
            }
        }

        return new MappedQueryExpr(startPos, endPos, type, from, rowFilter, filter,
                                   projection, effectiveProjection, maxArgument, orderBy);
    }

    private final Expr mFilter;
    private final String mOrderBy;

    private final List<ProjExpr> mEffectiveProjection; // not null

    private MappedQueryExpr(int startPos, int endPos, RelationType type,
                            RelationExpr from, RowFilter rowFilter, Expr filter,
                            List<ProjExpr> projection, List<ProjExpr> effectiveProjection,
                            int maxArgument, String orderBy)
    {
        super(startPos, endPos, type, from, rowFilter, projection, maxArgument);

        mFilter = filter;
        mOrderBy = orderBy;

        mEffectiveProjection = effectiveProjection;
    }

    @Override
    public boolean isPureFunction() {
        return super.isPureFunction() && (mFilter == null || mFilter.isPureFunction());
    }

    @Override
    public boolean isOrderDependent() {
        return super.isOrderDependent() || (mFilter != null && mFilter.isOrderDependent());
    }

    @Override
    public boolean isGrouping() {
        return super.isGrouping() || (mFilter != null && mFilter.isGrouping());
    }

    @Override
    public boolean isAccumulating() {
        return super.isAccumulating() || (mFilter != null && mFilter.isAccumulating());
    }

    @Override
    public boolean isAggregating() {
        return super.isAggregating() || (mFilter != null && mFilter.isAggregating());
    }

    @Override
    public QuerySpec querySpec() {
        if (mRowFilter != TrueFilter.THE) {
            throw new QueryException("Query performs custom filtering");
        }
        QuerySpec fromSpec = mFrom.querySpec();
        QuerySpec thisSpec = querySpec(true);
        if (thisSpec == null) {
            throw new QueryException("Query derives new columns");
        }
        return thisSpec.withFilter(fromSpec.filter());
    }

    @Override
    public QuerySpec tryQuerySpec(Class<?> rowType) {
        if (mFrom.rowTypeClass() != rowType || mRowFilter != TrueFilter.THE) { 
            return null;
        }
        QuerySpec fromSpec = mFrom.tryQuerySpec(rowType);
        if (fromSpec == null) {
            return null;
        }
        QuerySpec thisSpec = querySpec(true);
        if (thisSpec == null) {
            return null;
        }
        return thisSpec.withFilter(fromSpec.filter());
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            super.doEncodeKey(enc);
        }
    }

    private static final WeakCache<Object, QueryMapper, MappedQueryExpr> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public QueryMapper newValue(Object key, MappedQueryExpr expr) {
                return expr.makeMapper();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompiledQuery makeCompiledQuery() throws IOException {
        CompiledQuery source = mFrom.makeCompiledQuery();

        QueryMapper factory = cCache.obtain(makeKey(), this);

        Class targetClass = rowTypeClass();

        int argCount = maxArgument();

        if (argCount == 0) {
            Table table = source.table().map(targetClass, factory.mapperFor(RowUtils.NO_ARGS));
            if (mOrderBy != null) {
                table = ViewedTable.view(table, mOrderBy);
            }
            return CompiledQuery.make(table);
        }

        if (mOrderBy == null) {
            return new NoView(source, argCount, targetClass, factory);
        }

        return new WithView(source, argCount, targetClass, factory, mOrderBy);
    }

    @SuppressWarnings("unchecked")
    private static sealed class NoView extends CompiledQuery.Wrapped {
        private final Class mTargetClass;
        private final QueryMapper mFactory;

        NoView(CompiledQuery source, int argCount, Class targetClass, QueryMapper factory) {
            super(source, argCount);
            mTargetClass = targetClass;
            mFactory = factory;
        }

        @Override
        public Class rowType() {
            return mTargetClass;
        }

        @Override
        public Table table(Object... args) throws IOException {
            checkArgumentCount(args);
            return source.table(args).map(mTargetClass, mFactory.mapperFor(args));
        }
    }

    private static final class WithView extends NoView {
        private final String mViewStr;

        WithView(CompiledQuery source, int argCount, Class targetClass, QueryMapper factory,
                 String viewStr)
        {
            super(source, argCount, targetClass, factory);
            mViewStr = viewStr;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Table table(Object... args) throws IOException {
            return ViewedTable.view(super.table(args), mViewStr);
        }
    }

    private QueryMapper makeMapper() {
        Class<?> targetClass = rowTypeClass();

        Class<?> baseClass = mRowFilter == TrueFilter.THE
            ? QueryMapper.Unfiltered.class : QueryMapper.class;

        ClassMaker cm = RowGen.beginClassMaker
            (MappedQueryExpr.class, targetClass, targetClass.getName(), null, "mapper")
            .extend(baseClass).final_();

        // Keep a reference to the factory instance, to prevent it from being garbage collected
        // as long as the generated class still exists.
        cm.addField(Object.class, "_").private_().static_();

        {
            MethodMaker ctor = cm.addConstructor().private_();
            ctor.invokeSuperConstructor();
            ctor.field("_").set(ctor.this_());
        }

        int argCount = maxArgument();

        // Implement the factory method.
        {
            MethodMaker mm = cm.addMethod(Mapper.class, "mapperFor", Object[].class).public_();

            if (argCount == 0) {
                // Just return a singleton.
                mm.return_(mm.this_());
            } else {
                cm.addField(Object[].class, "args").private_().final_();

                MethodMaker ctor = cm.addConstructor(Object[].class).private_();
                ctor.invokeSuperConstructor();
                ctor.field("args").set(ctor.param(0));

                mm.return_(mm.new_(cm, mm.param(0)));
            }
        }

        Set<Column> evalColumns = gatherEvalColumns();

        addMapMethod(cm, evalColumns, argCount);

        addInverseMappingFunctions(cm, mEffectiveProjection);

        addPlanMethod(cm);

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (QueryMapper) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private Set<Column> gatherEvalColumns() {
        var evalColumns = new HashSet<Column>();

        if (mFilter != null) {
            mFilter.gatherEvalColumns(evalColumns);
        }

        for (Expr expr : mEffectiveProjection) {
            expr.gatherEvalColumns(evalColumns);
        }

        return evalColumns;
    }

    private void addMapMethod(ClassMaker cm, Set<Column> evalColumns, int argCount) {
        TupleType targetType = rowType();

        MethodMaker mm = cm.addMethod
            (targetType.clazz(), "map", Object.class, Object.class).public_();

        Variable sourceRow;
        if (evalColumns.isEmpty()) {
            // No need to cast the sourceRow because it won't be used.
            sourceRow = mm.param(0);
        } else {
            sourceRow = mm.param(0).cast(mFrom.rowTypeClass());
        }

        var targetRow = mm.param(1).cast(targetType.clazz());

        var argsVar = argCount == 0 ? null : mm.field("args").get();
        var context = new EvalContext(argsVar, sourceRow);

        // Eagerly evaluate AssignExprs. The result might be needed by downstream expressions,
        // the filter, or it might throw an exception. In the unlikely case that none of these
        // conditions are met, then the AssignExprs can be evaluated after filtering.
        // TODO: This is too eager -- it's not checking canThrowRuntimeException, and it's not
        // checking if any downstream expressions exist.
        IdentityHashMap<ProjExpr, Variable> projectedVars = null;
        if (mProjection != null) {
            for (ProjExpr pe : mProjection) {
                if (pe.wrapped() instanceof AssignExpr) {
                    Variable v = pe.makeEval(context);
                    if (projectedVars == null) {
                        projectedVars = new IdentityHashMap<>();
                    }
                    projectedVars.put(pe, v);
                }
            }
        }

        if (mRowFilter != TrueFilter.THE) {
            Label pass = mm.label();
            Label fail = mm.label();
            mFilter.makeFilter(context, pass, fail);
            fail.here();
            mm.return_(null);
            pass.here();
        }

        for (ProjExpr pe : mEffectiveProjection) {
            pe.makeSetColumn(context, projectedVars, targetType, targetRow);
        }

        mm.return_(targetRow);

        // Now implement the bridge method.
        MethodMaker bridge = cm.addMethod
            (Object.class, "map", Object.class, Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(targetType.clazz(), "map",
                                             null, bridge.param(0), bridge.param(1)));
    }

    private void addPlanMethod(ClassMaker cm) {
        if (mRowFilter == TrueFilter.THE) {
            return;
        }
        MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", QueryPlan.Mapper.class).public_();
        mm.return_(mm.new_(QueryPlan.Filter.class, mRowFilter.toString(), mm.param(0)));
    }
}
