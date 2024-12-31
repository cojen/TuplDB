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

import org.cojen.maker.ClassMaker;
import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Aggregator;
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

/**
 * Defines a QueryExpr which relies on an Aggregator to perform custom transformations and
 * filtering.
 *
 * @author Brian S. O'Neill
 * @see QueryExpr#make
 */
final class AggregatedQueryExpr extends QueryExpr {
    /**
     * @param filter can be null if rowFilter is TrueFilter
     * @param projection not null
     * @param groupBy number of projected columns to group by; must be >= 0
     * @param orderBy optional view ordering to apply after calling Table.aggregate
     * @see QueryExpr#make
     */
    static AggregatedQueryExpr make(int startPos, int endPos, RelationType type,
                                    RelationExpr from, RowFilter rowFilter, Expr filter,
                                    List<ProjExpr> projection, int groupBy,
                                    int maxArgument, String orderBy)
    {
        Set<String> group;
        if (groupBy == 0) {
            group = Set.of();
        } else {
            group = new HashSet<>(groupBy << 1);
            for (int i=0; i<groupBy; i++) {
                ProjExpr proj = projection.get(i);
                group.add(proj.name());
            }
        }

        if (filter != null) {
            filter = filter.asAggregate(group);
        }

        projection = replaceElements(projection, groupBy, (i, proj) -> proj.asAggregate(group));
        
        return new AggregatedQueryExpr(startPos, endPos, type, from, rowFilter, filter,
                                       projection, groupBy, maxArgument, orderBy);
    }

    private final int mNumGroupBy;
    private final Expr mFilter;
    private final String mOrderBy;

    private AggregatedQueryExpr(int startPos, int endPos, RelationType type,
                                RelationExpr from, RowFilter rowFilter, Expr filter,
                                List<ProjExpr> projection, int groupBy,
                                int maxArgument, String orderBy)
    {
        super(startPos, endPos, type, from, rowFilter, projection, maxArgument);

        mNumGroupBy = groupBy;
        mFilter = filter;
        mOrderBy = orderBy;
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
        return true;
    }

    @Override
    public boolean isAccumulating() {
        return true;
    }

    @Override
    public boolean isAggregating() {
        return true;
    }

    @Override
    public QuerySpec querySpec() {
        throw new QueryException("Query performs aggregation");
    }

    @Override
    public QuerySpec tryQuerySpec(Class<?> rowType) {
        return null;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            super.doEncodeKey(enc);
            enc.encodeInt(mNumGroupBy);
        }
    }

    private static final WeakCache<Object, QueryAggregator, AggregatedQueryExpr> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public QueryAggregator newValue(Object key, AggregatedQueryExpr expr) {
                return expr.makeQueryAggregator();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompiledQuery makeCompiledQuery() throws IOException {
        CompiledQuery source = mFrom.makeCompiledQuery();

        QueryAggregator qa = cCache.obtain(makeKey(), this);

        Class targetClass = rowTypeClass();

        int argCount = maxArgument();

        if (argCount == 0) {
            Table table = source.table().aggregate(targetClass, qa.factoryFor(RowUtils.NO_ARGS));
            if (mOrderBy != null) {
                table = ViewedTable.view(table, mOrderBy);
            }
            return CompiledQuery.make(table);
        }

        if (mOrderBy == null) {
            return new NoView(source, argCount, targetClass, qa);
        }

        return new WithView(source, argCount, targetClass, qa, mOrderBy);
    }

    @SuppressWarnings("unchecked")
    private static sealed class NoView extends CompiledQuery.Wrapped {
        private final Class mTargetClass;
        private final QueryAggregator mAggregator;

        NoView(CompiledQuery source, int argCount, Class targetClass, QueryAggregator qa) {
            super(source, argCount);
            mTargetClass = targetClass;
            mAggregator = qa;
        }

        @Override
        public Class rowType() {
            return mTargetClass;
        }

        @Override
        public Table table(Object... args) throws IOException {
            checkArgumentCount(args);
            return source.table(args).aggregate(mTargetClass, mAggregator.factoryFor(args));
        }
    }

    private static final class WithView extends NoView {
        private final String mViewStr;

        WithView(CompiledQuery source, int argCount, Class targetClass, QueryAggregator qa,
                 String viewStr)
        {
            super(source, argCount, targetClass, qa);
            mViewStr = viewStr;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Table table(Object... args) throws IOException {
            return ViewedTable.view(super.table(args), mViewStr);
        }
    }

    private QueryAggregator makeQueryAggregator() {
        Class<?> aggregatorClass = makeAggregatorClass();

        Class<?> targetClass = rowTypeClass();

        ClassMaker cm = RowGen.beginClassMaker
            (AggregatedQueryExpr.class, targetClass, targetClass.getName(), null, "qa")
            .extend(QueryAggregator.class).final_();

        // Keep a reference to the aggregator instance, to prevent it from being garbage collected
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
            MethodMaker mm = cm.addMethod(Aggregator.Factory.class, "factoryFor", Object[].class);
            mm.public_();

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

        // Implement the newAggregator method.
        {
            MethodMaker mm = cm.addMethod(Aggregator.class, "newAggregator").public_();
            if (argCount == 0) {
                mm.return_(mm.new_(aggregatorClass));
            } else {
                mm.return_(mm.new_(aggregatorClass, mm.field("args")));
            }
        }

        addPlanMethod(cm);

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (QueryAggregator) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private void addPlanMethod(ClassMaker cm) {
        if (mRowFilter == TrueFilter.THE) {
            return;
        }
        MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", QueryPlan.Aggregator.class);
        mm.public_();
        mm.return_(mm.new_(QueryPlan.Filter.class, mRowFilter.toString(), mm.param(0)));
    }

    private Class<?> makeAggregatorClass() {
        Class<?> targetType = rowTypeClass();

        ClassMaker cm = RowGen.beginClassMaker
            (AggregatedQueryExpr.class, targetType, targetType.getName(), null, "aggregator")
            .implement(Aggregator.class).final_();

        MethodMaker ctor;
        int argCount = maxArgument();

        if (argCount == 0) {
            ctor = cm.addConstructor();
            ctor.invokeSuperConstructor();
        } else {
            cm.addField(Object[].class, "args").private_().final_();
            ctor = cm.addConstructor(Object[].class);
            ctor.invokeSuperConstructor();
            ctor.field("args").set(ctor.param(0));
        }

        Class<?> sourceType = mFrom.rowType().clazz();

        // Make the "begin", "accumulate", and "finish" methods concurrently. The finish
        // context is treated as the "main" context, and CallExpr will access the appropriate
        // context for adding code to the right place.

        MethodMaker beginMaker = cm.addMethod(sourceType, "begin", Object.class).public_();
        MethodMaker accumMaker = cm.addMethod(sourceType, "accumulate", Object.class).public_();
        MethodMaker finishMaker = cm.addMethod(targetType, "finish", Object.class).public_();

        abstract class Context extends EvalContext {
            Context(Variable argsVar, Variable sourceRowVar) {
                super(argsVar, sourceRowVar);
            }

            @Override
            public Variable rowNum() {
                return methodMaker().field(rowNumName());
            }

            @Override
            public Variable groupNum() {
                return methodMaker().field(groupNumName());
            }

            @Override
            public Variable groupRowNum() {
                return methodMaker().field(groupRowNumName());
            }

            abstract String rowNumName();

            abstract String groupNumName();

            abstract String groupRowNumName();
        }

        Variable beginSourceVar, accumSourceVar, finishTargetVar;
        Context initContext, beginContext, accumContext, finishContext;

        {
            var argsVar = argCount == 0 ? null : ctor.field("args").get();

            initContext = new Context(argsVar, null) {
                private int mNumWorkFields;
                private String mRowNumName, mGroupNumName, mGroupRowNumName;

                @Override
                public MethodMaker methodMaker() {
                    return ctor;
                }

                @Override
                public FieldMaker newWorkField(Class<?> type) {
                    String name = "w$" + ++mNumWorkFields;
                    return methodMaker().classMaker().addField(type, name).private_();
                }

                @Override
                String rowNumName() {
                    String name = mRowNumName;
                    if (name == null) {
                        mRowNumName = name = newWorkField(long.class).name();
                        beginMaker.field(name).inc(1L);
                        accumMaker.field(name).inc(1L);
                    }
                    return name;
                }

                @Override
                String groupNumName() {
                    String name = mGroupNumName;
                    if (name == null) {
                        mGroupNumName = name = newWorkField(long.class).name();
                        beginMaker.field(name).inc(1L);
                    }
                    return name;
                }

                @Override
                String groupRowNumName() {
                    String name = mGroupRowNumName;
                    if (name == null) {
                        mGroupRowNumName = name = newWorkField(long.class).name();
                        beginMaker.field(name).set(1L);
                        accumMaker.field(name).inc(1L);
                    }
                    return name;
                }
            };
        }

        {
            beginSourceVar = beginMaker.param(0).cast(sourceType);
            var argsVar = argCount == 0 ? null : beginMaker.field("args").get();

            beginContext = new Context(argsVar, beginSourceVar) {
                @Override
                String rowNumName() {
                    return initContext.rowNumName();
                }

                @Override
                String groupNumName() {
                    return initContext.groupNumName();
                }

                @Override
                String groupRowNumName() {
                    return initContext.groupRowNumName();
                }
            };
        }

        {
            accumSourceVar = accumMaker.param(0).cast(sourceType);
            var argsVar = argCount == 0 ? null : accumMaker.field("args").get();

            accumContext = new Context(argsVar, accumSourceVar) {
                @Override
                String rowNumName() {
                    return initContext.rowNumName();
                }

                @Override
                String groupNumName() {
                    return initContext.groupNumName();
                }

                @Override
                String groupRowNumName() {
                    return initContext.groupRowNumName();
                }
            };
        }

        {
            finishTargetVar = finishMaker.param(0).cast(targetType);
            var argsVar = argCount == 0 ? null : finishMaker.field("args").get();

            finishContext = new Context(argsVar, null) { // no sourceRowVar
                @Override
                public MethodMaker methodMaker() {
                    return finishMaker;
                }

                @Override
                EvalContext initContext() {
                    return initContext;
                }

                @Override
                EvalContext beginContext() {
                    return beginContext;
                }

                @Override
                EvalContext accumContext() {
                    return accumContext;
                }

                @Override
                String rowNumName() {
                    return initContext.rowNumName();
                }

                @Override
                String groupNumName() {
                    return initContext.groupNumName();
                }

                @Override
                String groupRowNumName() {
                    return initContext.groupRowNumName();
                }
            };
        }

        // Eagerly evaluate AssignExprs. The result might be needed by downstream expressions,
        // the filter, or it might throw an exception. In the unlikely case that none of these
        // conditions are met, then the AssignExprs can be evaluated after filtering.
        // TODO: This is too eager -- it's not checking canThrowRuntimeException, and it's not
        // checking if any downstream expressions exist.
        IdentityHashMap<ProjExpr, Variable> projectedVars = null;
        for (int i=mNumGroupBy; i<mProjection.size(); i++) {
            ProjExpr pe = mProjection.get(i);
            if (pe.wrapped() instanceof AssignExpr) {
                Variable v = pe.makeEval(finishContext);
                if (projectedVars == null) {
                    projectedVars = new IdentityHashMap<>();
                }
                projectedVars.put(pe, v);
            }
        }

        if (mRowFilter != TrueFilter.THE) {
            Label pass = finishMaker.label();
            Label fail = finishMaker.label();
            mFilter.makeFilter(finishContext, pass, fail);
            fail.here();
            finishMaker.return_(null);
            pass.here();
        }

        for (int i=mNumGroupBy; i<mProjection.size(); i++) {
            ProjExpr pe = mProjection.get(i);
            pe.makeSetColumn(finishContext, projectedVars, rowType(), finishTargetVar);
        }

        beginMaker.return_(beginSourceVar);
        accumMaker.return_(accumSourceVar);
        finishMaker.return_(finishTargetVar);

        // Now implement the bridge methods.

        MethodMaker bridge = cm.addMethod(Object.class, "begin", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(sourceType, "begin", null, bridge.param(0)));

        bridge = cm.addMethod(Object.class, "accumulate", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(sourceType, "accumulate", null, bridge.param(0)));

        bridge = cm.addMethod(Object.class, "finish", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(targetType, "finish", null, bridge.param(0)));

        return cm.finishHidden().lookupClass();
    }
}
