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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Aggregator;
import org.cojen.tupl.Table;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowUtils;
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
            // FIXME: If any group ProjExpr isn't a ColumnExpr, then first remap "from" using
            // MappedQueryExpr.
            group = new HashSet<>(groupBy << 1);
            for (int i=0; i<groupBy; i++) {
                ProjExpr proj = projection.get(i);
                group.add(proj.name());
            }
        }

        if (filter != null) {
            // FIXME: Must not apply to columns in the group itself.
            filter = filter.asAggregate(group);
        }

        boolean copied = false;

        for (int i=groupBy; i<projection.size(); i++) {
            ProjExpr proj = projection.get(i);
            ProjExpr asAgg = proj.asAggregate(group);
            if (proj != asAgg) {
                if (!copied) {
                    projection = new ArrayList<>(projection);
                }
                projection.set(i, asAgg);
            }
        }
        
        return new AggregatedQueryExpr(startPos, endPos, type, from, rowFilter, filter,
                                       projection, groupBy, maxArgument, orderBy);
    }

    private final int mGroupBy;
    private final Expr mFilter;
    private final String mOrderBy;

    private AggregatedQueryExpr(int startPos, int endPos, RelationType type,
                                RelationExpr from, RowFilter rowFilter, Expr filter,
                                List<ProjExpr> projection, int groupBy,
                                int maxArgument, String orderBy)
    {
        super(startPos, endPos, type, from, rowFilter, projection, maxArgument);

        mGroupBy = groupBy;
        mFilter = filter;
        mOrderBy = orderBy;
    }

    @Override
    public boolean isPureFunction() {
        return super.isPureFunction() && (mFilter == null || mFilter.isPureFunction());
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
    protected final void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            super.doEncodeKey(enc);
            enc.encodeInt(mGroupBy);
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
                table = table.view(mOrderBy);
            }
            return CompiledQuery.make(table);
        }

        if (mOrderBy == null) {
            /* FIXME: with argCount
            return new NoView(source, argCount, targetClass, qa);
            */
            throw null;
        }

        /* FIXME: with argCount and orderBy
        return new WithView(source, argCount, targetClass, qa, mOrderBy);
        */
        throw null;
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
        } else {
            cm.addField(Object[].class, "args").private_().final_();
            ctor = cm.addConstructor(Object[].class, "args");
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
        Context beginContext, accumContext, finishContext;

        {
            beginSourceVar = beginMaker.param(0).cast(sourceType);
            var argsVar = argCount == 0 ? null : beginMaker.field("args").get();

            beginContext = new Context(argsVar, beginSourceVar) {
                private int mNumWorkFields;
                private String mRowNumName, mGroupNumName, mGroupRowNumName;

                @Override
                public Field newWorkField(Class<?> type) {
                    String name = "w$" + ++mNumWorkFields;
                    MethodMaker mm = methodMaker();
                    mm.classMaker().addField(type, name).private_();
                    return mm.field(name);
                }

                @Override
                String rowNumName() {
                    String name = mRowNumName;
                    if (name == null) {
                        Field field = newWorkField(long.class);
                        mRowNumName = name = field.name();
                        ctor.field(name).set(1L);
                        field.inc(1L);
                        accumMaker.field(name).inc(1L);
                    }
                    return name;
                }

                @Override
                String groupNumName() {
                    String name = mGroupNumName;
                    if (name == null) {
                        mGroupNumName = name = newWorkField(long.class).name();
                        ctor.field(name).set(1L);
                        accumMaker.field(name).inc(1L);
                    }
                    return name;
                }

                @Override
                String groupRowNumName() {
                    String name = mGroupRowNumName;
                    if (name == null) {
                        mGroupRowNumName = name = newWorkField(long.class).set(1L).name();
                        accumMaker.field(name).inc(1L);
                    }
                    return name;
                }
            };
        }

        {
            accumSourceVar = accumMaker.param(0).cast(sourceType);
            var argsVar = argCount == 0 ? null : accumMaker.field("args").get();

            accumContext = new Context(argsVar, accumSourceVar) {
                @Override
                String rowNumName() {
                    return beginContext.rowNumName();
                }

                @Override
                String groupNumName() {
                    return beginContext.groupNumName();
                }

                @Override
                String groupRowNumName() {
                    return beginContext.groupRowNumName();
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
                EvalContext beginContext() {
                    return beginContext;
                }

                @Override
                EvalContext accumContext() {
                    return accumContext;
                }

                @Override
                String rowNumName() {
                    return beginContext.rowNumName();
                }

                @Override
                String groupNumName() {
                    return beginContext.groupNumName();
                }

                @Override
                String groupRowNumName() {
                    return beginContext.groupRowNumName();
                }
            };
        }

        // Eagerly evaluate AssignExprs. The result might be needed by downstream expressions,
        // the filter, or it might throw an exception. In the unlikely case that none of these
        // conditions are met, then the AssignExprs can be evaluated after filtering.
        // TODO: This is too eager -- it's not checking canThrowRuntimeException, and it's not
        // checking if any downstream expressions exist.
        IdentityHashMap<AssignExpr, Variable> projectedVars = null;
        if (mProjection != null) {
            for (int i=mGroupBy; i<mProjection.size(); i++) {
                ProjExpr pe = mProjection.get(i);
                if (pe.wrapped() instanceof AssignExpr ae) {
                    Variable v = pe.makeEval(finishContext);
                    if (projectedVars == null) {
                        projectedVars = new IdentityHashMap<>();
                    }
                    projectedVars.put(ae, v);
                }
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

        for (int i=mGroupBy; i<mProjection.size(); i++) {
            ProjExpr pe = mProjection.get(i);
            if (pe.shouldExclude()) {
                continue;
            }
            Variable result;
            if (pe.wrapped() instanceof AssignExpr ae) {
                result = projectedVars.get(ae);
            } else {
                result = pe.makeEval(finishContext);
            }
            finishTargetVar.invoke(pe.name(), result);
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
