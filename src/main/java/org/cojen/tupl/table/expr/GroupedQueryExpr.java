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

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;

import java.util.function.Consumer;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Grouper;
import org.cojen.tupl.Table;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.WeakCache;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Defines a QueryExpr which relies on a Grouper to perform custom transformations and
 * filtering.
 *
 * @author Brian S. O'Neill
 * @see QueryExpr#make
 */
final class GroupedQueryExpr extends QueryExpr {
    // FIXME: Doesn't work properly with this query: {;} -- it never stops

    /**
     * @param filter can be null if rowFilter is TrueFilter
     * @param projection not null
     * @param groupBy number of projected columns to group by; must be >= 0
     * @param orderBy optional view ordering to apply after calling Table.group
     * @see QueryExpr#make
     */
    static GroupedQueryExpr make(int startPos, int endPos, RelationType type,
                                 RelationExpr from, RowFilter rowFilter, Expr filter,
                                 List<ProjExpr> projection, int groupBy,
                                 int maxArgument, String orderBy)
    {
        var newAssignments = new HashMap<ColumnExpr, AssignExpr>();

        if (filter != null) {
            filter = filter.asWindow(newAssignments);
        }

        projection = replaceElements(projection, groupBy,
                                     (i, proj) -> proj.asWindow(newAssignments));

        return new GroupedQueryExpr(startPos, endPos, type, from, rowFilter, filter,
                                    projection, groupBy, maxArgument, orderBy);
    }

    private final int mGroupBy;
    private final Expr mFilter;
    private final String mOrderBy;

    private GroupedQueryExpr(int startPos, int endPos, RelationType type,
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
        return false;
    }

    @Override
    public QuerySpec querySpec() {
        throw new QueryException("Query performs grouping");
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
            enc.encodeInt(mGroupBy);
        }
    }

    private static final WeakCache<Object, QueryGrouper, GroupedQueryExpr> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public QueryGrouper newValue(Object key, GroupedQueryExpr expr) {
                return expr.makeQueryGrouper();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompiledQuery makeCompiledQuery() throws IOException {
        CompiledQuery source = mFrom.makeCompiledQuery();

        QueryGrouper qg = cCache.obtain(makeKey(), this);

        Class targetClass = rowTypeClass();

        int argCount = maxArgument();

        if (argCount == 0) {
            Table table = source.table().group("", "", // FIXME: groupBy and orderBy
                                               targetClass, qg.factoryFor(RowUtils.NO_ARGS));
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

    private QueryGrouper makeQueryGrouper() {
        Class<?> grouperClass = makeGrouperClass();

        Class<?> targetClass = rowTypeClass();

        ClassMaker cm = RowGen.beginClassMaker
            (GroupedQueryExpr.class, targetClass, targetClass.getName(), null, "qg")
            .extend(QueryGrouper.class).final_();

        // Keep a reference to the grouper instance, to prevent it from being garbage collected
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
            MethodMaker mm = cm.addMethod(Grouper.Factory.class, "factoryFor", Object[].class);
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

        // Implement the newGrouper method.
        {
            MethodMaker mm = cm.addMethod(Grouper.class, "newGrouper").public_();
            if (argCount == 0) {
                mm.return_(mm.new_(grouperClass));
            } else {
                mm.return_(mm.new_(grouperClass, mm.field("args")));
            }
        }

        // FIXME: addInverseMappingFunctions

        addPlanMethod(cm);

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (QueryGrouper) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private void addPlanMethod(ClassMaker cm) {
        if (mRowFilter == TrueFilter.THE) {
            return;
        }
        MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", QueryPlan.Grouper.class);
        mm.public_();
        mm.return_(mm.new_(QueryPlan.Filter.class, mRowFilter.toString(), mm.param(0)));
    }

    private Class<?> makeGrouperClass() {
        Class<?> targetType = rowTypeClass();

        ClassMaker cm = RowGen.beginClassMaker
            (GroupedQueryExpr.class, targetType, targetType.getName(), null, "grouper")
            .implement(Grouper.class).final_();

        MethodMaker ctor;
        int argCount = maxArgument();

        if (argCount == 0) {
            ctor = cm.addConstructor();
            ctor.invokeSuperConstructor();
        } else {
            cm.addField(Object[].class, "args").private_().final_();
            ctor = cm.addConstructor(Object[].class, "args");
            ctor.invokeSuperConstructor();
            ctor.field("args").set(ctor.param(0));
        }

        Class<?> sourceType = mFrom.rowType().clazz();

        // Make the "begin", "accumulate", "finished", and "step" methods concurrently. The
        // step context is treated as the "main" context, and CallExpr will access the
        // appropriate context for adding code to the right place.

        MethodMaker beginMaker = cm.addMethod(sourceType, "begin", Object.class).public_();
        MethodMaker accumMaker = cm.addMethod(sourceType, "accumulate", Object.class).public_();
        MethodMaker finishedMaker = cm.addMethod(null, "finished").public_();
        MethodMaker stepMaker = cm.addMethod(targetType, "step", Object.class).public_();

        // FIXME: Lots of duplication with AggregatedQueryExpr.

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

        Variable beginSourceVar, accumSourceVar, stepTargetVar;
        Context initContext, beginContext, accumContext, finishedContext, stepContext;

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
                private int mNumWorkFields;
                private String mRowNumName, mGroupNumName, mGroupRowNumName;

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
            var argsVar = argCount == 0 ? null : finishedMaker.field("args").get();

            finishedContext = new Context(argsVar, null) { // no sourceRowVar
                @Override
                public MethodMaker methodMaker() {
                    return finishedMaker;
                }

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
            stepTargetVar = stepMaker.param(0).cast(targetType);
            var argsVar = argCount == 0 ? null : stepMaker.field("args").get();

            // FIXME: Lack of sourceRowVar is a problem when some projected columns are just
            // plain projected columns. Do they need to be replaced with a special function
            // like what AggregatedQueryExpr does? Yes, and they need buffering too.
            stepContext = new Context(argsVar, null) { // no sourceRowVar
                Label mCheckLabel = stepMaker.label().here();

                @Override
                public MethodMaker methodMaker() {
                    return stepMaker;
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
                EvalContext finishedContext() {
                    return finishedContext;
                }

                @Override
                void checkContext(Consumer<EvalContext> body) {
                    // All of the checks must be performed before the core step logic.
                    mCheckLabel = mCheckLabel.insert(() -> body.accept(this));
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
        IdentityHashMap<AssignExpr, Variable> projectedVars = null;
        if (mProjection != null) {
            for (int i=mGroupBy; i<mProjection.size(); i++) {
                ProjExpr pe = mProjection.get(i);
                if (pe.wrapped() instanceof AssignExpr ae) {
                    Variable v = pe.makeEval(stepContext);
                    if (projectedVars == null) {
                        projectedVars = new IdentityHashMap<>();
                    }
                    projectedVars.put(ae, v);
                }
            }
        }

        if (mRowFilter != TrueFilter.THE) {
            Label pass = stepMaker.label();
            Label fail = stepMaker.label();
            mFilter.makeFilter(stepContext, pass, fail);
            fail.here();
            stepMaker.return_(null);
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
                result = pe.makeEval(stepContext);
            }
            stepTargetVar.invoke(pe.name(), result);
        }

        beginMaker.return_(beginSourceVar);
        accumMaker.return_(accumSourceVar);
        stepMaker.return_(stepTargetVar);

        // Now implement the bridge methods.

        MethodMaker bridge = cm.addMethod(Object.class, "begin", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(sourceType, "begin", null, bridge.param(0)));

        bridge = cm.addMethod(Object.class, "accumulate", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(sourceType, "accumulate", null, bridge.param(0)));

        bridge = cm.addMethod(Object.class, "step", Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(targetType, "step", null, bridge.param(0)));

        return cm.finishHidden().lookupClass();
    }
}
