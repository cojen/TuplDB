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

package org.cojen.tupl.table;

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Grouper;
import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Base class for generated grouped tables.
 *
 * @author Brian S. O'Neill
 * @see Table#group
 */
public abstract class GroupedTable<S, T> extends AbstractMappedTable<S, T>
    implements QueryFactoryCache.Helper
{
    private record FactoryKey(Class<?> sourceType, String groupBy, String orderBy,
                              Class<?> targetType, Class<?> factoryClass) { } 

    private static final WeakCache<FactoryKey, MethodHandle, Table> cFactoryCache;

    static {
        cFactoryCache = new WeakCache<>() {
            @Override
            public MethodHandle newValue(FactoryKey key, Table source) {
                return makeTableFactory(key, source);
            }
        };
    }

    public static <S, T> GroupedTable<S, T> group(Table<S> source, String groupBy, String orderBy,
                                                  Class<T> targetType,
                                                  Grouper.Factory<S, T> factory)
    {
        Objects.requireNonNull(groupBy);
        Objects.requireNonNull(orderBy);
        Objects.requireNonNull(targetType);

        var key = new FactoryKey(source.rowType(), groupBy, orderBy,
                                 targetType, factory.getClass());

        try {
            return (GroupedTable<S, T>) cFactoryCache.obtain(key, source)
                .invokeExact(source, factory);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * MethodHandle signature:
     *
     *  GroupedTable<S, T> make(Table<S> source, Grouper.Factory<S, T>)
     */
    private static MethodHandle makeTableFactory(FactoryKey key, Table<?> source) {
        Class<?> sourceType = key.sourceType();
        Class<?> targetType = key.targetType();

        assert source.rowType() == sourceType;

        RowInfo sourceInfo = RowInfo.find(source.rowType());

        OrderBy groupBy;
        try {
            groupBy = OrderBy.forSpec(sourceInfo, key.groupBy());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(groupByMessage(e));
        } catch (IllegalStateException e) {
            throw new IllegalStateException(groupByMessage(e));
        }

        OrderBy orderBy = OrderBy.forSpec(sourceInfo, key.orderBy());

        // Remove unnecessary order-by columns.
        orderBy.keySet().removeAll(groupBy.keySet());

        String groupBySpec = groupBy.spec();
        String orderBySpec = orderBy.spec();

        ClassMaker cm = RowInfo.find(targetType).rowGen().beginClassMaker
            (GroupedTable.class, targetType, "grouped").final_()
            .extend(GroupedTable.class).implement(TableBasicsMaker.find(targetType));

        {
            MethodMaker ctor = cm.addConstructor(Table.class, Grouper.Factory.class).private_();
            var cacheVar = ctor.var(QueryFactoryCache.class).setExact(new QueryFactoryCache());
            ctor.invokeSuperConstructor
                (cacheVar, ctor.param(0), groupBySpec, orderBySpec, ctor.param(1));
        }

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated table class still exists.
        cm.addField(Object.class, "_").static_().private_();

        MethodHandles.Lookup lookup = cm.finishLookup();
        Class<?> tableClass = lookup.lookupClass();

        MethodMaker mm = MethodMaker.begin
            (lookup, GroupedTable.class, null, Table.class, Grouper.Factory.class);
        mm.return_(mm.new_(tableClass, mm.param(0), mm.param(1)));

        MethodHandle mh = mm.finish();

        try {
            // Assign the singleton reference.
            lookup.findStaticVarHandle(tableClass, "_", Object.class).set(mh);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }

        return mh;
    }

    private final QueryFactoryCache mQueryFactoryCache;

    protected final String mGroupBySpec, mOrderBySpec;
    protected final Grouper.Factory<S, T> mGrouperFactory;
    protected final Comparator<S> mGroupComparator;

    protected GroupedTable(QueryFactoryCache queryFactoryCache,
                           Table<S> source, String groupBySpec, String orderBySpec,
                           Grouper.Factory<S, T> factory)
    {
        super(source);
        mQueryFactoryCache = queryFactoryCache;
        mGroupBySpec = groupBySpec;
        mOrderBySpec = orderBySpec;
        mGrouperFactory = factory;
        mGroupComparator = source.comparator(groupBySpec);
    }

    private static String groupByMessage(RuntimeException e) {
        String message = e.getMessage();
        if (message == null) {
            throw e;
        }
        return message.replace("ordering", "group-by");
    }

    @Override
    public final Scanner<T> newScanner(T targetRow, Transaction txn,
                                       String query, Object... args)
        throws IOException
    {
        return query(query).newScanner(targetRow, txn, args);
    }

    @Override
    public final boolean tryLoad(Transaction txn, T row) throws IOException {
        throw new ViewConstraintException();
    }

    @Override
    public boolean exists(Transaction txn, T row) throws IOException {
        throw new ViewConstraintException();
    }

    /**
     * Returns columns (or null) for QueryPlan.Grouper.
     */
    public final String[] groupByColumns() {
        return splitSpec(mGroupBySpec);
    }

    /**
     * Returns columns (or null) for QueryPlan.Grouper.
     */
    public final String[] orderByColumns() {
        return splitSpec(mOrderBySpec);
    }

    private String[] splitSpec(String spec) {
        return spec.isEmpty() ? null : OrderBy.splitSpec(spec);
    }

    @Override
    protected final Query<T> newQuery(String query) throws IOException {
        try {
            return (Query<T>) mQueryFactoryCache.obtain(query, this).invoke(this);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public MethodHandle makeQueryFactory(QuerySpec targetQuery) {
        var splitter = new Splitter(targetQuery);

        Class<T> targetType = rowType();
        RowInfo targetInfo = RowInfo.find(targetType);

        ClassMaker cm = targetInfo.rowGen().beginClassMaker
            (GroupedTable.class, targetType, "query").final_().extend(BaseQuery.class);

        {
            MethodMaker mm = cm.addConstructor(GroupedTable.class).private_();
            var tableVar = mm.param(0);
            QuerySpec sourceQuery = splitter.mSourceQuery;
            if (sourceQuery == null) {
                mm.invokeSuperConstructor(tableVar);
            } else {
                mm.invokeSuperConstructor(tableVar, sourceQuery.toString());
            }
        }

        Class scannerClass = GroupedScanner.class;

        RowFilter targetRemainder = splitter.mTargetRemainder;

        if (targetRemainder != TrueFilter.THE || targetQuery.projection() != null) {
            // Subclass the GroupedScanner class and override the finish method.

            ClassMaker scMaker = targetInfo.rowGen().beginClassMaker
                (GroupedTable.class, targetType, "scanner").final_().extend(GroupedScanner.class);

            MethodMaker ctor;
            if (targetRemainder == TrueFilter.THE) {
                ctor = scMaker.addConstructor
                    (GroupedTable.class, Scanner.class, Object.class, Grouper.class);
            } else {
                ctor = scMaker.addConstructor
                    (GroupedTable.class, Scanner.class, Object.class, Grouper.class,
                     Object[].class).varargs();
            }

            MethodMaker mm = scMaker.addMethod(boolean.class, "finish", Object.class);
            mm.protected_().override();

            var targetRowVar = mm.param(0);

            if (targetRemainder != TrueFilter.THE) {
                scMaker.addField(Predicate.class, "predicate").private_().final_();
                MethodHandle mh = PlainPredicateMaker
                    .predicateHandle(targetType, targetRemainder.toString());
                ctor.field("predicate").set(ctor.invoke(mh, ctor.param(4)));

                Label cont = mm.label();
                mm.field("predicate").invoke("test", targetRowVar).ifTrue(cont);
                mm.return_(false);
                cont.here();
            }

            Map<String, ColumnInfo> projection = targetQuery.projection();

            if (projection != null) {
                TableMaker.unset
                    (targetInfo, targetRowVar.cast(RowMaker.find(targetType)), projection);
            }

            mm.return_(true);

            ctor.invokeSuperConstructor(ctor.param(0), ctor.param(1), ctor.param(2), ctor.param(3));

            scannerClass = scMaker.finishHidden().lookupClass();
        }

        splitter.addPrepareArgsMethod(cm);

        // Add the newScanner method.

        {
            String methodName = "newScanner";

            MethodMaker mm = cm.addMethod
                (Scanner.class, methodName, Object.class, Transaction.class, Object[].class)
                .public_().varargs();

            var targetRowVar = mm.param(0);
            var txnVar = mm.param(1);
            var argsVar = mm.param(2);
            var tableVar = mm.field("table").get();

            argsVar = splitter.prepareArgs(argsVar);

            var sourceScannerVar = mm.field("squery").invoke(methodName, txnVar, argsVar);
            var grouperVar = tableVar.invoke("newGrouper", sourceScannerVar);

            Variable resultVar;

            if (targetRemainder == TrueFilter.THE) {
                resultVar = mm.new_(scannerClass, tableVar,
                                    sourceScannerVar, targetRowVar, grouperVar);
            } else {
                resultVar = mm.new_(scannerClass, tableVar,
                                    sourceScannerVar, targetRowVar, grouperVar, argsVar);
            }

            SortPlan sortPlan = splitter.mSortPlan;

            if (sortPlan.sortOrder != null) {
                var comparatorVar = mm.var(Comparator.class).setExact(sortPlan.sortComparator);

                Variable projectionVar = null;

                if (targetQuery.projection() != null) {
                    projectionVar = mm.var(Set.class).setExact
                        (SortedQueryLauncher.canonicalize(targetQuery.projection().keySet()));
                }

                resultVar = tableVar.invoke("sort", resultVar, comparatorVar,
                                            projectionVar, sortPlan.sortOrderSpec);
            }

            mm.return_(resultVar);
        }

        // Add the plan method.

        {
            MethodMaker mm = cm.addMethod
                (QueryPlan.class, "scannerPlan", Transaction.class, Object[].class)
                .public_().varargs();

            var txnVar = mm.param(0);
            var argsVar = splitter.prepareArgs(mm.param(1));
            var tableVar = mm.field("table").get();
            var sourceQueryVar = mm.field("squery");

            final var planVar = sourceQueryVar.invoke("scannerPlan", txnVar, argsVar);

            var targetVar = mm.var(Class.class).set(targetType).invoke("getName");
            var groupByVar = tableVar.invoke("groupByColumns");
            var orderByVar = tableVar.invoke("orderByColumns");

            var grouperPlanVar = mm.new_
                (QueryPlan.Grouper.class, targetVar, null, groupByVar, orderByVar, planVar);
            planVar.set(tableVar.invoke("plan", grouperPlanVar));

            if (targetRemainder != TrueFilter.THE) {
                planVar.set(mm.new_(QueryPlan.Filter.class, targetRemainder.toString(), planVar));
            }

            SortPlan sortPlan = splitter.mSortPlan;

            if (sortPlan.sortOrder != null) {
                var columnsVar = mm.var(OrderBy.class).invoke("splitSpec", sortPlan.sortOrderSpec);
                planVar.set(mm.new_(QueryPlan.Sort.class, columnsVar, planVar));
            }

            mm.return_(planVar);
        }

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated query class still exists.
        cm.addField(Object.class, "handle").private_().static_();

        return QueryFactoryCache.ctorHandle(cm.finishLookup(), GroupedTable.class);
    }

    @Override
    protected final String sourceProjection() {
        return mGrouperFactory.sourceProjection();
    }

    @Override
    protected final Class<?> inverseFunctions() {
        return mGrouperFactory.getClass();
    }

    @Override
    protected final SortPlan analyzeSort(InverseFinder finder, QuerySpec targetQuery) {
        OrderBy groupBy = OrderBy.forSpec(finder.mSourceColumns, mGroupBySpec);
        OrderBy orderBy = OrderBy.forSpec(finder.mSourceColumns, mOrderBySpec);
        OrderBy targetOrderBy = targetQuery.orderBy();

        var plan = new SortPlan();

        if (targetOrderBy == null) {
            groupBy.putAll(orderBy);
            if (!groupBy.isEmpty()) {
                plan.sourceOrder = groupBy;
            }
            return plan;
        }

        OrderBy sourceOrderBy = groupBy;

        // Attempt to push down the target ordering to the source. The end result is the same
        // logical grouping as before, but the order in which they are processed can be
        // different.

        pushDown: {
            for (var rule : targetOrderBy.values()) {
                ColumnFunction source = finder.tryFindSource(rule.column());
                ColumnInfo sc;
                if (source == null || !source.isUntransformed() ||
                    !groupBy.containsKey((sc = source.column()).name))
                {
                    break pushDown;
                }
                if (sourceOrderBy == groupBy) {
                    sourceOrderBy = new OrderBy();
                }
                sourceOrderBy.put(sc.name, new OrderBy.Rule(sc, rule.type()));
            }

            // All target ordering columns have been pushed to the source.
            targetOrderBy = null;
        }

        if (sourceOrderBy != groupBy) {
            // Apply any unused groupBy columns.
            for (var rule : groupBy.values()) {
                sourceOrderBy.putIfAbsent(rule.column().name, rule);
            }
        }

        // Apply the ordering within the source groups.
        for (Map.Entry<String, OrderBy.Rule> e : orderBy.entrySet()) {
            sourceOrderBy.putIfAbsent(e.getKey(), e.getValue());
        }

        plan.sourceOrder = sourceOrderBy;

        plan.sortOrder = targetOrderBy;

        if (targetOrderBy != null) {
            String spec = targetOrderBy.spec();
            plan.sortOrderSpec = spec;
            plan.sortComparator = ComparatorMaker.comparator(rowType(), targetOrderBy, spec);
        }

        return plan;
    }

    public abstract static class BaseQuery<S, T> implements Query<T> {
        protected final GroupedTable<S, T> table;
        protected final Query<S> squery;

        protected BaseQuery(GroupedTable<S, T> table) throws IOException {
            this.table = table;
            this.squery = table.mSource.queryAll();
        }

        protected BaseQuery(GroupedTable<S, T> table, String queryStr) throws IOException {
            this.table = table;
            this.squery = table.mSource.query(queryStr);
        }
    }

    /**
     * Called by generated Query instances.
     */
    public final Grouper<S, T> newGrouper() throws IOException {
        return mGrouperFactory.newGrouper();
    }

    /**
     * Called by generated Query instances.
     */
    public final Grouper<S, T> newGrouper(Scanner<S> sourceScanner) throws IOException {
        try {
            return mGrouperFactory.newGrouper();
        } catch (Throwable e) {
            try {
                sourceScanner.close();
            } catch (Throwable e2) {
                RowUtils.suppress(e, e2);
            }
            throw e;
        }
    }

    /**
     * Called by generated Query instances.
     */
    public final QueryPlan plan(QueryPlan.Grouper plan) {
        return mGrouperFactory.plan(plan);
    }
}
