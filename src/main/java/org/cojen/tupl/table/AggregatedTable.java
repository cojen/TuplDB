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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Aggregator;
import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.expr.CompiledQuery;
import org.cojen.tupl.table.expr.Parser;

import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

import static java.util.Spliterator.*;

/**
 * Base class for generated aggregated tables.
 *
 * @author Brian S. O'Neill
 * @see Table#aggregate
 */
public abstract class AggregatedTable<S, T> extends WrappedTable<S, T>
    implements QueryFactoryCache.Helper
{
    private record FactoryKey(Class<?> sourceType, Class<?> targetType, Class<?> factoryClass) { }

    private static final WeakCache<FactoryKey, MethodHandle, Table> cFactoryCache;

    static {
        cFactoryCache = new WeakCache<>() {
            @Override
            public MethodHandle newValue(FactoryKey key, Table source) {
                return makeTableFactory(key, source);
            }
        };
    }

    public static <S, T> AggregatedTable<S, T> aggregate(Table<S> source, Class<T> targetType,
                                                         Aggregator.Factory<S, T> factory)
    {
        Objects.requireNonNull(targetType);

        var key = new FactoryKey(source.rowType(), targetType, factory.getClass());
        MethodHandle mh = cFactoryCache.obtain(key, source);

        try {
            return (AggregatedTable<S, T>) mh.invokeExact(mh, source, factory);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * MethodHandle signature:
     *
     *  AggregatedTable<S, T> make(MethodHandle self, Table<S> source, Aggregator.Factory<S, T>)
     */
    private static MethodHandle makeTableFactory(FactoryKey key, Table<?> source) {
        Class<?> sourceType = key.sourceType();
        Class<?> targetType = key.targetType();

        assert source.rowType() == sourceType;

        RowInfo targetInfo = RowInfo.find(targetType);

        // Verify that the target primary key refers to source columns exactly.
        if (!targetInfo.keyColumns.isEmpty()) {
            RowInfo sourceInfo = RowInfo.find(source.rowType());
            for (ColumnInfo targetColumn : targetInfo.keyColumns.values()) {
                String name = targetColumn.name;
                ColumnInfo sourceColumn = sourceInfo.allColumns.get(name);
                if (sourceColumn == null) {
                    throw new IllegalArgumentException
                        ("Target primary key refers to a column which doesn't exist: " + name);
                }
                if (!sourceColumn.isCompatibleWith(targetColumn)) {
                    throw new IllegalArgumentException
                        ("Target primary key refers to an incompatible source column: " + name);
                }
            }
        }

        ClassMaker cm = targetInfo.rowGen().beginClassMaker
            (AggregatedTable.class, targetType, "aggregated").final_()
            .extend(AggregatedTable.class).implement(TableBasicsMaker.find(targetType));

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as references to the generated table instances still exist.
        cm.addField(Object.class, "_").private_().final_();

        // All AggregatedTable instances will refer to the exact same cache.
        cm.addField(QueryFactoryCache.class, "cache").private_().static_().final_()
            .initExact(new QueryFactoryCache());

        {
            MethodMaker ctor = cm.addConstructor
                (MethodHandle.class, Table.class, Aggregator.Factory.class).private_();
            ctor.field("_").set(ctor.param(0));
            ctor.invokeSuperConstructor(ctor.field("cache"), ctor.param(1), ctor.param(2));
        }

        // Add the compareSourceRows method.
        {
            MethodMaker mm = cm.addMethod
                (int.class, "compareSourceRows", Object.class, Object.class).protected_();

            if (targetInfo.keyColumns.isEmpty()) {
                mm.return_(0);
            } else {
                var spec = new StringBuilder();
                for (String name : targetInfo.keyColumns.keySet()) {
                    spec.append('+').append(name);
                }
                var cmpVar = mm.var(Comparator.class).setExact(source.comparator(spec.toString()));
                mm.return_(cmpVar.invoke("compare", mm.param(0), mm.param(1)));
            }
        }

        // Add the finishTarget method.
        {
            MethodMaker mm = cm.addMethod
                (null, "finishTarget", Object.class, Object.class).protected_();

            if (targetInfo.keyColumns.isEmpty()) {
                mm.invoke("cleanRow", mm.param(1));
            } else {
                var sourceVar = mm.param(0).cast(sourceType);
                var targetVar = mm.param(1).cast(targetType);
                for (String name : targetInfo.keyColumns.keySet()) {
                    targetVar.invoke(name, sourceVar.invoke(name));
                }
                mm.invoke("cleanRow", targetVar);
            }
        }

        // Override additional methods determined by whether or not the target row has a
        // primary key.

        if (targetInfo.keyColumns.isEmpty()) {
            cm.addMethod(long.class, "estimateSize").protected_().override().return_(1L);

            cm.addMethod(int.class, "characteristics", Scanner.class)
                .protected_().override().return_(DISTINCT | NONNULL);
        } else {
            // Define a method to check if the primary key is set, to be used by the load and
            // exists methods.
            Class<?> targetClass = RowMaker.find(targetType);
            {
                MethodMaker mm = cm.addMethod
                    (boolean.class, "checkPrimaryKeySet", targetClass).private_().static_();
                targetInfo.rowGen().checkSet(mm, targetInfo.keyColumns, mm.param(0));
            }

            // Override the tryLoad method.
            {
                MethodMaker mm = cm.addMethod
                    (boolean.class, "tryLoad", Transaction.class, Object.class);
                mm.public_().override();

                Variable txnVar = mm.param(0);
                Variable rowVar = mm.param(1).cast(targetClass);

                Label ready = mm.label();
                mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);
                mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();
                ready.here();

                var argsVar = mm.new_(Object[].class, targetInfo.keyColumns.size());

                int argNum = 0;
                for (String name : targetInfo.keyColumns.keySet()) {
                    argsVar.aset(argNum++, rowVar.field(name));
                }

                String queryString = queryString(targetInfo.keyColumns.values());

                var scannerVar = mm.invoke("newScanner", rowVar, txnVar, queryString, argsVar);
                var foundVar = scannerVar.invoke("row").ne(null);
                scannerVar.invoke("close");

                Label cont = mm.label();
                foundVar.ifTrue(cont);
                targetInfo.rowGen().markNonPrimaryKeyColumnsUnset(rowVar);
                cont.here();
                mm.return_(foundVar);
            }

            // Override the exists method. Note that the implementation just calls the tryLoad
            // method, and so there's no performance advantage. In theory, a different query
            // could be used which doesn't project any columns, but it won't make a real
            // difference because the projection implementation just unsets the columns rather
            // than prevent them from being materialized in the first place.
            {
                MethodMaker mm = cm.addMethod
                    (boolean.class, "exists", Transaction.class, Object.class).public_().override();
                mm.return_(mm.invoke("tryLoad", mm.param(0), mm.invoke("cloneRow", mm.param(1))));
            }
        }

        MethodHandles.Lookup lookup = cm.finishLookup();
        Class<?> tableClass = lookup.lookupClass();

        MethodMaker mm = MethodMaker.begin
            (lookup, AggregatedTable.class, null,
             MethodHandle.class, Table.class, Aggregator.Factory.class);
        mm.return_(mm.new_(tableClass, mm.param(0), mm.param(1), mm.param(2)));

        return mm.finish();
    }

    private static String queryString(Collection<ColumnInfo> columns) {
        var bob = new StringBuilder();

        for (ColumnInfo column : columns) {
            if (!bob.isEmpty()) {
                bob.append(" && ");
            }
            bob.append(column.name).append(" == ?");
        }

        return bob.toString();
    }

    private final QueryFactoryCache mQueryFactoryCache;

    protected final Aggregator.Factory<S, T> mAggregatorFactory;

    protected AggregatedTable(QueryFactoryCache queryFactoryCache,
                              Table<S> source, Aggregator.Factory<S, T> factory)
    {
        super(source);
        mQueryFactoryCache = queryFactoryCache;
        mAggregatorFactory = factory;
    }

    @Override
    public final Scanner<T> newScanner(T targetRow, Transaction txn,
                                       String query, Object... args)
        throws IOException
    {
        return query(query).newScanner(targetRow, txn, args);
    }

    /**
     * This method must be overridden when the target row has a primary key.
     */
    @Override
    public boolean tryLoad(Transaction txn, T targetRow) throws IOException {
        Scanner<T> s = newScanner(targetRow, txn);
        boolean found = s.row() != null;
        s.close();
        if (!found) {
            unsetRow(targetRow);
        }
        return found;
    }

    /**
     * This method must be overridden when the target row has a primary key.
     */
    @Override
    public boolean exists(Transaction txn, T targetRow) throws IOException {
        return anyRows(txn);
    }

    /**
     * Called by AggregatedScanner. Returns zero if the source rows are in the same group.
     */
    protected abstract int compareSourceRows(S r1, S r2);

    /**
     * Called by AggregatedScanner. Sets the primary key columns of the target row, and calls
     * cleanRow(target).
     */
    protected abstract void finishTarget(S source, T target);

    /**
     * Called by AggregatedScanner. This method must be overridden when the target row doesn't
     * have a primary key.
     */
    protected long estimateSize() {
        return Long.MAX_VALUE;
    }

    /**
     * Called by AggregatedScanner. This method must be overridden when the target row doesn't
     * have a primary key.
     */
    protected int characteristics(Scanner<S> source) {
        return (source.characteristics() & ~(SIZED | SUBSIZED)) | ORDERED | SORTED;
    }

    @Override // MultiCache; see also WrappedTable
    protected final Object cacheNewValue(Type type, Object key, Object helper)
        throws IOException
    {
        if (type == TYPE_1) { // see the inherited query method
            var queryStr = (String) key;
            try {
                return (Query<T>) mQueryFactoryCache.obtain(queryStr, this).invoke(this);
            } catch (Throwable e) {
                throw RowUtils.rethrow(e);
            }
        }

        if (type == TYPE_2) { // see the inherited derive method
            return CompiledQuery.makeDerived(this, type, key, helper);
        }

        throw new AssertionError();
    }

    @Override
    public MethodHandle makeQueryFactory(QuerySpec targetQuery) {
        Class<S> sourceType = mSource.rowType();
        RowInfo sourceInfo = RowInfo.find(sourceType);

        // Prepare an initial source query, which will be replaced later.
        QuerySpec sourceQuery;
        {
            String proj = mAggregatorFactory.sourceProjection();
            if (proj == null) {
                sourceQuery = new QuerySpec(null, null, TrueFilter.THE);
            } else {
                sourceQuery = Parser.parseQuerySpec(sourceType, '{' + proj + '}');
            }
        }

        Class<T> targetType = rowType();
        RowInfo targetInfo = RowInfo.find(targetType);

        final RowFilter sourceFilter, targetFilter;

        {
            RowFilter filter = targetQuery.filter();

            if (targetInfo.keyColumns.isEmpty()) {
                sourceFilter = TrueFilter.THE;
                targetFilter = filter;
            } else {
                try {
                    filter = filter.cnf();
                } catch (ComplexFilterException e) {
                    // The split won't be as effective, and so the target filter will do
                    // more work.
                }

                var split = new RowFilter[2];
                filter.split(sourceInfo.allColumns, split);

                sourceFilter = split[0];
                targetFilter = split[1];
            }
        }

        OrderBy sourceOrderBy = null, targetOrderBy;

        if (targetInfo.keyColumns.isEmpty()) {
            // At most one row is produced, so no need to sort.
            targetOrderBy = null;
        } else if ((targetOrderBy = targetQuery.orderBy()) != null) {
            // As soon as all of the group-by columns are encountered in the requested target
            // ordering, the rest can be discarded.

            int numGroupBy = targetInfo.keyColumns.size();
            Iterator<Map.Entry<String, OrderBy.Rule>> it = targetOrderBy.entrySet().iterator();

            while (it.hasNext()) {
                String name = it.next().getKey();
                if (targetInfo.keyColumns.containsKey(name) && --numGroupBy == 0 && it.hasNext()) {
                    // Copy and discard the rest.
                    targetOrderBy = new OrderBy(targetOrderBy);
                    do {
                        targetOrderBy.remove(it.next().getKey());
                    } while (it.hasNext());
                }
            }

            // Push as many order-by columns to the source as possible.

            it = targetOrderBy.entrySet().iterator();

            while (true) {
                if (!it.hasNext()) {
                    // All order-by columns have been pushed to the source, and so no target
                    // sort is required.
                    targetOrderBy = null;
                    break;
                }

                Map.Entry<String, OrderBy.Rule> e = it.next();
                String name = e.getKey();

                if (!sourceInfo.allColumns.containsKey(name)) {
                    // Once a target-only column is encountered, no more columns can be pushed
                    // to the source, and a target sort is required.
                    break;
                }

                if (sourceOrderBy == null) {
                    sourceOrderBy = new OrderBy();
                }

                sourceOrderBy.put(name, e.getValue());
            }
        }

        if (!targetInfo.keyColumns.isEmpty()) {
            // To ensure proper grouping, all group-by columns (the target primary key) must be
            // appended to the source order-by.
            if (sourceOrderBy == null) {
                sourceOrderBy = new OrderBy();
            }
            for (ColumnInfo column : targetInfo.keyColumns.values()) {
                String name = column.name;
                if (!sourceOrderBy.containsKey(name)) {
                    OrderBy.Rule rule = targetQuery.orderByRule(name);
                    if (rule == null && (rule = sourceQuery.orderByRule(name)) == null) {
                        rule = new OrderBy.Rule(column, column.typeCode);
                    }
                    sourceOrderBy.put(name, rule);
                }
            }
        }

        sourceQuery = sourceQuery.withOrderBy(sourceOrderBy).withFilter(sourceFilter);
        targetQuery = targetQuery.withOrderBy(targetOrderBy).withFilter(targetFilter);

        ClassMaker queryMaker = targetInfo.rowGen().beginClassMaker
            (AggregatedTable.class, targetType, "query").final_().extend(BaseQuery.class);

        {
            MethodMaker mm = queryMaker.addConstructor(AggregatedTable.class).private_();
            var tableVar = mm.param(0);
            if (sourceQuery == null) {
                mm.invokeSuperConstructor(tableVar);
            } else {
                mm.invokeSuperConstructor(tableVar, sourceQuery.toString());
            }
        }

        // Add the newScanner method.

        {
            String methodName = "newScanner";

            MethodMaker mm = queryMaker.addMethod
                (Scanner.class, methodName, Object.class, Transaction.class, Object[].class)
                .public_().varargs();

            var targetRowVar = mm.param(0);
            var txnVar = mm.param(1);
            var argsVar = mm.param(2);
            var tableVar = mm.field("table").get();

            var sourceScannerVar = mm.field("squery").invoke(methodName, txnVar, argsVar);
            var aggregatorVar = tableVar.invoke("newAggregator", sourceScannerVar);

            // Define the comparator returned by AggregatedScanner.

            Variable aggregateComparatorVar = null;

            if (!targetInfo.keyColumns.isEmpty()) {
                var aggregateOrderBy = new OrderBy(sourceOrderBy);
                Iterator<Map.Entry<String, OrderBy.Rule>> it =
                    aggregateOrderBy.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<String, OrderBy.Rule> e = it.next();
                    if (!targetInfo.keyColumns.containsKey(e.getKey())) {
                        // Can only refer to target primary key columns.
                        it.remove();
                    }
                }
            
                if (!sourceOrderBy.isEmpty()) {
                    aggregateComparatorVar = mm.var(Comparator.class).setExact
                        (ComparatorMaker.comparator
                         (targetType, aggregateOrderBy, aggregateOrderBy.spec()));
                }
            }

            var targetScannerVar = mm.new_(AggregatedScanner.class, tableVar, sourceScannerVar,
                                           aggregateComparatorVar, targetRowVar, aggregatorVar);

            targetScannerVar = WrappedScanner.wrap(targetType, argsVar, targetScannerVar,
                                                   targetQuery.filter(), targetQuery.projection());

            if (targetQuery.orderBy() != null) {
                OrderBy orderBy = targetQuery.orderBy();
                String orderBySpec = orderBy.spec();

                var comparatorVar = mm.var(Comparator.class).setExact
                    (ComparatorMaker.comparator(targetType, orderBy, orderBySpec));

                Variable projectionVar = null;

                if (targetQuery.projection() != null) {
                    projectionVar = mm.var(Set.class).setExact
                        (SortedQueryLauncher.canonicalize(targetQuery.projection().keySet()));
                }

                targetScannerVar = tableVar.invoke("sort", targetScannerVar, comparatorVar,
                                                   projectionVar, orderBySpec);
            }

            mm.return_(targetScannerVar);
        }

        // Add the plan method.

        {
            MethodMaker mm = queryMaker.addMethod
                (QueryPlan.class, "scannerPlan", Transaction.class, Object[].class)
                .public_().varargs();

            var txnVar = mm.param(0);
            var argsVar = mm.param(1);
            var tableVar = mm.field("table").get();
            var sourceQueryVar = mm.field("squery");

            final var planVar = sourceQueryVar.invoke("scannerPlan", txnVar, argsVar);

            var targetVar = mm.var(Class.class).set(targetType).invoke("getName");
            var groupByVar = tableVar.invoke("groupByColumns");

            var aggregatorPlanVar = mm.new_
                (QueryPlan.Aggregator.class, targetVar, null, groupByVar, planVar);
            planVar.set(tableVar.invoke("plan", aggregatorPlanVar));

            if (targetQuery.filter() != TrueFilter.THE) {
                planVar.set(mm.new_(QueryPlan.Filter.class,
                                    targetQuery.filter().toString(), planVar));
            }

            if (targetQuery.orderBy() != null) {
                String spec = targetQuery.orderBy().spec();
                var columnsVar = mm.var(OrderBy.class).invoke("splitSpec", spec);
                planVar.set(mm.new_(QueryPlan.Sort.class, columnsVar, planVar));
            }

            mm.return_(planVar);
        }

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated query class still exists.
        queryMaker.addField(Object.class, "handle").private_().static_();

        return QueryFactoryCache.ctorHandle(queryMaker.finishLookup(), AggregatedTable.class);
    }

    public abstract static class BaseQuery<S, T> implements Query<T> {
        protected final AggregatedTable<S, T> table;
        protected final Query<S> squery;

        protected BaseQuery(AggregatedTable<S, T> table) throws IOException {
            this.table = table;
            this.squery = table.mSource.queryAll();
        }

        protected BaseQuery(AggregatedTable<S, T> table, String queryStr) throws IOException {
            this.table = table;
            this.squery = table.mSource.query(queryStr);
        }

        @Override
        public final Class<T> rowType() {
            return table.rowType();
        }

        @Override
        public final int argumentCount() {
            return squery.argumentCount();
        }
    }

    /**
     * Called by generated Query instances.
     */
    public final Aggregator<S, T> newAggregator(Scanner<S> sourceScanner) throws IOException {
        try {
            return mAggregatorFactory.newAggregator();
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
    public final QueryPlan plan(QueryPlan.Aggregator plan) {
        return mAggregatorFactory.plan(plan);
    }

    /**
     * Called by generated Query instances. Returns columns (or null) for QueryPlan.Aggregator.
     */
    public final String[] groupByColumns() {
        RowInfo info = RowInfo.find(rowType());
        if (info.keyColumns.isEmpty()) {
            return null;
        }
        var columns = new String[info.keyColumns.size()];
        int ix = 0;
        for (String name : info.keyColumns.keySet()) {
            columns[ix++] = name;
        }
        return columns;
    }
}
