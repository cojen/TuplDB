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
import java.lang.invoke.MethodType;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.LockMode;
import org.cojen.tupl.Mapper;
import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;
import org.cojen.tupl.Updater;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.expr.CompiledQuery;

import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Base class for generated mapped tables.
 *
 * @author Brian S O'Neill
 * @see Table#map
 */
public abstract class MappedTable<S, T> extends AbstractMappedTable<S, T>
    implements QueryFactoryCache.Helper
{
    private record FactoryKey(Class<?> sourceType, Class<?> targetType, Class<?> mapperClass) { }

    private static final WeakCache<FactoryKey, MethodHandle, Object> cFactoryCache;

    static {
        cFactoryCache = new WeakCache<>() {
            @Override
            public MethodHandle newValue(FactoryKey key, Object unused) {
                return makeTableFactory(key);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <S, T> MappedTable<S, T> map(Table<S> source, Class<T> targetType,
                                               Mapper<S, T> mapper)
    {
        Objects.requireNonNull(targetType);

        var key = new FactoryKey(source.rowType(), targetType, mapper.getClass());
        MethodHandle mh = cFactoryCache.obtain(key, null);

        try {
            return (MappedTable<S, T>) mh.invokeExact(mh, source, mapper);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * MethodHandle signature:
     *
     * MappedTable<S, T> make(MethodHandle self, Table<S> source, Mapper<S, T> mapper)
     */
    private static MethodHandle makeTableFactory(FactoryKey key) {
        Class<?> targetType = key.targetType();
        RowInfo info = RowInfo.find(targetType);

        ClassMaker tableMaker = info.rowGen().beginClassMaker
            (MappedTable.class, targetType, "mapped").final_()
            .extend(MappedTable.class).implement(TableBasicsMaker.find(targetType));

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as references to the generated table instances still exist.
        tableMaker.addField(Object.class, "_").private_().final_();

        // All MappedTable instances will refer to the exact same cache.
        tableMaker.addField(QueryFactoryCache.class, "cache").private_().static_().final_()
            .initExact(new QueryFactoryCache());

        {
            MethodMaker ctor = tableMaker.addConstructor
                (MethodHandle.class, Table.class, Mapper.class).private_();
            ctor.field("_").set(ctor.param(0));
            ctor.invokeSuperConstructor(ctor.field("cache"), ctor.param(1), ctor.param(2));
        }

        addHasPrimaryKeyMethod(tableMaker, key.sourceType(), targetType, key.mapperClass());

        addMarkValuesUnset(key, info, tableMaker);

        MethodHandles.Lookup lookup = tableMaker.finishLookup();
        Class<?> tableClass = lookup.lookupClass();

        MethodMaker mm = MethodMaker.begin
            (lookup, MappedTable.class, null, MethodHandle.class, Table.class, Mapper.class);
        mm.return_(mm.new_(tableClass, mm.param(0), mm.param(1), mm.param(2)));

        return mm.finish();
    }

    private static void addMarkValuesUnset(FactoryKey key, RowInfo targetInfo, ClassMaker cm) {
        RowInfo sourceInfo = RowInfo.find(key.sourceType());

        // Map of target columns which have an inverse mapping to a source primary key column.
        Map<String, ColumnInfo> mapToSource = new HashMap<>();

        for (Method m : key.mapperClass().getMethods()) {
            if (!Modifier.isStatic(m.getModifiers())) {
                continue;
            }

            Class<?> retType = m.getReturnType();
            if (retType == null || retType == void.class) {
                continue;
            }

            Class<?>[] paramTypes = m.getParameterTypes();
            if (paramTypes.length != 1) {
                continue;
            }

            String name = m.getName();
            int ix = name.indexOf("_to_");
            if (ix <= 0) {
                continue;
            }

            String sourceName = RowMethodsMaker.unescape(name.substring(ix + "_to_".length()));

            if (ColumnSet.findColumn(sourceInfo.keyColumns, sourceName) == null) {
                continue;
            }

            String targetName = name.substring(0, ix);
            ColumnInfo target = targetInfo.allColumns.get(targetName);
            if (target != null) {
                mapToSource.put(targetName, target);
            }
        }

        if (mapToSource.isEmpty()) {
            return;
        }

        MethodMaker mm = cm.addMethod(null, "markValuesUnset", Object.class).protected_();
        var targetRowVar = mm.param(0).cast(RowMaker.find(key.targetType()));
        TableMaker.unset(targetInfo, targetRowVar, mapToSource);
    }

    private final QueryFactoryCache mQueryFactoryCache;

    private final Mapper<S, T> mMapper;

    private InverseMapper<S, T> mInversePk, mInverseFull, mInverseUpdate;

    protected MappedTable(QueryFactoryCache queryFactoryCache,
                          Table<S> source, Mapper<S, T> mapper)
    {
        super(source);
        mQueryFactoryCache = queryFactoryCache;
        mMapper = mapper;
    }

    @Override
    public final Scanner<T> newScanner(T targetRow, Transaction txn,
                                       String query, Object... args)
        throws IOException
    {
        return query(query).newScanner(targetRow, txn, args);
    }

    @Override
    public final Updater<T> newUpdater(Transaction txn) throws IOException {
        return newUpdater(txn, "{*}", (Object[]) null);
    }

    @Override
    public final Updater<T> newUpdater(Transaction txn, String query, Object... args)
        throws IOException
    {
        return query(query).newUpdater(txn, args);
    }

    @Override
    public final boolean tryLoad(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMapForLoad(mSource, targetRow);
        if (mSource.tryLoad(txn, sourceRow)) {
            T mappedRow = mMapper.map(sourceRow, newRow());
            if (mappedRow != null) {
                cleanRow(mappedRow);
                copyRow(mappedRow, targetRow);
                return true;
            }
        }
        markValuesUnset(targetRow);
        return false;
    }

    @Override
    public final boolean exists(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMapForLoad(mSource, targetRow);
        return mSource.tryLoad(txn, sourceRow) && mMapper.map(sourceRow, newRow()) != null;
    }

    @Override
    public final void store(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMap(mSource, targetRow);
        mMapper.checkStore(mSource, sourceRow);
        mSource.store(txn, sourceRow);
        cleanRow(targetRow);
    }

    @Override
    public final T exchange(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMap(mSource, targetRow);
        mMapper.checkStore(mSource, sourceRow);
        S oldSourceRow = mSource.exchange(txn, sourceRow);
        cleanRow(targetRow);
        if (oldSourceRow == null) {
            return null;
        }
        T oldTargetRow = mMapper.map(oldSourceRow, newRow());
        if (oldTargetRow != null) {
            cleanRow(oldTargetRow);
        }
        return oldTargetRow;
    }

    @Override
    public final boolean tryInsert(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMap(mSource, targetRow);
        mMapper.checkStore(mSource, sourceRow);
        if (!mSource.tryInsert(txn, sourceRow)) {
            return false;
        }
        cleanRow(targetRow);
        return true;
    }

    @Override
    public final boolean tryReplace(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMap(mSource, targetRow);
        mMapper.checkStore(mSource, sourceRow);
        if (!mSource.tryReplace(txn, sourceRow)) {
            return false;
        }
        cleanRow(targetRow);
        return true;
    }

    @Override
    public final boolean tryUpdate(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseUpdate().inverseMap(mSource, targetRow);
        mMapper.checkUpdate(mSource, sourceRow);
        if (!mSource.tryUpdate(txn, sourceRow)) {
            return false;
        }
        cleanRow(targetRow);
        return true;
    }

    @Override
    public final boolean tryMerge(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseUpdate().inverseMap(mSource, targetRow);
        mMapper.checkUpdate(mSource, sourceRow);
        if (!mSource.tryMerge(txn, sourceRow)) {
            return false;
        }
        T mappedRow = mMapper.map(sourceRow, newRow());
        if (mappedRow != null) {
            cleanRow(mappedRow);
            copyRow(mappedRow, targetRow);
        } else {
            // Can't load back the row. One option is to roll back the transaction, but then the
            // behavior would be inconsistent with the update operation. Unsetting all the
            // columns allows the operation to complete and signal that something is amiss.
            unsetRow(targetRow);
        }
        return true;
    }

    @Override
    public final boolean tryDelete(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMap(mSource, targetRow);
        mMapper.checkDelete(mSource, sourceRow);
        return mSource.tryDelete(txn, sourceRow);
    }

    @Override // MultiCache; see also WrappedTable
    @SuppressWarnings("unchecked")
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

    /**
     * All columns which don't map to source primary key columns are unset. Is overridden by
     * generated code if necessary.
     */
    protected void markValuesUnset(T targetRow) {
        unsetRow(targetRow);
    }

    /**
     * Returns an inverse mapper which requires that the primary key columns need to be set.
     */
    final InverseMapper<S, T> inversePk() {
        var invMapper = mInversePk;
        if (invMapper == null) {
            invMapper = makeInversePk();
        }
        return invMapper;
    }

    private synchronized InverseMapper<S, T> makeInversePk() {
        var invMapper = mInversePk;
        if (invMapper == null) {
            mInversePk = invMapper = makeInverseMapper(1);
        }
        return invMapper;
    }

    /**
     * Returns an inverse mapper which requires that all columns need to be set.
     */
    private InverseMapper<S, T> inverseFull() {
        var invMapper = mInverseFull;
        if (invMapper == null) {
            invMapper = makeInverseFull();
        }
        return invMapper;
    }

    private synchronized InverseMapper<S, T> makeInverseFull() {
        var invMapper = mInverseFull;
        if (invMapper == null) {
            mInverseFull = invMapper = makeInverseMapper(2);
        }
        return invMapper;
    }

    /**
     * Returns an inverse mapper which requires that the primary key columns need to be set,
     * and only dirty columns are updated.
     */
    final InverseMapper<S, T> inverseUpdate() {
        var invMapper = mInverseUpdate;
        if (invMapper == null) {
            invMapper = makeInverseUpdate();
        }
        return invMapper;
    }

    private synchronized InverseMapper<S, T> makeInverseUpdate() {
        var invMapper = mInverseUpdate;
        if (invMapper == null) {
            mInverseUpdate = invMapper = makeInverseMapper(3);
        }
        return invMapper;
    }

    /**
     * @param mode 1: pk, mode 2: full, mode 3: update
     */
    @SuppressWarnings("unchecked")
    private InverseMapper<S, T> makeInverseMapper(int mode) {
        RowInfo sourceInfo = RowInfo.find(mSource.rowType());
        RowInfo targetInfo = RowInfo.find(rowType());

        Map<String, ColumnInfo> sourceColumns;
        if (mode == 1) {
            // pk: only select sources that refer to the primary key
            sourceColumns = sourceInfo.keyColumns;
        } else {
            sourceColumns = sourceInfo.allColumns;
        }

        var finder = new InverseFinder(sourceColumns, inverseFunctions());

        // Maps source columns to the targets that map to it.
        var toTargetMap = new LinkedHashMap<String, Set<ColumnFunction>>();

        int matchedSources = 0;

        for (ColumnInfo targetColumn : targetInfo.allColumns.values()) {
            ColumnFunction source = finder.tryFindSource(targetColumn, false);

            if (source == null) {
                continue;
            }

            String sourceName;
            {
                ColumnInfo sourceColumn = source.column();
                if (sourceColumn == null) {
                    sourceName = null;
                } else {
                    matchedSources++;
                    sourceName = sourceColumn.name;
                }
            }

            Set<ColumnFunction> set = toTargetMap.get(sourceName);

            if (set == null) {
                set = new TreeSet<>((a, b) -> {
                    // Prefer untransformed mappers, so order them first.
                    if (a.isUntransformed()) {
                        if (!b.isUntransformed()) {
                            return -1;
                        }
                    } else if (b.isUntransformed()) {
                        return 1;
                    }

                    return a.column().name.compareTo(b.column().name);
                });

                toTargetMap.put(sourceName, set);
            }

            set.add(new ColumnFunction(targetColumn, source.function()));
        }

        if (mode < 3) {
            if (matchedSources != sourceColumns.size()) {
                // Not enough source columns have been mapped.
                return NoInverse.instance();
            }
        } else {
            // At least the source primary key columns must be mapped.
            for (String name : sourceInfo.keyColumns.keySet()) {
                if (!toTargetMap.containsKey(name)) {
                    return NoInverse.instance();
                }
            }
        }

        RowGen targetGen = targetInfo.rowGen();

        ClassMaker cm = targetGen.beginClassMaker
            (MappedTable.class, rowType(), null).final_().implement(InverseMapper.class);

        cm.addConstructor().private_();

        MethodMaker mm = cm.addMethod(null, "inverseMap", Object.class, Object.class);
        mm.public_();

        // Only attempt to check that the source columns are set if the source table type is
        // expected to have the special check methods defined.
        boolean checkSet = StoredTable.class.isAssignableFrom(mSource.getClass());

        Class<?> sourceRowType = mSource.rowType();
        if (checkSet) {
            // The special methods act upon the implementation class.
            sourceRowType = RowMaker.find(sourceRowType);
        }

        var targetRowVar = mm.param(1).cast(RowMaker.find(rowType()));
        var sourceRowVar = mm.param(0).cast(sourceRowType);

        Map<String, Integer> targetColumnNumbers = targetGen.columnNumbers();

        for (Map.Entry<String, Set<ColumnFunction>> e : toTargetMap.entrySet()) {
            String sourceColumnName = e.getKey();

            if (sourceColumnName == null) {
                // The inverse mapping function is of the form "target_to_" and is used to only
                // validate the target column value.
                if (mode > 1) for (ColumnFunction targetColumnFun : e.getValue()) {
                    ColumnInfo targetColumn = targetColumnFun.column();
                    var targetColumnVar = targetRowVar.field(targetColumn.name);
                    Method fun = targetColumnFun.function();
                    mm.var(fun.getDeclaringClass()).invoke(fun.getName(), targetColumnVar);
                }
                continue;
            }

            ColumnInfo sourceColumnInfo = ColumnSet.findColumn
                (sourceInfo.allColumns, sourceColumnName);

            Label nextSource = mm.label();

            // Assign the source column with the first target column which is set.

            Iterator<ColumnFunction> it = e.getValue().iterator();
            while (true) {
                ColumnFunction targetColumnFun = it.next();
                ColumnInfo targetColumn = targetColumnFun.column();

                int targetColumnNum = targetColumnNumbers.get(targetColumn.name);
                var stateVar = targetRowVar.field(targetGen.stateField(targetColumnNum));
                Label nextTarget = it.hasNext() ? mm.label() : nextSource;
                int stateMask = RowGen.stateFieldMask(targetColumnNum);

                if (mode < 3 || sourceInfo.keyColumns.containsKey(sourceColumnName)) {
                    stateVar.and(stateMask).ifEq(0, nextTarget);
                } else {
                    // Only update the column if it's dirty.
                    stateVar.and(stateMask).ifNe(stateMask, nextTarget);
                }

                var targetColumnVar = targetRowVar.field(targetColumn.name);
                Variable sourceColumnVar;

                if (!targetColumnFun.isUntransformed()) {
                    Method fun = targetColumnFun.function();
                    sourceColumnVar = mm.var
                        (fun.getDeclaringClass()).invoke(fun.getName(), targetColumnVar);
                } else {
                    sourceColumnVar = mm.var(sourceColumnInfo.type);
                    Converter.convertExact(mm, sourceColumnName,
                                           targetColumn, targetColumnVar,
                                           sourceColumnInfo, sourceColumnVar);
                }

                sourceRowVar.invoke(sourceColumnName, sourceColumnVar);

                if (!it.hasNext()) {
                    break;
                }

                nextSource.goto_();

                nextTarget.here();
            }

            nextSource.here();
        }

        if (checkSet) {
            var sourceVar = mm.var(mSource.getClass());
            Label ok = mm.label();

            if (mode == 2) { // full
                sourceVar.invoke("checkAllSet", sourceRowVar).ifTrue(ok);
                sourceVar.invoke("requireAllSet", sourceRowVar);
            } else {
                sourceVar.invoke("checkPrimaryKeySet", sourceRowVar).ifTrue(ok);
                mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();
            }

            ok.here();
        }

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (InverseMapper<S, T>) mh.invoke();
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
            (MappedTable.class, targetType, "query").final_().extend(BaseQuery.class);

        {
            MethodMaker mm = cm.addConstructor(MappedTable.class).private_();
            var tableVar = mm.param(0);
            QuerySpec sourceQuery = splitter.mSourceQuery;
            if (sourceQuery == null) {
                mm.invokeSuperConstructor(tableVar);
            } else {
                mm.invokeSuperConstructor(tableVar, sourceQuery.toString());
            }
        }

        RowFilter targetRemainder = splitter.mTargetRemainder;

        if (targetRemainder != TrueFilter.THE || targetQuery.projection() != null) {
            // Allow unbound query instances to serve as Mapper wrappers for supporting
            // predicate testing and projection.

            cm.implement(Mapper.class);

            MethodMaker mm = cm.addConstructor(Mapper.class, Object[].class).private_().varargs();
            mm.invokeSuperConstructor();

            cm.addField(Mapper.class, "mapper").private_().final_();
            mm.field("mapper").set(mm.param(0));

            if (targetRemainder != TrueFilter.THE) {
                cm.addField(Predicate.class, "predicate").private_().final_();
                MethodHandle mh = PlainPredicateMaker
                    .predicateHandle(targetType, targetRemainder.toString());
                mm.field("predicate").set(mm.invoke(mh, mm.param(1)));
            }

            mm = cm.addMethod(Object.class, "map", Object.class, Object.class).public_();

            var sourceRowVar = mm.param(0);
            var targetRowVar = mm.param(1);

            targetRowVar.set(mm.field("mapper").invoke("map", sourceRowVar, targetRowVar));

            Label done = mm.label();
            targetRowVar.ifEq(null, done);

            if (targetRemainder != TrueFilter.THE) {
                Label cont = mm.label();
                mm.field("predicate").invoke("test", targetRowVar).ifTrue(cont);
                mm.return_(null);
                cont.here();
            }

            Map<String, ColumnInfo> projection = targetQuery.projection();

            if (projection != null) {
                TableMaker.unset
                    (targetInfo, targetRowVar.cast(RowMaker.find(targetType)), projection);
            }

            done.here();
            mm.return_(targetRowVar);

            // Need to wrap the "check" methods which are used by MappedUpdater.

            mm = cm.addMethod(null, "checkUpdate", Table.class, Object.class).public_();
            mm.field("mapper").invoke("checkUpdate", mm.param(0), mm.param(1));

            mm = cm.addMethod(null, "checkDelete", Table.class, Object.class).public_();
            mm.field("mapper").invoke("checkDelete", mm.param(0), mm.param(1));
        }

        splitter.addPrepareArgsMethod(cm);

        for (int which = 1; which <= 2; which++) {
            String methodName = which == 1 ? "newScanner" : "newUpdater";

            MethodMaker mm = cm.addMethod
                (which == 1 ? Scanner.class : Updater.class, methodName,
                 Object.class, Transaction.class, Object[].class)
                .public_().varargs();

            var targetRowVar = mm.param(0);
            var txnVar = mm.param(1);
            Variable argsVar = mm.param(2);
            var tableVar = mm.field("table");

            SortPlan sortPlan = splitter.mSortPlan;

            if (which != 1 && sortPlan.sortOrder != null) {
                // Use a WrappedUpdater around a sorted Scanner.
                mm.return_(tableVar.invoke
                           ("newWrappedUpdater", mm.this_(), txnVar, targetRowVar, argsVar));
                continue;
            }

            argsVar = splitter.prepareArgs(argsVar);

            var mapperVar = tableVar.invoke("mapper");

            if (targetRemainder != TrueFilter.THE || targetQuery.projection() != null) {
                mapperVar.set(mm.new_(cm, mapperVar, argsVar));
            }

            var sourceScannerVar = mm.field("squery").invoke(methodName, txnVar, argsVar);

            Variable resultVar;

            if (sortPlan.sortOrder != null) {
                if (which != 1) {
                    throw new AssertionError();
                }

                var comparatorVar = mm.var(Comparator.class).setExact(sortPlan.sortComparator);

                Variable projectionVar = null;

                if (targetQuery.projection() != null) {
                    projectionVar = mm.var(Set.class).setExact
                        (SortedQueryLauncher.canonicalize(targetQuery.projection().keySet()));
                }

                resultVar = mm.new_(MappedScanner.class, tableVar,
                                    sourceScannerVar, targetRowVar, mapperVar);

                resultVar = tableVar.invoke("sort", resultVar, comparatorVar,
                                            projectionVar, sortPlan.sortOrderSpec);
            } else if (which == 1) {
                resultVar = mm.new_(MappedScanner.class, tableVar,
                                    sourceScannerVar, targetRowVar, mapperVar);
            } else {
                resultVar = mm.new_(MappedUpdater.class, tableVar,
                                    sourceScannerVar, targetRowVar, mapperVar);
            }

            mm.return_(resultVar);
        }

        // Add the plan method.

        {
            MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", boolean.class,
                                          Transaction.class, Object[].class)
                .protected_().varargs();

            var forUpdaterVar = mm.param(0);
            var txnVar = mm.param(1);
            var argsVar = splitter.prepareArgs(mm.param(2));
            var sourceQueryVar = mm.field("squery");

            final var planVar = mm.var(QueryPlan.class);

            Label forUpdater = mm.label();
            forUpdaterVar.ifTrue(forUpdater);
            planVar.set(sourceQueryVar.invoke("scannerPlan", txnVar, argsVar));
            Label ready = mm.label().goto_();
            forUpdater.here();
            planVar.set(sourceQueryVar.invoke("updaterPlan", txnVar, argsVar));
            ready.here();

            var targetVar = mm.var(Class.class).set(targetType).invoke("getName");
            var mapperVar = mm.field("table").invoke("mapper");

            var mapperPlanVar = mm.new_(QueryPlan.Mapper.class, targetVar, null, planVar);
            planVar.set(mapperVar.invoke("plan", mapperPlanVar));

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

        cm.addMethod(int.class, "argumentCount").public_()
            .return_(targetQuery.filter().maxArgument());

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated query class still exists.
        cm.addField(Object.class, "handle").private_().static_();

        return QueryFactoryCache.ctorHandle(cm.finishLookup(), MappedTable.class);
    }

    @Override
    protected String sourceProjection() {
        return mMapper.sourceProjection();
    }

    @Override
    protected Class<?> inverseFunctions() {
        return mMapper.getClass();
    }

    @Override
    protected SortPlan analyzeSort(InverseFinder finder, QuerySpec targetQuery) {
        OrderBy targetOrder = targetQuery.orderBy();

        var plan = new SortPlan();

        if (targetOrder == null) {
            return plan;
        }

        OrderBy sourceOrder = null;

        for (var rule : targetOrder.values()) {
            ColumnFunction source = finder.tryFindSource(rule.column(), true);
            if (source == null || !source.isUntransformed()) {
                break;
            }
            if (sourceOrder == null) {
                sourceOrder = new OrderBy();
            }
            ColumnInfo sourceColumn = source.column();
            sourceOrder.put(sourceColumn.name, new OrderBy.Rule(sourceColumn, rule.type()));
        }

        if (!mMapper.performsFiltering()
            && sourceOrder != null && sourceOrder.size() >= targetOrder.size())
        {
            // Can push the entire sort operation to the source.
            plan.sourceOrder = sourceOrder;
            return plan;
        }

        /*
          Apply the entire sort operation on the target scanner. It's possible for a partial
          ordering to be performed on the source, but it's a bit complicated. The key is to
          ensure that no sort operation is performed against the source. A double sort doesn't
          make any sense.

          For now partial ordering isn't supported, but here's what needs to happen:

          Examine the source query plan with the sourceOrder applied to it. If no sort is
          performed, then a partial sort is possible. Otherwise, examine the sort and remove
          the ordering columns that the sort applies to. Examine the surviving ordering
          columns, and keep the leading contiguous ones. If none remain, then no partial sort
          is possible. If a partial sort is still possible, examine the source query plan again
          to verify that no sort will be applied.

          The target sorter needs to be given the sourceComparator, which identifies row
          groups. The final sort only needs to apply these groups and not the whole set.
        */

        plan.sortOrder = targetOrder;
        String targetSpec = targetOrder.spec();
        plan.sortOrderSpec = targetSpec;
        plan.sortComparator = ComparatorMaker.comparator(rowType(), targetOrder, targetSpec);

        return plan;
    }

    /**
     * Called by generated Query instances.
     */
    public final Mapper<S, T> mapper() {
        return mMapper;
    }

    /**
     * Called by generated Query instances.
     *
     * @see SortedQueryLauncher#newUpdater
     */
    public final Updater<T> newWrappedUpdater(Query<T> query,
                                              Transaction txn, T targetRow, Object... args)
        throws IOException
    {
        if (txn != null) {
            if (txn.lockMode() != LockMode.UNSAFE) {
                txn.enter();
            }

            Scanner<T> scanner;
            try {
                scanner = query.newScanner(targetRow, txn, args);
                // Commit the transaction scope to promote and keep all the locks which were
                // acquired by the sort operation.
                txn.commit();
            } finally {
                txn.exit();
            }

            return new WrappedUpdater<>(this, txn, scanner);
        }

        // Need to create a transaction to acquire locks, but true auto-commit behavior isn't
        // really feasible because update order won't match lock acquisition order. In
        // particular, the locks cannot be released out of order. Instead, keep the transaction
        // open until the updater finishes and always commit. Unfortunately, if the commit
        // fails then all updates fail instead of just one.

        txn = newTransaction(null);

        Scanner<T> scanner;
        try {
            scanner = query.newScanner(targetRow, txn, args);
        } catch (Throwable e) {
            txn.exit();
            throw e;
        }

        return new WrappedUpdater.EndCommit<>(this, txn, scanner);
    }

    public static abstract class BaseQuery<S, T> implements Query<T> {
        protected final MappedTable<S, T> table;
        protected final Query<S> squery;

        protected BaseQuery(MappedTable<S, T> table) throws IOException {
            this.table = table;
            this.squery = table.mSource.queryAll();
        }

        protected BaseQuery(MappedTable<S, T> table, String queryStr) throws IOException {
            this.table = table;
            this.squery = table.mSource.query(queryStr);
        }

        // Used when Query is also a Mapper.
        protected BaseQuery() {
            this.table = null;
            this.squery = null;
        }

        @Override
        public final Class<T> rowType() {
            return table.rowType();
        }

        @Override
        public final QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
            return plan(false, txn, args);
        }

        @Override
        public final QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
            return plan(true, txn, args);
        }

        protected abstract QueryPlan plan(boolean forUpdater, Transaction txn, Object... args)
            throws IOException;
    }

    public interface InverseMapper<S, T> {
        /**
         * @param sourceRow non null
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         * @throws UnmodifiableViewException if inverse mapping isn't possible
         */
        void inverseMap(S sourceRow, T targetRow) throws IOException;

        /**
         * @param source only expected to be used for calling newRow
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         * @throws UnmodifiableViewException if inverse mapping isn't possible
         */
        default S inverseMap(Table<S> source, T targetRow) throws IOException {
            S sourceRow = source.newRow();
            inverseMap(sourceRow, targetRow);
            return sourceRow;
        }

        /**
         * @param source only expected to be used for calling newRow
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         * @throws ViewConstraintException if inverse mapping isn't possible
         */
        default S inverseMapForLoad(Table<S> source, T targetRow) throws IOException {
            return inverseMap(source, targetRow);
        }
    }

    public static class NoInverse implements InverseMapper {
        private static final NoInverse THE = new NoInverse();

        @SuppressWarnings("unchecked")
        public static <S, T> InverseMapper<S, T> instance() {
            return (InverseMapper<S, T>) THE;
        }

        private NoInverse() {
        }

        @Override
        public void inverseMap(Object sourceRow, Object targetRow) throws IOException {
            throw new UnmodifiableViewException();
        }

        @Override
        public Object inverseMap(Table source, Object targetRow) throws IOException {
            throw new UnmodifiableViewException();
        }

        @Override
        public Object inverseMapForLoad(Table source, Object targetRow) throws IOException {
            throw new ViewConstraintException();
        }
    }
}
