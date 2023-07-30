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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.function.Function;
import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Mapper;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;
import org.cojen.tupl.Updater;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.filter.ColumnFilter;
import org.cojen.tupl.rows.filter.ColumnToArgFilter;
import org.cojen.tupl.rows.filter.ColumnToColumnFilter;
import org.cojen.tupl.rows.filter.ComplexFilterException;
import org.cojen.tupl.rows.filter.Parser;
import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.Query;
import org.cojen.tupl.rows.filter.TrueFilter;

/**
 * Base class for generated mapped tables.
 *
 * @author Brian S O'Neill
 * @see Table#map
 */
public abstract class MappedTable<S, T> implements Table<T> {
    private static final SoftCache<Class<?>, Factory<?, ?>, Object> cCache;

    static {
        cCache = new SoftCache<>() {
            @Override
            public Factory<?, ?> newValue(Class<?> targetType, Object unused) {
                return makeTableFactory(targetType);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <S, T> Factory<S, T> factory(Class<T> targetType) {
        return (Factory<S, T>) cCache.obtain(targetType, null);
    }

    final Table<S> mSource;
    final Mapper<S, T> mMapper;

    private final SoftCache<String, ScannerFactory<S, T>, Query> mScannerFactoryCache;

    private InverseMapper<S, T> mInversePk, mInverseFull, mInverseUpdate;

    protected MappedTable(Table<S> source, Mapper<S, T> mapper) {
        mSource = source;
        mMapper = mapper;

        mScannerFactoryCache = new SoftCache<>() {
            @Override
            protected ScannerFactory<S, T> newValue(String queryStr, Query query) {
                if (query == null) {
                    RowInfo rowInfo = RowInfo.find(rowType());
                    query = new Parser(rowInfo.allColumns, queryStr).parseQuery(null);
                    String canonical = query.toString();
                    if (!canonical.equals(queryStr)) {
                        return obtain(canonical, query);
                    }
                }
                return makeScannerFactory(query);
            }
        };
    }

    @Override
    public Scanner<T> newScanner(Transaction txn) throws IOException {
        return new MappedScanner<>(this, mSource.newScanner(txn), null, mMapper);
    }

    @Override
    public Scanner<T> newScannerWith(Transaction txn, T targetRow) throws IOException {
        return new MappedScanner<>(this, mSource.newScanner(txn), targetRow, mMapper);
    }

    @Override
    public Scanner<T> newScanner(Transaction txn, String query, Object... args)
        throws IOException
    {
        return mScannerFactoryCache.obtain(query, null).newScannerWith(this, txn, null, args);
    }

    @Override
    public Scanner<T> newScannerWith(Transaction txn, T targetRow, String query, Object... args)
        throws IOException
    {
        return mScannerFactoryCache.obtain(query, null).newScannerWith(this, txn, targetRow, args);
    }

    /*
    @Override
    public Updater<T> newUpdater(Transaction txn) throws IOException {
        // FIXME: newUpdater
        throw null;
    }

    @Override
    public Updater<T> newUpdater(Transaction txn, String query, Object... args)
        throws IOException
    {
        // FIXME: newUpdater
        throw null;
    }
    */

    @Override
    public final Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public final boolean isEmpty() throws IOException {
        return mSource.isEmpty() || !anyRows(Transaction.BOGUS);
    }

    @Override
    public boolean load(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMap(mSource, targetRow);
        if (mSource.load(txn, sourceRow)) {
            unsetRow(targetRow);
            mMapper.map(sourceRow, targetRow);
            markAllUndirty(targetRow);
            return true;
        }
        return false;
    }

    @Override
    public boolean exists(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMap(mSource, targetRow);
        return mSource.exists(txn, sourceRow);
    }

    @Override
    public void store(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMapForWrite(mSource, targetRow);
        mSource.store(txn, sourceRow);
        markAllUndirty(targetRow);
    }

    @Override
    public T exchange(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMapForWrite(mSource, targetRow);
        S oldSourceRow = mSource.exchange(txn, sourceRow);
        markAllUndirty(targetRow);
        if (oldSourceRow == null) {
            return null;
        }
        T oldTargetRow = newRow();
        mMapper.map(oldSourceRow, oldTargetRow);
        markAllUndirty(oldTargetRow);
        return oldTargetRow;
    }

    @Override
    public boolean insert(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMapForWrite(mSource, targetRow);
        if (!mSource.insert(txn, sourceRow)) {
            return false;
        }
        markAllUndirty(targetRow);
        return true;
    }

    @Override
    public boolean replace(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseFull().inverseMapForWrite(mSource, targetRow);
        if (!mSource.replace(txn, sourceRow)) {
            return false;
        }
        markAllUndirty(targetRow);
        return true;
    }

    @Override
    public boolean update(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseUpdate().inverseMapForWrite(mSource, targetRow);
        if (!mSource.update(txn, sourceRow)) {
            return false;
        }
        markAllUndirty(targetRow);
        return true;
    }

    @Override
    public boolean merge(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inverseUpdate().inverseMapForWrite(mSource, targetRow);
        if (!mSource.merge(txn, sourceRow)) {
            return false;
        }
        mMapper.map(sourceRow, targetRow);
        markAllUndirty(targetRow);
        return true;
    }

    @Override
    public boolean delete(Transaction txn, T targetRow) throws IOException {
        Objects.requireNonNull(targetRow);
        S sourceRow = inversePk().inverseMapForWrite(mSource, targetRow);
        return mSource.delete(txn, sourceRow);
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, String query, Object... args) throws IOException {
        return mScannerFactoryCache.obtain(query, null).plan(false, this, txn, args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String query, Object... args) throws IOException {
        return mScannerFactoryCache.obtain(query, null).plan(true, this, txn, args);
    }

    @Override
    public final void close() throws IOException {
        // Do nothing.
    }

    @Override
    public final boolean isClosed() {
        return false;
    }

    /**
     * All columns which are dirty are to be marked clean, and unset columns remain as such.
     */
    protected abstract void markAllUndirty(T targetRow);

    /**
     * Returns an inverse mapper which requires that the primary key columns need to be set.
     */
    private InverseMapper<S, T> inversePk() {
        var invMapper = (InverseMapper<S, T>) mInversePk;
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
        var invMapper = (InverseMapper<S, T>) mInverseFull;
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
    private InverseMapper<S, T> inverseUpdate() {
        var invMapper = (InverseMapper<S, T>) mInverseUpdate;
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
    private InverseMapper<S, T> makeInverseMapper(int mode) {
        RowInfo sourceInfo = RowInfo.find(mSource.rowType());
        RowInfo targetInfo = RowInfo.find(rowType());

        // Maps source columns to the targets that map to it.
        var toTargetMap = new LinkedHashMap<String, Set<Mapper.Column>>();

        for (ColumnInfo targetColumn : targetInfo.allColumns.values()) {
            Mapper.Column source;
            try {
                source = mMapper.sourceColumn(targetColumn.name);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }

            if (source == null) {
                continue;
            }

            ColumnInfo sourceColumn = sourceInfo.allColumns.get(source.name());

            if (sourceColumn == null) {
                throw new IllegalStateException
                    ("Target column \"" + targetColumn.name + "\" maps to a nonexistent " +
                     "source column: " + source.name());
            }

            // pk: only select sources that refer to the primary key
            if (mode == 1 && !sourceInfo.keyColumns.containsKey(source.name())) {
                continue;
            }

            MethodHandle mapper = source.mapper();

            if (mapper != null) {
                MethodType mt = mapper.type();
                if (mt.returnType() == void.class || mt.parameterCount() != 1) {
                    throw new IllegalStateException
                        ("Source column mapper isn't a function: " + source);
                }
                if (!mt.parameterType(0).isAssignableFrom(targetColumn.type)) {
                    throw new IllegalStateException
                        ("Source column mapper parameter type mismatch: " + source + "; " +
                         targetColumn.type.getName());
                }
                if (!sourceColumn.type.isAssignableFrom(mt.returnType())) {
                    throw new IllegalStateException
                        ("Source column mapper return type mismatch: " + source + "; " +
                         sourceColumn.type.getName());
                }
            }

            Set<Mapper.Column> set = toTargetMap.get(source.name());

            if (set == null) {
                set = new TreeSet<>((a, b) -> {
                    // Prefer identity mappers, so order them first.
                    if (a.mapper() == null) {
                        if (b.mapper() != null) {
                            return -1;
                        }
                    } else if (b.mapper() == null) {
                        return 1;
                    }

                    return a.name().compareTo(b.name());
                });

                toTargetMap.put(source.name(), set);
            }

            set.add(new Mapper.Column(targetColumn.name, mapper));
        }

        if (mode < 3) {
            int expect = mode == 1 ? sourceInfo.keyColumns.size() : sourceInfo.allColumns.size();
            if (toTargetMap.size() != expect) {
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

        MethodMaker mm = cm.addMethod(Object.class, "inverseMap", Table.class, Object.class);
        mm.public_();

        var targetRowVar = mm.param(1).cast(RowMaker.find(rowType()));
        var sourceRowVar = mm.param(0).invoke("newRow").cast(mSource.rowType());

        Map<String, Integer> targetColumnNumbers = targetGen.columnNumbers();

        for (Map.Entry<String, Set<Mapper.Column>> e : toTargetMap.entrySet()) {
            String sourceColumnName = e.getKey();
            ColumnInfo sourceColumnInfo = sourceInfo.allColumns.get(sourceColumnName);

            Label nextSource = mm.label();

            // Assign the source column with the first target column which is set.

            Iterator<Mapper.Column> it = e.getValue().iterator();
            while (true) {
                Mapper.Column targetColumn = it.next();

                ColumnInfo targetColumnInfo = targetInfo.allColumns.get(targetColumn.name());

                int targetColumnNum = targetColumnNumbers.get(targetColumn.name());
                var stateVar = targetRowVar.field(targetGen.stateField(targetColumnNum));
                Label nextTarget = it.hasNext() ? mm.label() : nextSource;
                int stateMask = RowGen.stateFieldMask(targetColumnNum);

                if (mode < 3 || sourceInfo.keyColumns.containsKey(sourceColumnName)) {
                    stateVar.and(stateMask).ifEq(0, nextTarget);
                } else {
                    // Only update the column if it's dirty.
                    stateVar.and(stateMask).ifNe(stateMask, nextTarget);
                }

                var targetColumnVar = targetRowVar.field(targetColumn.name());
                Variable sourceColumnVar;

                MethodHandle mh = targetColumn.mapper();

                if (mh != null) {
                    sourceColumnVar = mm.invoke(mh, targetColumnVar);
                } else {
                    sourceColumnVar = mm.var(sourceColumnInfo.type);
                    Converter.convertExact(mm, sourceColumnName,
                                           targetColumnInfo, targetColumnVar,
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

        mm.return_(sourceRowVar);

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (InverseMapper<S, T>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private static <S, T> Factory<S, T> makeTableFactory(Class<T> targetType) {
        RowInfo info = RowInfo.find(targetType);

        ClassMaker tableMaker = info.rowGen().beginClassMaker
            (MappedTable.class, targetType, "mapped").final_()
            .extend(MappedTable.class).implement(TableBasicsMaker.find(targetType));

        {
            MethodMaker ctor = tableMaker.addConstructor(Table.class, Mapper.class);
            ctor.invokeSuperConstructor(ctor.param(0), ctor.param(1));
        }

        // Keep a reference to the singleton factory instance, to prevent the factory from
        // being garbage collected as long as the generated table class still exists.
        tableMaker.addField(Factory.class, "_").static_();

        // Add the markAllUndirty method.
        {
            MethodMaker mm = tableMaker.addMethod(null, "markAllUndirty", Object.class);
            mm.protected_();
            TableMaker.markAllUndirty(mm.param(0).cast(RowMaker.find(targetType)), info);
        }

        Class<?> tableClass = tableMaker.finish();

        ClassMaker factoryMaker = info.rowGen().beginClassMaker
            (MappedTable.class, targetType, null).final_().implement(Factory.class);

        {
            MethodMaker ctor = factoryMaker.addConstructor();
            ctor.invokeSuperConstructor();
            // Assign the singleton reference.
            ctor.var(tableClass).field("_").set(ctor.this_());
        }

        {
            MethodMaker mm = factoryMaker.addMethod
                (MappedTable.class, "make", Table.class, Mapper.class).public_();
            mm.return_(mm.new_(tableClass, mm.param(0), mm.param(1)));
        }

        try {
            MethodHandles.Lookup lookup = factoryMaker.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Factory<S, T>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    public interface Factory<S, T> {
        MappedTable<S, T> make(Table<S> source, Mapper<S, T> mapper);
    }

    private ScannerFactory<S, T> makeScannerFactory(Query targetQuery) {
        final RowFilter targetFilter;
        {
            RowFilter filter = targetQuery.filter();
            try {
                filter = filter.cnf();
            } catch (ComplexFilterException e) {
                // The split won't be as effective, and so the remainder mapper will do more work.
            }
            targetFilter = filter;
        }

        record ArgMapper(int targetArgNum, MethodHandle sourceMapper) { } 

        var checker = new Function<ColumnFilter, RowFilter>() {
            Map<String, ColumnInfo> sourceRowColumns = RowInfo.find(mSource.rowType()).allColumns;
            Map<String, Mapper.Column> sourceMappers = new HashMap<>();
            Map<ArgMapper, Integer> argMappers;
            int maxArg;

            @Override
            public RowFilter apply(ColumnFilter cf) {
                String columnName = cf.column().name;
                Mapper.Column sourceMapper = sourceMapper(columnName);
                if (sourceMapper == null) {
                    return null;
                }

                if (cf instanceof ColumnToArgFilter c2a) {
                    c2a = c2a.withColumn(sourceRowColumns.get(sourceMapper.name()));
                    if (sourceMapper.mapper() == null) {
                        return c2a;
                    }
                    if (argMappers == null) {
                        argMappers = new HashMap<>();
                        maxArg = targetFilter.maxArgument();
                    }
                    var argMapper = new ArgMapper(c2a.argument(), sourceMapper.mapper());
                    Integer sourceArg = argMappers.get(argMapper);
                    if (sourceArg == null) {
                        sourceArg = ++maxArg;
                        argMappers.put(argMapper, sourceArg);
                    }
                    return c2a.withArgument(sourceArg);
                } else if (cf instanceof ColumnToColumnFilter c2c) {
                    // Can only convert to a source filter when both columns rely on an
                    // identity mapping, and a common type exists. It's unlikely that a common
                    // type doesn't exist.
                    if (sourceMapper.mapper() != null) {
                        return null;
                    }
                    String otherColumnName = c2c.otherColumn().name;
                    Mapper.Column otherSourceMapper = sourceMapper(otherColumnName);
                    if (otherSourceMapper == null || otherSourceMapper.mapper() != null) {
                        return null;
                    }
                    return c2c.tryWithColumns(sourceRowColumns.get(columnName),
                                              sourceRowColumns.get(otherColumnName));
                }

                return null;
            }

            private Mapper.Column sourceMapper(String targetColumnName) {
                Mapper.Column sourceMapper = sourceMappers.get(targetColumnName);

                if (sourceMapper == null && !sourceMappers.containsKey(targetColumnName)) {
                    try {
                        sourceMapper = mMapper.sourceColumn(targetColumnName);
                    } catch (ReflectiveOperationException e) {
                        throw new IllegalStateException(e);
                    }
                    if (!sourceRowColumns.containsKey(sourceMapper.name())) {
                        throw new IllegalStateException
                            ("Target column \"" + targetColumnName + "\" maps to a nonexistent " +
                             "source column: " + sourceMapper.name());
                    }
                    sourceMappers.put(targetColumnName, sourceMapper);
                }

                return sourceMapper;
            }

            void addPrepareArgsMethod(ClassMaker cm) {
                if (argMappers == null) {
                    // No special method is needed.
                    return;
                }

                MethodMaker mm = cm.addMethod(Object[].class, "prepareArgs", Object[].class)
                    .varargs().static_().final_();

                // Generate the necessary source query arguments from the provided target
                // arguments.

                Label ready = mm.label();
                var argsVar = mm.param(0);
                argsVar.ifEq(null, ready);

                argsVar.set(mm.var(Arrays.class).invoke("copyOf", argsVar, maxArg));

                for (Map.Entry<ArgMapper, Integer> e : argMappers.entrySet()) {
                    ArgMapper argMapper = e.getKey();
                    MethodHandle sourceMapper = argMapper.sourceMapper();
                    var argVar = argsVar.aget(argMapper.targetArgNum - 1);
                    Label cont = mm.label();
                    argVar.ifEq(null, cont);
                    var targetArgVar = ConvertCallSite.make
                        (mm, sourceMapper.type().parameterType(0), argVar);
                    var sourceArgVar = mm.invoke(sourceMapper, targetArgVar);
                    argsVar.aset(e.getValue() - 1, sourceArgVar);

                    cont.here();
                }

                ready.here();
                mm.return_(argsVar);
            }

            Variable prepareArgs(Variable argsVar) {
                if (argMappers == null) {
                    // No special method was defined.
                    return argsVar;
                } else {
                    return argsVar.methodMaker().invoke("prepareArgs", argsVar);
                }
            }
        };

        var split = new RowFilter[2];
        targetFilter.split(checker, split);

        /* FIXME: Projection and ordering will require special handling.

           Query projection needs renames, and any remainder requires post projection.

           If query is only renames, push the sort to the source. Otherwise, due a post sort.

           Define a new sorter class that can write resolved row objects to temp indexes when a
           size threshold is reached. This can also be used for joins.
         */
        Query sourceQuery = targetQuery.withFilter(split[0]);

        RowFilter targetRemainder = split[1];

        Class<T> targetType = rowType();
        RowInfo info = RowInfo.find(targetType);

        ClassMaker cm = info.rowGen().beginClassMaker
            (MappedTable.class, targetType, null).final_().implement(ScannerFactory.class);

        cm.addConstructor().private_();

        checker.addPrepareArgsMethod(cm);

        {
            MethodMaker mm = cm.addMethod
                (Scanner.class, "newScannerWith",
                 MappedTable.class, Transaction.class, Object.class, Object[].class)
                .public_().varargs();

            var tableVar = mm.param(0);
            var txnVar = mm.param(1);
            var targetRowVar = mm.param(2);
            var argsVar = checker.prepareArgs(mm.param(3));

            var mapperVar = tableVar.invoke("mapper");

            if (targetRemainder != TrueFilter.THE) {
                MethodHandle mh = PlainPredicateMaker
                    .predicateHandle(targetType, targetRemainder.toString());
                mapperVar = mm.new_(PredicateMapper.class, mapperVar, mm.invoke(mh, argsVar));
            }

            var sourceTableVar = tableVar.invoke("source");
            Variable sourceScannerVar;

            if (sourceQuery.filter() == TrueFilter.THE) {
                sourceScannerVar = sourceTableVar.invoke("newScanner", txnVar);
            } else {
                sourceScannerVar = sourceTableVar.invoke
                    ("newScanner", txnVar, sourceQuery.toString(), argsVar);
            }

            mm.return_(mm.new_(MappedScanner.class, tableVar,
                               sourceScannerVar, targetRowVar, mapperVar));
        }

        // Add the plan method.

        {
            MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", boolean.class,
                                          MappedTable.class, Transaction.class, Object[].class)
                .public_().varargs();

            var forUpdaterVar = mm.param(0);
            var tableVar = mm.param(1);
            var txnVar = mm.param(2);
            var argsVar = checker.prepareArgs(mm.param(3));

            String sourceQueryStr =
                sourceQuery.filter() == TrueFilter.THE ? null : sourceQuery.toString();

            var sourceTableVar = tableVar.invoke("source");
            var planVar = mm.var(QueryPlan.class);

            Label forUpdater = mm.label();
            forUpdaterVar.ifTrue(forUpdater);
            planVar.set(sourceTableVar.invoke("scannerPlan", txnVar, sourceQueryStr, argsVar));
            Label ready = mm.label().goto_();
            forUpdater.here();
            planVar.set(sourceTableVar.invoke("updaterPlan", txnVar, sourceQueryStr, argsVar));
            ready.here();

            var targetVar = mm.var(Class.class).set(targetType).invoke("getName");
            var usingVar = tableVar.invoke("mapper").invoke("toString");

            planVar = mm.new_(QueryPlan.Mapper.class, targetVar, usingVar, planVar);

            if (targetRemainder != TrueFilter.THE) {
                planVar = mm.new_(QueryPlan.Filter.class, targetRemainder.toString(), planVar);
            }

            mm.return_(planVar);
        }

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (ScannerFactory<S, T>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    public interface ScannerFactory<S, T> {
        Scanner<T> newScannerWith(MappedTable<S, T> table,
                                  Transaction txn, T targetRow, Object... args)
            throws IOException;

        QueryPlan plan(boolean forUpdater, MappedTable<S, T> table, Transaction txn, Object... args)
            throws IOException;
    }

    /**
     * Called by the generated ScannerFactory.
     */
    public final Table<S> source() {
        return mSource;
    }

    /**
     * Called by the generated ScannerFactory.
     */
    public final Mapper<S, T> mapper() {
        return mMapper;
    }

    /**
     * Tests a predicate after mapping a source row to a target row.
     */
    public static class PredicateMapper<S, T> implements Mapper<S, T> {
        private final Mapper<S, T> mMapper;
        private final Predicate<T> mPredicate;

        public PredicateMapper(Mapper<S, T> mapper, Predicate<T> predicate) {
            mMapper = mapper;
            mPredicate = predicate;
        }

        public T map(S source, T target) throws IOException {
            target = mMapper.map(source, target);
            return mPredicate.test(target) ? target : null;
        }
    }

    public interface InverseMapper<S, T> {
        /**
         * @param source only expected to be used for calling newRow
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         * @throws ViewConstraintException if inverse mapping isn't possible
         */
        S inverseMap(Table<S> source, T targetRow) throws IOException;

        /**
         * @param source only expected to be used for calling newRow
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         * @throws UnmodifiableViewException if inverse mapping isn't possible
         */
        default S inverseMapForWrite(Table<S> source, T targetRow) throws IOException {
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
        public Object inverseMap(Table source, Object targetRow) throws IOException {
            throw new ViewConstraintException();
        }

        @Override
        public Object inverseMapForWrite(Table source, Object targetRow) throws IOException {
            throw new UnmodifiableViewException();
        }
    }
}
