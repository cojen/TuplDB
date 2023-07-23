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
import java.lang.invoke.VarHandle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
    private static final WeakCache<Class<?>, Factory<?, ?>, Object> cCache;

    static {
        cCache = new WeakCache<>() {
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

    /* FIXME: InverseMappers are generated as hidden classes to support unloading. They should
       be softly cached to prevent premature unloading.
    */

    private volatile InverseMapper<S, T> mSourceRowPk, mSourceRowFull;

    private static final VarHandle cSourceRowPkHandle, cSourceRowFullHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cSourceRowPkHandle = lookup.findVarHandle
                (MappedTable.class, "mSourceRowPk", InverseMapper.class);
            cSourceRowFullHandle = lookup.findVarHandle
                (MappedTable.class, "mSourceRowFull", InverseMapper.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

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
        S sourceRow = sourceRowPk(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new ViewConstraintException();
        }
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
        S sourceRow = sourceRowPk(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new ViewConstraintException();
        }
        return mSource.exists(txn, sourceRow);
    }

    @Override
    public void store(Transaction txn, T targetRow) throws IOException {
        S sourceRow = sourceRowFull(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new UnmodifiableViewException();
        }
        mSource.store(txn, sourceRow);
        markAllUndirty(targetRow);
    }

    @Override
    public T exchange(Transaction txn, T targetRow) throws IOException {
        S sourceRow = sourceRowFull(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new UnmodifiableViewException();
        }
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
        S sourceRow = sourceRowFull(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new UnmodifiableViewException();
        }
        if (!mSource.insert(txn, sourceRow)) {
            return false;
        }
        markAllUndirty(targetRow);
        return true;
    }

    @Override
    public boolean replace(Transaction txn, T targetRow) throws IOException {
        S sourceRow = sourceRowFull(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new UnmodifiableViewException();
        }
        if (!mSource.replace(txn, sourceRow)) {
            return false;
        }
        markAllUndirty(targetRow);
        return true;
    }

    /*
    @Override
    public boolean update(Transaction txn, T targetRow) throws IOException {
        // FIXME: Need to perform an inverse mapping.
        throw null;
    }

    @Override
    public boolean merge(Transaction txn, T targetRow) throws IOException {
        // FIXME: Need to perform an inverse mapping.
        throw null;
    }
    */

    @Override
    public boolean delete(Transaction txn, T targetRow) throws IOException {
        S sourceRow = sourceRowPk(Objects.requireNonNull(targetRow));
        if (sourceRow == null) {
            throw new UnmodifiableViewException();
        }
        return mSource.delete(txn, sourceRow);
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, String query, Object... args) throws IOException {
        return mScannerFactoryCache.obtain(query, null).scannerPlan(this, txn, args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String query, Object... args) throws IOException {
        return mScannerFactoryCache.obtain(query, null).updaterPlan(this, txn, args);
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
     * Returns a new source row instance or null if inverse mapping isn't possible. Only the
     * primary key source columns need to be set.
     *
     * @throws IllegalStateException if the target primary key isn't fully specified
     */
    private S sourceRowPk(T targetRow) throws IOException {
        var invMapper = (InverseMapper<S, T>) cSourceRowPkHandle.getOpaque(this);
        if (invMapper == null) {
            // FIXME: generate and stash using setOpaque
            throw null;
        }
        return invMapper.inverseMap(mSource, targetRow);
    }

    /**
     * Returns a new source row instance or null if inverse mapping isn't possible. All source
     * row columns must be set.
     *
     * @throws IllegalStateException if any required target columns aren't set
     */
    private S sourceRowFull(T targetRow) throws IOException {
        var invMapper = (InverseMapper<S, T>) cSourceRowFullHandle.getOpaque(this);
        if (invMapper == null) {
            // FIXME: generate and stash using setOpaque
            throw null;
        }
        return invMapper.inverseMap(mSource, targetRow);
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
            Map<String, Mapper.SourceColumn> sourceMappers = new HashMap<>();
            Map<ArgMapper, Integer> argMappers;
            int maxArg;

            @Override
            public RowFilter apply(ColumnFilter cf) {
                String columnName = cf.column().name;
                Mapper.SourceColumn sourceMapper = sourceMapper(columnName);
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
                    Mapper.SourceColumn otherSourceMapper = sourceMapper(otherColumnName);
                    if (otherSourceMapper == null || otherSourceMapper.mapper() != null) {
                        return null;
                    }
                    return c2c.tryWithColumns(sourceRowColumns.get(columnName),
                                              sourceRowColumns.get(otherColumnName));
                }

                return null;
            }

            private Mapper.SourceColumn sourceMapper(String targetColumnName) {
                Mapper.SourceColumn sourceMapper = sourceMappers.get(targetColumnName);

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
            (MappedTable.class, targetType, "mapped").final_().implement(ScannerFactory.class);

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

        // Add the scannerPlan and updaterPlan methods, which call a common method.

        {
            MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", boolean.class,
                                          MappedTable.class, Transaction.class, Object[].class)
                .private_().varargs();

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

        {
            MethodMaker mm = cm.addMethod(QueryPlan.class, "scannerPlan",
                                          MappedTable.class, Transaction.class, Object[].class)
                .public_().varargs();
            mm.return_(mm.invoke("plan", false, mm.param(0), mm.param(1), mm.param(2)));
        }

        {
            MethodMaker mm = cm.addMethod(QueryPlan.class, "updaterPlan",
                                          MappedTable.class, Transaction.class, Object[].class)
                .public_().varargs();
            mm.return_(mm.invoke("plan", true, mm.param(0), mm.param(1), mm.param(2)));
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

        QueryPlan scannerPlan(MappedTable<S, T> table, Transaction txn, Object... args)
            throws IOException;

        QueryPlan updaterPlan(MappedTable<S, T> table, Transaction txn, Object... args)
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

    @FunctionalInterface
    public interface InverseMapper<S, T> {
        /**
         * @param source only expected to be used for calling newRow
         * @param targetRow non null
         * @throws IllegalStateException if any required target columns aren't set
         */
        S inverseMap(Table<S> source, T targetRow) throws IOException;
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
        public Object inverseMap(Table source, Object targetRow) {
            return null;
        }
    }
}
