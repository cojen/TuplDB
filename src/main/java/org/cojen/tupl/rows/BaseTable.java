/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicate;
import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;
import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.filter.ComplexFilterException;
import org.cojen.tupl.filter.FalseFilter;
import org.cojen.tupl.filter.FullFilter;
import org.cojen.tupl.filter.Parser;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.TrueFilter;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;

import org.cojen.tupl.views.ViewUtils;

/**
 * Base class for all generated table classes.
 *
 * @author Brian S O'Neill
 */
public abstract class BaseTable<R> implements Table<R>, ScanControllerFactory<R> {
    // Need a strong reference to this to prevent premature GC.
    final TableManager<R> mTableManager;

    protected final Index mSource;

    private final SoftCache<String, ScanControllerFactory<R>> mFilterFactoryCache;
    private final SoftCache<String, ScanControllerFactory<R>> mFilterFactoryCacheDoubleCheck;

    private HashMap<String, Latch> mFilterLatchMap;

    private Trigger<R> mTrigger;
    private static final VarHandle cTriggerHandle;

    private WeakCache<String, Comparator<R>> mComparatorCache;
    private static final VarHandle cComparatorCacheHandle;

    // Is null if unsupported.
    protected final RowPredicateLock<R> mIndexLock;

    private WeakCache<Object, MethodHandle> mPartialDecodeCache;
    private static final VarHandle cPartialDecodeCacheHandle;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            cTriggerHandle = lookup.findVarHandle
                (BaseTable.class, "mTrigger", Trigger.class);
            cComparatorCacheHandle = lookup.findVarHandle
                (BaseTable.class, "mComparatorCache", WeakCache.class);
            cPartialDecodeCacheHandle = lookup.findVarHandle
                (BaseTable.class, "mPartialDecodeCache", WeakCache.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param indexLock is null if unsupported
     */
    protected BaseTable(TableManager<R> manager, Index source, RowPredicateLock<R> indexLock) {
        mTableManager = manager;

        mSource = Objects.requireNonNull(source);

        mFilterFactoryCache = new SoftCache<>();
        mFilterFactoryCacheDoubleCheck =
            joinedPrimaryTableClass() == null ? null : new SoftCache<>();

        if (supportsSecondaries()) {
            var trigger = new Trigger<R>();
            trigger.mMode = Trigger.SKIP;
            cTriggerHandle.setRelease(this, trigger);
        }

        mIndexLock = indexLock;
    }

    public final TableManager<R> tableManager() {
        return mTableManager;
    }

    @Override
    public final RowScanner<R> newRowScanner(Transaction txn) throws IOException {
        return newRowScanner(txn, unfiltered());
    }

    @Override
    public final RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowScanner(txn, scannerFilteredFactory(txn, filter).scanController(args));
    }

    RowScanner<R> newRowScanner(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        final BasicRowScanner<R> scanner;
        RowPredicateLock.Closer closer = null;

        if (txn == null && controller instanceof JoinedScanController) {
            txn = mSource.newTransaction(null);
            txn.lockMode(LockMode.REPEATABLE_READ);
            scanner = new AutoCommitRowScanner<>(this, controller);
        } else {
            scanner = new BasicRowScanner<>(this, controller);

            if (txn != null && !txn.lockMode().noReadLock) {
                RowPredicateLock<R> lock = mIndexLock;
                if (lock != null) {
                    // This case is reached when a transaction was provided which is read
                    // committed or higher. Adding a predicate lock prevents new rows from
                    // being inserted into the scan range for the duration of the transaction
                    // scope. If the lock mode is repeatable read, then rows which have been
                    // read cannot be deleted, effectively making the transaction serializable.
                    closer = lock.addPredicate(txn, controller.predicate());
                }
            }
        }

        try {
            scanner.init(txn);
            return scanner;
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    ScanControllerFactory<R> scannerFilteredFactory(Transaction txn, String filter) {
        SoftCache<String, ScanControllerFactory<R>> cache;
        // Need to double check the filter after joining to the primary, in case there were any
        // changes after the secondary entry was loaded.
        if (!RowUtils.isUnlocked(txn) || (cache = mFilterFactoryCacheDoubleCheck) == null) {
            cache = mFilterFactoryCache;
        }
        ScanControllerFactory<R> factory = cache.get(filter);
        if (factory == null) {
            factory = findFilteredFactory(cache, filter, null);
        }
        return factory;
    }

    @Override
    public final RowUpdater<R> newRowUpdater(Transaction txn) throws IOException {
        return newRowUpdater(txn, unfiltered());
    }

    @Override
    public final RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowUpdater(txn, updaterFilteredFactory(txn, filter).scanController(args));
    }

    protected RowUpdater<R> newRowUpdater(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        return newRowUpdater(txn, controller, null);
    }

    /**
     * @param secondary non-null if joining from a secondary index to this primary table
     */
    protected RowUpdater<R> newRowUpdater(Transaction txn, ScanController<R> controller,
                                          BaseTableIndex<R> secondary)
        throws IOException
    {
        final BasicRowUpdater<R> updater;
        RowPredicateLock.Closer closer = null;

        addPredicate: {

            if (txn == null) {
                txn = mSource.newTransaction(null);
                updater = new AutoCommitRowUpdater<>(this, controller);
                // Don't add a predicate lock.
                break addPredicate;
            }

            switch (txn.lockMode()) {
            case UPGRADABLE_READ: default: {
                updater = new BasicRowUpdater<>(this, controller);
                break;
            }

            case REPEATABLE_READ: {
                // Need to use upgradable locks to prevent deadlocks.
                updater = new UpgradableRowUpdater<>(this, controller);
                break;
            }

            case READ_COMMITTED: {
                // Row locks are released when possible, but a predicate lock will still be
                // held for the duration of the transaction. It's not worth the trouble to
                // determine if it can be safely released when the updater finishes.
                updater = new NonRepeatableRowUpdater<>(this, controller);
                break;
            }

            case READ_UNCOMMITTED:
                updater = new NonRepeatableRowUpdater<>(this, controller);
                // Don't add a predicate lock.
                break addPredicate;

            case UNSAFE:
                updater = new BasicRowUpdater<>(this, controller);
                // Don't add a predicate lock.
                break addPredicate;
            }

            RowPredicateLock<R> lock = secondary == null ? mIndexLock : secondary.mIndexLock;
            if (lock == null) {
                break addPredicate;
            }

            // This case is reached when a transaction was provided which is read committed
            // or higher. Adding a predicate lock prevents new rows from being inserted
            // into the scan range for the duration of the transaction scope. If the lock
            // mode is repeatable read, then rows which have been read cannot be deleted,
            // effectively making the transaction serializable.
            closer = lock.addPredicate(txn, controller.predicate());
        }

        try {
            if (secondary == null) {
                updater.init(txn);
                return updater;
            } else {
                var joined = new JoinedRowUpdater<>(secondary, controller, updater);
                joined.init(txn);
                return joined;
            }
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    ScanControllerFactory<R> updaterFilteredFactory(Transaction txn, String filter) {
        SoftCache<String, ScanControllerFactory<R>> cache;
        // Need to double check the filter after joining to the primary, in case there were any
        // changes after the secondary entry was loaded. Note that no double check is needed
        // with READ_UNCOMMITTED, because the updater for it still acquires locks.
        if (!RowUtils.isUnsafe(txn) || (cache = mFilterFactoryCacheDoubleCheck) == null) {
            cache = mFilterFactoryCache;
        }
        ScanControllerFactory<R> factory = cache.get(filter);
        if (factory == null) {
            factory = findFilteredFactory(cache, filter, null);
        }
        return factory;
    }

    @Override
    public final String toString() {
        var b = new StringBuilder();
        RowUtils.appendMiniString(b, this);
        b.append('{');
        b.append("rowType").append(": ").append(rowType().getName());
        b.append(", ").append("primaryIndex").append(": ").append(mSource);
        return b.append('}').toString();
    }

    @Override
    public final Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public final boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Comparator<R> comparator(String spec) {
        WeakCache<String, Comparator<R>> cache = mComparatorCache;

        if (cache == null) {
            cache = new WeakCache<>();
            var existing = (WeakCache<String, Comparator<R>>)
                cComparatorCacheHandle.compareAndExchange(this, null, cache);
            if (existing != null) {
                cache = existing;
            }
        }

        Comparator<R> comparator = cache.get(spec);

        if (comparator == null) {
            synchronized (cache) {
                comparator = makeComparator(cache, spec);
            }
        }

        return comparator;
    }

    private Comparator<R> makeComparator(WeakCache<String, Comparator<R>> cache, String spec) {
        Comparator<R> comparator = cache.get(spec);

        if (comparator == null) {
            var maker = new ComparatorMaker<R>(rowType(), spec);
            String clean = maker.cleanRules();
            if (spec.equals(clean)) {
                comparator = maker.finish();
            } else {
                comparator = makeComparator(cache, clean);
            }
            cache.put(spec, comparator);
        }

        return comparator;
    }

    @Override
    public final RowPredicate<R> predicate(String filter, Object... args) {
        if (filter == null) {
            return RowPredicate.all();
        }
        ScanControllerFactory<R> factory = mFilterFactoryCache.get(filter);
        if (factory == null) {
            factory = findFilteredFactory(mFilterFactoryCache, filter, null);
        }
        return factory.predicate(args);
    }

    @Override
    public Table<R> viewAlternateKey(String... columns) throws IOException {
        return viewIndexTable(true, columns);
    }

    @Override
    public Table<R> viewSecondaryIndex(String... columns) throws IOException {
        return viewIndexTable(false, columns);
    }

    private Table<R> viewIndexTable(boolean alt, String... columns) throws IOException {
        var rs = rowStoreRef().get();
        if (rs == null) {
            throw new DatabaseException("Closed");
        }
        return rs.indexTable(this, alt, columns);
    }

    @Override
    public BaseTable<R> viewUnjoined() {
        return this;
    }

    @Override
    public Table<R> viewReverse() {
        return new ReverseTable<R>(this);
    }

    @Override
    public final QueryPlan queryPlan(Transaction txn, String filter, Object... args) {
        if (filter == null) {
            return plan(args);
        } else {
            return scannerFilteredFactory(txn, filter).plan(args);
        }
    }

    @Override // ScanControllerFactory
    public final ScanControllerFactory<R> reverse() {
        return new ScanControllerFactory<R>() {
            @Override
            public QueryPlan plan(Object... args) {
                return planReverse(args);
            }

            @Override
            public ScanControllerFactory<R> reverse() {
                return BaseTable.this;
            }

            @Override
            public RowPredicate<R> predicate(Object... args) {
                return RowPredicate.all();
            }

            @Override
            public ScanController<R> scanController(Object... args) {
                return unfilteredReverse();
            }

            @Override
            public ScanController<R> scanController(RowPredicate predicate) {
                return unfilteredReverse();
            }
        };
    }

    @Override // ScanControllerFactory
    public final RowPredicate<R> predicate(Object... args) {
        return RowPredicate.all();
    }

    @Override // ScanControllerFactory
    public final ScanController<R> scanController(Object... args) {
        return unfiltered();
    }

    @Override // ScanControllerFactory
    public final ScanController<R> scanController(RowPredicate predicate) {
        return unfiltered();
    }

    /**
     * @param ff the parsed and reduced filter string; can be null initially
     */
    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> findFilteredFactory
        (SoftCache<String, ScanControllerFactory<R>> cache, String filter, FullFilter ff)
    {
        Latch latch;
        while (true) {
            check: synchronized (cache) {
                ScanControllerFactory<R> factory = cache.get(filter);
                if (factory != null) {
                    return factory;
                }
                if (mFilterLatchMap == null) {
                    mFilterLatchMap = new HashMap<>();
                } else if ((latch = mFilterLatchMap.get(filter)) != null) {
                    // Wait for the latch outside the synchronized block.
                    break check;
                }
                latch = new Latch(Latch.EXCLUSIVE);
                mFilterLatchMap.put(filter, latch);
                // Break out of the loop and do the work.
                break;
            }
            // Wait for another thread to do the work and try again.
            latch.acquireShared();
        }

        ScanControllerFactory<R> factory;
        Throwable ex = null;

        obtain: try {
            Class<?> rowType = rowType();
            RowInfo rowInfo = RowInfo.find(rowType);
            Map<String, ColumnInfo> allColumns = rowInfo.allColumns;
            Map<String, ColumnInfo> availableColumns = allColumns;

            RowGen primaryRowGen = null;
            if (joinedPrimaryTableClass() != null) {
                // Join to the primary.
                primaryRowGen = rowInfo.rowGen();
            }

            byte[] secondaryDesc = secondaryDescriptor();
            if (secondaryDesc != null) {
                rowInfo = RowStore.indexRowInfo(rowInfo, secondaryDesc);
                if (joinedPrimaryTableClass() == null) {
                    availableColumns = rowInfo.allColumns;
                }
            }

            if (ff == null) {
                ff = new Parser(allColumns, filter).parseFull(availableColumns).reduce();
            }

            RowFilter rf = ff.filter();

            if (rf instanceof FalseFilter) {
                factory = EmptyScanController.factory();
                break obtain;
            }

            if (rf instanceof TrueFilter && ff.projection() == null) {
                factory = this;
                break obtain;
            }

            String canonical = ff.toString();
            if (!canonical.equals(filter)) {
                factory = findFilteredFactory(cache, canonical, ff);
                break obtain;
            }

            var keyColumns = rowInfo.keyColumns.values().toArray(ColumnInfo[]::new);
            RowFilter[][] ranges = multiRangeExtract(rf, keyColumns);
            splitRemainders(rowInfo, ranges);

            if (cache == mFilterFactoryCacheDoubleCheck && primaryRowGen != null) {
                doubleCheckRemainder(ranges, primaryRowGen.info);
            }

            Class<? extends RowPredicate> baseClass;

            // FIXME: Although no predicate lock is required, a row lock is required.
            if (false && ranges.length == 1 && RowFilter.matchesOne(ranges[0], keyColumns)) {
                // No predicate lock is required when the filter matches at most one row.
                baseClass = null;
            } else {
                baseClass = mIndexLock == null ? null : mIndexLock.evaluatorClass();
            }

            RowGen rowGen = rowInfo.rowGen();

            byte[] projectionSpec = DecodePartialMaker.makeFullSpec
                (primaryRowGen != null ? primaryRowGen : rowGen, ff.projection());

            Class<? extends RowPredicate> predClass = new RowPredicateMaker
                (rowStoreRef(), baseClass, rowType, rowGen, primaryRowGen,
                 mTableManager.mPrimaryIndex.id(), mSource.id(), rf, filter, ranges).finish();

            if (ranges.length > 1) {
                var rangeFactories = new ScanControllerFactory[ranges.length];
                for (int i=0; i<ranges.length; i++) {
                    rangeFactories[i] = newFilteredFactory
                        (rowGen, ranges[i], predClass, projectionSpec);
                }
                factory = new RangeUnionScanControllerFactory(rangeFactories);
                break obtain;
            }

            // Only one range to scan.
            RowFilter[] range = ranges[0];

            if (range[0] == null && range[1] == null) {
                // Full scan, so just use the original reduced filter. It's possible that
                // the dnf/cnf form is reduced even further, but when doing a full scan,
                // let the user define the order in which the filter terms are examined.
                range[2] = rf;
                range[3] = null;
                splitRemainders(rowInfo, range);
            }

            factory = newFilteredFactory(rowGen, range, predClass, projectionSpec);
        } catch (Throwable e) {
            factory = null;
            ex = e;
        }

        synchronized (cache) {
            if (factory != null) {
                cache.put(filter, factory);
            }
            mFilterLatchMap.remove(filter, latch);
            if (mFilterLatchMap.isEmpty()) {
                mFilterLatchMap = null;
            }
        }

        latch.releaseExclusive();

        if (ex != null) {
            throw Utils.rethrow(ex);
        }

        return factory;
    }

    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> newFilteredFactory(RowGen rowGen, RowFilter[] range,
                                                        Class<? extends RowPredicate> predClass,
                                                        byte[] projectionSpec)
    {
        SingleScanController<R> unfiltered = unfiltered();

        RowFilter lowBound = range[0];
        RowFilter highBound = range[1];
        RowFilter filter = range[2];
        RowFilter joinFilter = range[3];

        return new FilteredScanMaker<R>
            (rowStoreRef(), secondaryDescriptor(), this, joinedPrimaryTableClass(),
             unfiltered, predClass, rowType(), rowGen,
             mSource.id(), lowBound, highBound, filter, joinFilter, projectionSpec).finish();
    }

    private RowFilter[][] multiRangeExtract(RowFilter rf, ColumnInfo... keyColumns) {
        try {
            return rf.dnf().multiRangeExtract(false, false, keyColumns);
        } catch (ComplexFilterException e) {
            complex(rf, e);
            try {
                return new RowFilter[][] {rf.cnf().rangeExtract(keyColumns)};
            } catch (ComplexFilterException e2) {
                return new RowFilter[][] {new RowFilter[] {null, null, rf, null}};
            }
        }
    }

    /**
     * @param rowInfo for secondary (method does nothing if this is the primary table)
     */
    private void splitRemainders(RowInfo rowInfo, RowFilter[]... ranges) {
        if (joinedPrimaryTableClass() != null) {
            // First filter on the secondary entry, and then filter on the joined primary entry.
            RowFilter.splitRemainders(rowInfo.allColumns, ranges);
        }
    }

    /**
     * Applies a double check of the remainder filter, applicable only to joins.
     *
     * @param rowInfo for primary
     */
    private static void doubleCheckRemainder(RowFilter[][] ranges, RowInfo rowInfo) {
        for (RowFilter[] r : ranges) {
            // Build up a complete remainder that does full fully redundant filtering. Order
            // the terms such that ones most likely to have any effect come first.
            RowFilter remainder = r[3].and(r[2]);
            if (r[0] != null) {
                remainder = remainder.and(r[0]);
            }
            if (r[1] != null) {
                remainder = remainder.and(r[1]);
            }
            // Remove terms that only check the primary key, because they won't change with a join.
            remainder = remainder.retain(rowInfo.valueColumns, false, TrueFilter.THE);
            r[3] = remainder.reduceMore();
        }
    }

    private void complex(RowFilter rf, ComplexFilterException e) {
        RowStore rs = rowStoreRef().get();
        if (rs != null) {
            EventListener listener = rs.mDatabase.eventListener();
            if (listener != null) {
                listener.notify(EventType.TABLE_COMPLEX_FILTER,
                                "Complex filter: %1$s \"%2$s\" %3$s",
                                rowType().getName(), rf.toString(), e.getMessage());
            }
        }
    }

    /**
     * Partially decodes a row from a key.
     */
    protected abstract R toRow(byte[] key);

    protected abstract WeakReference<RowStore> rowStoreRef();

    protected abstract QueryPlan planReverse(Object... args);

    /**
     * Returns a new or singleton instance.
     */
    protected abstract SingleScanController<R> unfiltered();

    /**
     * Returns a new or singleton instance.
     */
    protected abstract SingleScanController<R> unfilteredReverse();

    /**
     * Returns a MethodHandle which decodes rows partially.
     *
     * MethodType is: void (RowClass row, byte[] key, byte[] value)
     *
     * The spec defines two BitSets, which refer to columns to decode. The first BitSet
     * indicates which columns aren't in the row object and must be decoded. The second BitSet
     * indicates which columns must be marked as clean. All other columns are unset.
     *
     * @param spec must have an even length; first half refers to columns to decode and second
     * half refers to columns to mark clean
     */
    protected final MethodHandle decodePartialHandle(byte[] spec, int schemaVersion) {
        WeakCache<Object, MethodHandle> cache = mPartialDecodeCache;

        if (cache == null) {
            cache = new WeakCache<>();
            var existing = (WeakCache<Object, MethodHandle>)
                cPartialDecodeCacheHandle.compareAndExchange(this, null, cache);
            if (existing != null) {
                cache = existing;
            }
        }

        final Object key = schemaVersion == 0 ?
            ArrayKey.make(spec) : ArrayKey.make(schemaVersion, spec);

        MethodHandle decoder = cache.get(key);

        if (decoder == null) {
            synchronized (cache) {
                decoder = cache.get(key);
                if (decoder == null) {
                    decoder = makeDecodePartialHandle(spec, schemaVersion);
                    cache.put(key, decoder);
                }
            }
        }

        return decoder;
    }

    protected abstract MethodHandle makeDecodePartialHandle(byte[] spec, int schemaVersion);

    protected final void redoPredicateMode(Transaction txn) throws IOException {
        RowPredicateLock<R> lock = mIndexLock;
        if (lock != null) {
            lock.redoPredicateMode(txn);
        }
    }

    /**
     * Called when no trigger is installed.
     */
    protected final void store(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);
            try (RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row)) {
                source.store(txn, key, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Called when no trigger is installed.
     */
    protected final byte[] exchange(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        byte[] oldValue;
        try {
            redoPredicateMode(txn);
            try (RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row)) {
                oldValue = source.exchange(txn, key, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }

        return oldValue;
    }

    /**
     * Called when no trigger is installed.
     */
    protected final boolean insert(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        boolean result;
        try {
            redoPredicateMode(txn);
            try (RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row)) {
                result = source.insert(txn, key, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }

        return result;
    }

    /**
     * Override if this table implements a secondary index.
     */
    protected byte[] secondaryDescriptor() {
        return null;
    }

    /**
     * Override if this table implements a secondary index and joins to the primary.
     */
    protected Class<?> joinedPrimaryTableClass() {
        return null;
    }

    boolean supportsSecondaries() {
        return true;
    }

    /**
     * Set the table trigger and then wait for the old trigger to no longer be used. Waiting is
     * necessary to prevent certain race conditions. For example, when adding a secondary
     * index, a backfill task can't safely begin until it's known that no operations are in
     * flight which aren't aware of the new index. Until this method returns, it should be
     * assumed that both the old and new trigger are running concurrently.
     *
     * @param trigger can pass null to remove the trigger
     * @throws UnsupportedOperationException if triggers aren't supported by this table
     */
    final void setTrigger(Trigger<R> trigger) {
        if (mTrigger == null) {
            throw new UnsupportedOperationException();
        }

        if (trigger == null) {
            trigger = new Trigger<>();
            trigger.mMode = Trigger.SKIP;
        }

        ((Trigger<R>) cTriggerHandle.getAndSet(this, trigger)).disable();
    }

    /**
     * Returns the trigger quickly, which is null if triggers aren't supported.
     */
    final Trigger<R> getTrigger() {
        return mTrigger;
    }

    /**
     * Returns the current trigger, which must be held shared during the operation. As soon as
     * acquired, check if the trigger is disabled. This method must be public because it's
     * sometimes accessed from generated code which isn't a subclass of BaseTable.
     */
    public final Trigger<R> trigger() {
        return (Trigger<R>) cTriggerHandle.getOpaque(this);
    }

    static RowFilter parseFilter(Class<?> rowType, String filter) {
        var parser = new Parser(RowInfo.find(rowType).allColumns, filter);
        parser.skipProjection();
        return parser.parseFilter();
    }
}
