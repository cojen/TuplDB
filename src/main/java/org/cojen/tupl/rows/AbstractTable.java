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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.HashMap;
import java.util.Objects;

import java.util.stream.Stream;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;
import org.cojen.tupl.Index;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicate;
import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.filter.ComplexFilterException;
import org.cojen.tupl.filter.FalseFilter;
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
public abstract class AbstractTable<R> implements Table<R> {
    // Need a strong reference to this to prevent premature GC.
    final TableManager<R> mTableManager;

    protected final Index mSource;

    private final SoftCache<String, ScanControllerFactory<R>> mFilterFactoryCache;

    private HashMap<String, Latch> mFilterLatchMap;

    private Trigger<R> mTrigger;
    private static final VarHandle cTriggerHandle;

    // Is null if unsupported.
    protected final RowPredicateLock<R> mIndexLock;

    static {
        try {
            cTriggerHandle = MethodHandles.lookup().findVarHandle
                (AbstractTable.class, "mTrigger", Trigger.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param indexLock is null if unsupported
     */
    protected AbstractTable(TableManager<R> manager, Index source, RowPredicateLock<R> indexLock) {
        mTableManager = manager;

        mSource = Objects.requireNonNull(source);

        mFilterFactoryCache = new SoftCache<>();

        if (supportsSecondaries()) {
            var trigger = new Trigger<R>();
            trigger.mMode = Trigger.SKIP;
            cTriggerHandle.setRelease(this, trigger);
        }

        mIndexLock = indexLock;
    }

    @Override
    public final RowScanner<R> newRowScanner(Transaction txn) throws IOException {
        return newRowScanner(txn, unfiltered());
    }

    @Override
    public final RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowScanner(txn, filtered(filter, args));
    }

    private RowScanner<R> newRowScanner(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        final var scanner = new BasicRowScanner<>(this, controller);
        RowPredicateLock.Closer closer = null;

        if (txn != null && !txn.lockMode().noReadLock) {
            RowPredicateLock<R> lock = mIndexLock;
            if (lock != null) {
                // This case is reached when a transaction was provided which is read committed
                // or higher. Adding a predicate lock prevents new rows from being inserted
                // into the scan range for the duration of the transaction scope. If the lock
                // mode is repeatable read, then rows which have been read cannot be deleted,
                // effectively making the transaction serializable.
                closer = lock.addPredicate(txn, controller.predicate());
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

    @Override
    public final RowUpdater<R> newRowUpdater(Transaction txn) throws IOException {
        return newRowUpdater(txn, unfiltered());
    }

    @Override
    public final RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowUpdater(txn, filtered(filter, args));
    }

    protected RowUpdater<R> newRowUpdater(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        final BasicRowUpdater<R> updater;
        RowPredicateLock.Closer closer = null;

        addPredicate: {
            final RowPredicateLock<R> lock;

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

            lock = mIndexLock;
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
            updater.init(txn);
            return updater;
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    @Override
    public final Stream<R> newStream(Transaction txn) {
        try {
            return RowSpliterator.newStream(newRowScanner(txn));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public final Stream<R> newStream(Transaction txn, String filter, Object... args) {
        try {
            return RowSpliterator.newStream(newRowScanner(txn, filter, args));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public final String toString() {
        return mSource.toString();
    }

    @Override
    public final Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
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
    public Table<R> viewUnjoined() {
        return this;
    }

    private ScanController<R> filtered(String filter, Object... args) {
        return filteredFactory(filter).newScanController(args);
    }

    private ScanControllerFactory<R> filteredFactory(String filter) {
        ScanControllerFactory<R> factory = mFilterFactoryCache.get(filter);
        if (factory == null) {
            factory = findFilteredFactory(filter);
        }
        return factory;
    }

    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> findFilteredFactory(final String filter) {
        Latch latch;
        while (true) {
            check: synchronized (mFilterFactoryCache) {
                ScanControllerFactory<R> factory = mFilterFactoryCache.get(filter);
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
            RowFilter rf = parse(rowType, filter).reduce();

            if (rf instanceof FalseFilter) {
                factory = EmptyScanController.factory();
                break obtain;
            }

            if (rf instanceof TrueFilter) {
                SingleScanController<R> unfiltered = unfiltered();
                factory = (Object... args) -> unfiltered;
                break obtain;
            }

            String canonical = rf.toString();
            if (!canonical.equals(filter)) {
                factory = findFilteredFactory(canonical);
                break obtain;
            }

            RowInfo primaryRowInfo = RowInfo.find(rowType);
            RowInfo rowInfo = primaryRowInfo;

            byte[] secondaryDesc = secondaryDescriptor();
            if (secondaryDesc != null) {
                rowInfo = RowStore.indexRowInfo(rowInfo, secondaryDesc);
            }

            var keyColumns = rowInfo.keyColumns.values().toArray(ColumnInfo[]::new);
            RowFilter[][] ranges = multiRangeExtract(rowInfo, rf, keyColumns);

            Class<? extends RowPredicate> baseClass;

            // FIXME: Although no predicate lock is required, a row lock is required.
            if (false && ranges.length == 1 && RowFilter.matchesOne(ranges[0], keyColumns)) {
                // No predicate lock is required when the filter matches at most one row.
                baseClass = null;
            } else {
                baseClass = mIndexLock == null ? null : mIndexLock.evaluatorClass();
            }

            Class<? extends RowPredicate> predClass = new RowPredicateMaker
                (rowStoreRef(), baseClass, rowType, rowInfo.rowGen(),
                 mTableManager.mPrimaryIndex.id(), mSource.id(), rf, filter, ranges).finish();

            if (ranges.length > 1) {
                var rangeFactories = new ScanControllerFactory[ranges.length];
                for (int i=0; i<ranges.length; i++) {
                    rangeFactories[i] = newFilteredFactory(rowInfo, ranges[i], predClass);
                }
                factory = new MultiScanControllerFactory(rangeFactories);
                break obtain;
            }

            RowFilter[] range = ranges[0];

            if (range[1] == null && range[2] == null) {
                // Full scan, so just use the original reduced filter. It's possible that
                // the dnf/cnf form is reduced even further, but when doing a full scan,
                // let the user define the order in which the filter terms are examined.
                range[0] = rf;
            }

            factory = newFilteredFactory(rowInfo, range, predClass);
        } catch (Throwable e) {
            factory = null;
            ex = e;
        }

        synchronized (mFilterFactoryCache) {
            if (factory != null) {
                mFilterFactoryCache.put(filter, factory);
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
    private ScanControllerFactory<R> newFilteredFactory(RowInfo rowInfo, RowFilter[] range,
                                                        Class<? extends RowPredicate> predClass)
    {
        SingleScanController<R> unfiltered = unfiltered();

        RowFilter remainder = range[0];
        RowFilter lowBound = range[1];
        RowFilter highBound = range[2];

        String remainderStr = remainder == null ? null : remainder.toString();

        Class<?> tableClass = getClass();

        Class<?> primaryTableClass = primaryTableClass();
        if (primaryTableClass == null) {
            primaryTableClass = tableClass;
        }

        return new FilteredScanMaker<R>
            (rowStoreRef(), tableClass, primaryTableClass,
             unfiltered, predClass, rowType(), rowInfo,
             mSource.id(), remainder, remainderStr, lowBound, highBound).finish();
    }

    private RowFilter[][] multiRangeExtract(RowInfo rowInfo, RowFilter rf,
                                            ColumnInfo... keyColumns)
    {
        try {
            return rf.dnf().multiRangeExtract(false, false, keyColumns);
        } catch (ComplexFilterException e) {
            complex(rf, e);
            try {
                return new RowFilter[][] {rf.cnf().rangeExtract(keyColumns)};
            } catch (ComplexFilterException e2) {
                return new RowFilter[][] {new RowFilter[] {rf, null, null}};
            }
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
    protected abstract R asRow(byte[] key);

    protected abstract WeakReference<RowStore> rowStoreRef();

    /**
     * Returns a singleton instance.
     */
    protected abstract SingleScanController<R> unfiltered();

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
            RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row);
            try {
                source.store(txn, key, value);
            } finally {
                closer.close();
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
            RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row);
            try {
                oldValue = source.exchange(txn, key, value);
            } finally {
                closer.close();
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
            RowPredicateLock.Closer closer = mIndexLock.openAcquire(txn, row);
            try {
                result = source.insert(txn, key, value);
            } finally {
                closer.close();
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
     * Override if this table implements a secondary index.
     */
    protected Class<?> primaryTableClass() {
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
     * sometimes accessed from generated code which isn't a subclass of AbstractTable.
     */
    public final Trigger<R> trigger() {
        return (Trigger<R>) cTriggerHandle.getOpaque(this);
    }

    static RowFilter parse(Class<?> rowType, String filter) {
        return new Parser(RowInfo.find(rowType).allColumns, filter).parse();
    }
}
