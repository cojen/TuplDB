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

import java.util.HashMap;
import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.filter.ComplexFilterException;
import org.cojen.tupl.filter.FalseFilter;
import org.cojen.tupl.filter.Parser;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.TrueFilter;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;

/**
 * Base class for all generated table classes.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractTable<R> implements Table<R> {
    // Need a strong reference to this to prevent premature GC.
    final TableManager<R> mTableManager;

    protected final Index mSource;

    private final WeakCache<String, ScanControllerFactory<R>> mFilterFactoryCache;

    private HashMap<String, Latch> mFilterLatchMap;

    private Trigger<R> mTrigger;
    private static final VarHandle cTriggerHandle;

    static {
        try {
            cTriggerHandle = MethodHandles.lookup().findVarHandle
                (AbstractTable.class, "mTrigger", Trigger.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    protected AbstractTable(TableManager<R> manager, Index source) {
        mTableManager = manager;
        mSource = Objects.requireNonNull(source);
        mFilterFactoryCache = new WeakCache<>();
        if (supportsSecondaries()) {
            Trigger<R> trigger = new Trigger<>();
            trigger.mMode = Trigger.SKIP;
            cTriggerHandle.setRelease(this, trigger);
        }
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn) throws IOException {
        return newRowScanner(txn, unfiltered());
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowScanner(txn, filtered(filter, args));
    }

    private RowScanner<R> newRowScanner(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        var scanner = new BasicRowScanner<>(mSource, controller);
        scanner.init(txn);
        return scanner;
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn) throws IOException {
        return newRowUpdater(txn, unfiltered());
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return newRowUpdater(txn, filtered(filter, args));
    }

    protected RowUpdater<R> newRowUpdater(Transaction txn, ScanController<R> controller)
        throws IOException
    {
        AbstractTable<R> table = mTrigger != null ? this : null;

        BasicRowUpdater<R> updater;
        if (txn == null) {
            txn = mSource.newTransaction(null);
            updater = new AutoCommitRowUpdater<>(mSource, controller, table);
        } else {
            switch (txn.lockMode()) {
            default:
                updater = new BasicRowUpdater<>(mSource, controller, table);
                break;
            case REPEATABLE_READ:
                updater = new UpgradableRowUpdater<>(mSource, controller, table);
                break;
            case READ_COMMITTED:
            case READ_UNCOMMITTED:
                txn.enter();
                updater = new NonRepeatableRowUpdater<>(mSource, controller, table);
                break;
            }
        }

        updater.init(txn);

        return updater;
    }

    @Override
    public String toString() {
        return mSource.toString();
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public Table<R> alternateKeyTable(String... columns) throws IOException {
        var rs = rowStoreRef().get();
        return rs == null ? null : rs.alternateKeyTable(this, columns);
    }

    @Override
    public Table<R> secondaryIndexTable(String... columns) throws IOException {
        var rs = rowStoreRef().get();
        return rs == null ? null : rs.secondaryIndexTable(this, columns);
    }

    private ScanController<R> filtered(String filter, Object... args) throws IOException {
        return scanControllerFactory(filter).newScanController(args);
    }

    private ScanControllerFactory<R> scanControllerFactory(String filter) {
        ScanControllerFactory<R> factory = mFilterFactoryCache.get(filter);
        if (factory == null) {
            factory = findScanControllerFactory(filter);
        }
        return factory;
    }

    private ScanControllerFactory<R> findScanControllerFactory(final String filter) {
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

        try {
            RowFilter rf = parse(rowType(), filter).reduce();

            if (rf instanceof FalseFilter) {
                factory = EmptyScanController.factory();
            } else if (rf instanceof TrueFilter) {
                SingleScanController<R> unfiltered = unfiltered();
                factory = (Object... args) -> unfiltered;
            } else {
                String canonical = rf.toString();
                if (!canonical.equals(filter)) {
                    factory = findScanControllerFactory(canonical);
                } else {
                    RowInfo rowInfo = RowInfo.find(rowType());

                    byte[] secondaryDesc = secondaryDescriptor();
                    if (secondaryDesc != null) {
                        rowInfo = RowStore.indexRowInfo(rowInfo, secondaryDesc);
                    }

                    RowFilter[][] ranges = multiRangeExtract(rowInfo, rf);

                    RowFilter[] range;
                    if (ranges.length > 1) {
                        // FIXME: Try to create canonical ScanControllerFactory instances for
                        // each range by combining the range components together.
                        throw new Error("FIXME: multiRangeExtract");
                    } else {
                        range = ranges[0];
                    }

                    RowFilter remainder = range[0];
                    RowFilter lowBound = range[1];
                    RowFilter highBound = range[2];

                    String remainderStr = remainder == null ? null : remainder.toString();

                    factory = newFilteredFactory
                        (rowInfo, remainder, remainderStr, lowBound, highBound);
                }
            }
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
    private ScanControllerFactory<R> newFilteredFactory(RowInfo rowInfo,
                                                        RowFilter filter, String filterStr,
                                                        RowFilter lowBound, RowFilter highBound)
    {
        Class unfilteredClass = unfiltered().getClass();

        return new FilteredScanMaker<R>
            (rowStoreRef(), getClass(), unfilteredClass, rowType(), rowInfo,
             mSource.id(), filter, filterStr, lowBound, highBound).finish();
    }

    private RowFilter[][] multiRangeExtract(RowInfo rowInfo, RowFilter rf) {
        var keyColumns = rowInfo.keyColumns.values().toArray(ColumnInfo[]::new);

        try {
            return rf.dnf().multiRangeExtract(false, false, keyColumns);
        } catch (ComplexFilterException e) {
            // FIXME: log the exception?
            try {
                return new RowFilter[][] {rf.cnf().rangeExtract(false, keyColumns)};
            } catch (ComplexFilterException e2) {
                // FIXME: log the exception?
                return new RowFilter[][] {new RowFilter[] {rf, null, null}};
            }
        }
    }

    protected abstract WeakReference<RowStore> rowStoreRef();

    /**
     * Returns a singleton instance.
     */
    protected abstract SingleScanController<R> unfiltered();

    /**
     * Override if this table implements a secondary index.
     */
    protected byte[] secondaryDescriptor() {
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
    void setTrigger(Trigger<R> trigger) {
        if (mTrigger == null) {
            throw new UnsupportedOperationException();
        }

        if (trigger == null) {
            trigger = new Trigger<>();
            trigger.mMode = Trigger.SKIP;
        }

        ((Trigger<R>) cTriggerHandle.getAndSet(this, trigger)).disabled();
    }

    /**
     * Returns the current trigger, which must be held shared during the operation. As soon as
     * acquired, check if the trigger is disabled. This method must be public because it's
     * sometimes accessed from generated code which isn't a subclass of AbstractTable.
     */
    public Trigger<R> trigger() {
        return (Trigger<R>) cTriggerHandle.getOpaque(this);
    }

    static RowFilter parse(Class<?> rowType, String filter) {
        return new Parser(RowInfo.find(rowType).allColumns, filter).parse();
    }
}
