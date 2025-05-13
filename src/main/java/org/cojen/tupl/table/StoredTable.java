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

package org.cojen.tupl.table;

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Entry;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicate;
import org.cojen.tupl.core.RowPredicateLock;
import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;
import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.expr.CompiledQuery;
import org.cojen.tupl.table.expr.Parser;
import org.cojen.tupl.table.expr.RelationExpr;

import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.remote.RemoteTableProxy;

import org.cojen.tupl.views.ViewUtils;

import static java.util.Spliterator.*;

/**
 * Base class for all generated table classes which store rows.
 *
 * @author Brian S O'Neill
 */
public abstract class StoredTable<R> extends BaseTable<R> implements ScanControllerFactory<R> {
    final TableManager<R> mTableManager;

    protected final Index mSource;

    // QueryLauncher types.
    static final int PLAIN = 0b00, DOUBLE_CHECK = 0b01,
        FOR_UPDATE = 0b10, FOR_UPDATE_DOUBLE_CHECK = FOR_UPDATE | DOUBLE_CHECK;

    private Trigger<R> mTrigger;
    private static final VarHandle cTriggerHandle;

    protected final RowPredicateLock<R> mIndexLock;

    private WeakCache<TupleKey, MethodHandle, byte[]> mDecodePartialCache;
    private static final VarHandle cDecodePartialCacheHandle;

    private WeakCache<Object, MethodHandle, byte[]> mWriteRowCache;
    private static final VarHandle cWriteRowCacheHandle;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            cTriggerHandle = lookup.findVarHandle
                (StoredTable.class, "mTrigger", Trigger.class);
            cDecodePartialCacheHandle = lookup.findVarHandle
                (StoredTable.class, "mDecodePartialCache", WeakCache.class);
            cWriteRowCacheHandle = lookup.findVarHandle
                (StoredTable.class, "mWriteRowCache", WeakCache.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    protected StoredTable(TableManager<R> manager, Index source, RowPredicateLock<R> indexLock) {
        mTableManager = Objects.requireNonNull(manager);
        mSource = Objects.requireNonNull(source);
        mIndexLock = Objects.requireNonNull(indexLock);

        if (supportsSecondaries()) {
            var trigger = new Trigger<R>();
            trigger.mMode = Trigger.SKIP;
            cTriggerHandle.setRelease(this, trigger);
        }
    }

    public final TableManager<R> tableManager() {
        return mTableManager;
    }

    @Override
    public boolean hasPrimaryKey() {
        return true;
    }

    @Override
    public final Scanner<R> newScanner(R row, Transaction txn) throws IOException {
        return newScanner(row, txn, unfiltered());
    }

    final Scanner<R> newScanner(R row, Transaction txn, ScanController<R> controller)
        throws IOException
    {
        final BasicScanner<R> scanner;
        RowPredicateLock.Closer closer = null;

        newScanner: {
            if (txn == null) {
                // A null transaction behaves like a read committed transaction (as usual), but
                // it doesn't acquire predicate locks. This makes it weaker than a transaction
                // which is explicitly read committed.

                if (joinedPrimaryTableClass() != null) {
                    // Need to guard against this secondary index from being concurrently
                    // dropped. This is like adding a predicate lock which matches nothing.
                    txn = mSource.newTransaction(null);
                    closer = mIndexLock.addGuard(txn);

                    if (controller.isJoined()) {
                        // See the JoinedScanController.validate method. When using
                        // READ_COMMITTED, it assumes a predicate lock is preventing deletes.
                        // Since none is used here, explicitly retain row locks against the
                        // secondary until after the primary row has been loaded.
                        txn.lockMode(LockMode.REPEATABLE_READ);
                        scanner = new AutoUnlockScanner<>(this, controller);
                    } else {
                        // Since the validate method won't be called, READ_COMMITTED is fine.
                        txn.lockMode(LockMode.READ_COMMITTED);
                        scanner = new TxnResetScanner<>(this, controller);
                    }

                    break newScanner;
                }
            } else if (!txn.lockMode().noReadLock) {
                // This case is reached when a transaction was provided which is read committed
                // or higher. Adding a predicate lock prevents new rows from being inserted
                // into the scan range for the duration of the transaction scope. If the lock
                // mode is repeatable read, then rows which have been read cannot be deleted,
                // effectively making the transaction serializable.
                closer = mIndexLock.addPredicate(txn, controller.predicate());
            }

            scanner = new BasicScanner<>(this, controller);
        }

        try {
            scanner.init(txn, row);
            return scanner;
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    /**
     * Note: Doesn't support orderBy.
     */
    final Scanner<R> newScannerThisTable(Transaction txn, R row,
                                         String queryStr, Object... args)
        throws IOException
    {
        return newScanner(row, txn, scannerFilteredFactory(txn, queryStr).scanController(args));
    }

    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> scannerFilteredFactory(Transaction txn, String queryStr)
        throws IOException
    {
        // Might need to double-check the filter after joining to the primary, in case there
        // were any changes after the secondary entry was loaded. See the cacheNewValue method.
        MultiCache.Type cacheType = RowUtils.isUnlocked(txn) ? TYPE_4 : TYPE_3;
        return (ScanControllerFactory<R>) cacheObtain(cacheType, queryStr, null);
    }

    @Override
    public final Updater<R> newUpdater(R row, Transaction txn) throws IOException {
        return newUpdater(row, txn, unfiltered());
    }

    protected Updater<R> newUpdater(R row, Transaction txn, ScanController<R> controller)
        throws IOException
    {
        return newUpdater(row, txn, controller, null);
    }

    /**
     * @param secondary non-null if joining from a secondary index to this primary table
     */
    final Updater<R> newUpdater(R row, Transaction txn, ScanController<R> controller,
                                StoredTableIndex<R> secondary)
        throws IOException
    {
        final BasicUpdater<R> updater;
        RowPredicateLock.Closer closer = null;

        addPredicate: {
            if (txn == null) {
                txn = mSource.newTransaction(null); // always UPGRADABLE_READ
                updater = new AutoCommitUpdater<>(this, controller);
                if (secondary != null) {
                    // Need to guard against the secondary index from being concurrently
                    // dropped. This is like adding a predicate lock which matches nothing.
                    secondary.mIndexLock.addGuard(txn);
                }
                break addPredicate;
            }

            switch (txn.lockMode()) {
            case UPGRADABLE_READ: default: {
                updater = new BasicUpdater<>(this, controller);
                break;
            }

            case REPEATABLE_READ: {
                // Need to use upgradable locks to prevent deadlocks.
                updater = new UpgradableUpdater<>(this, controller);
                break;
            }

            case READ_COMMITTED: {
                // Row locks are released when possible, but a predicate lock will still be
                // held for the duration of the transaction. It's not worth the trouble to
                // determine if it can be safely released when the updater finishes.
                updater = new NonRepeatableUpdater<>(this, controller);
                break;
            }

            case READ_UNCOMMITTED:
                updater = new NonRepeatableUpdater<>(this, controller);
                // Don't add a predicate lock.
                break addPredicate;

            case UNSAFE:
                updater = new BasicUpdater<>(this, controller);
                // Don't add a predicate lock.
                break addPredicate;
            }

            RowPredicateLock<R> lock = secondary == null ? mIndexLock : secondary.mIndexLock;

            // This case is reached when a transaction was provided which is read committed
            // or higher. Adding a predicate lock prevents new rows from being inserted
            // into the scan range for the duration of the transaction scope. If the lock
            // mode is repeatable read, then rows which have been read cannot be deleted,
            // effectively making the transaction serializable.
            closer = lock.addPredicate(txn, controller.predicate());
        }

        try {
            if (secondary == null) {
                updater.init(txn, row);
                return updater;
            } else {
                var joined = new JoinedUpdater<>(secondary, controller, updater);
                joined.init(txn, row);
                return joined;
            }
        } catch (Throwable e) {
            if (closer != null) {
                closer.close();
            }
            throw e;
        }
    }

    /**
     * Note: Doesn't support orderBy.
     */
    final Updater<R> newUpdaterThisTable(R row, Transaction txn,
                                         String queryStr, Object... args)
        throws IOException
    {
        return newUpdater(row, txn, updaterFilteredFactory(txn, queryStr).scanController(args));
    }

    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> updaterFilteredFactory(Transaction txn, String queryStr)
        throws IOException
    {
        // Might need to double-check the filter after joining to the primary, in case there
        // were any changes after the secondary entry was loaded. Note that no double check is
        // needed with READ_UNCOMMITTED, because the updater for it still acquires locks. Also
        // note that FOR_UPDATE isn't used, because mFilterFactoryCache doesn't support it.
        // See the cacheNewValue method.
        MultiCache.Type cacheType = RowUtils.isUnsafe(txn) ? TYPE_4 : TYPE_3;
        return (ScanControllerFactory<R>) cacheObtain(cacheType, queryStr, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final QueryLauncher<R> query(String queryStr) throws IOException {
        // See the cacheNewValue method.
        return (QueryLauncher<R>) cacheObtain(MultiCache.TYPE_1, queryStr, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Table<Row> derive(String query, Object... args) throws IOException {
        // See the cacheNewValue method.
        return ((CompiledQuery<Row>) cacheObtain(MultiCache.TYPE_2, query, this)).table(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        // See the cacheNewValue method.
        var key = new CompiledQuery.DerivedKey(derivedType, query);
        return ((CompiledQuery<D>) cacheObtain(MultiCache.TYPE_2, key, this)).table(args);
    }

    @Override
    public Table<R> distinct() {
        return this;
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

    /**
     * Scan and write all rows of this table to a remote endpoint. This method doesn't flush
     * the output stream.
     */
    @Override
    @SuppressWarnings("unchecked")
    public final void scanWrite(Transaction txn, Pipe out) throws IOException {
        var writer = new RowWriter<R>(out);

        try {
            // Pass the writer as if it's a row, but it's actually a RowConsumer.
            Scanner<R> scanner = newScanner((R) writer, txn);
            try {
                while (scanner.step((R) writer) != null);
            } catch (Throwable e) {
                Utils.closeQuietly(scanner);
                throw e;
            }
        } catch (RuntimeException | IOException e) {
            writer.writeTerminalException(e);
            return;
        }

        writer.writeTerminator();
    }

    /**
     * Scan and write a subset of rows from this table to a remote endpoint. This method
     * doesn't flush the output stream.
     */
    @Override
    public final void scanWrite(Transaction txn, Pipe out, String queryStr, Object... args)
        throws IOException
    {
        var writer = new RowWriter<R>(out);

        try {
            query(queryStr).scanWrite(txn, writer, args);
        } catch (RuntimeException | IOException e) {
            writer.writeTerminalException(e);
            return;
        }

        writer.writeTerminator();
    }

    /**
     * Scan and write a subset of rows from this table to a remote endpoint. This method
     * doesn't flush the output stream.
     *
     * @param query expected to be a Query object obtained from this StoredTable
     */
    public final void scanWrite(Transaction txn, Pipe out, Query<R> query, Object... args)
        throws IOException
    {
        var writer = new RowWriter<R>(out);

        try {
            ((QueryLauncher) query).scanWrite(txn, writer, args);
        } catch (RuntimeException | IOException e) {
            writer.writeTerminalException(e);
            return;
        }

        writer.writeTerminator();
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
    public int characteristics() {
        return NONNULL | ORDERED | CONCURRENT | DISTINCT;
    }

    /**
     * Returns a view of this table where the primary key is specified by the columns of an
     * alternate key, and the row is fully resolved by joining to the primary table. Direct
     * stores against the returned table aren't permitted, and an {@link
     * UnmodifiableViewException} is thrown when attempting to do so. Modifications are
     * permitted when using a {@link Updater}.
     *
     * @param columns column specifications for the alternate key
     * @return alternate key as a table
     * @throws IllegalStateException if alternate key wasn't found
     */
    protected StoredTableIndex<R> viewAlternateKey(String... columns) throws IOException {
        return viewIndexTable(true, columns);
    }

    /**
     * Returns a view of this table where the primary key is specified by the columns of a
     * secondary index, and the row is fully resolved by joining to the primary table. Direct
     * stores against the returned table aren't permitted, and an {@link
     * UnmodifiableViewException} is thrown when attempting to do so. Modifications are
     * permitted when using a {@link Updater}.
     *
     * @param columns column specifications for the secondary index
     * @return secondary index as a table
     * @throws IllegalStateException if secondary index wasn't found
     */
    protected StoredTableIndex<R> viewSecondaryIndex(String... columns) throws IOException {
        return viewIndexTable(false, columns);
    }

    final StoredTableIndex<R> viewIndexTable(boolean alt, String... columns) throws IOException {
        return rowStore().indexTable(this, alt, columns);
    }

    /**
     * Returns a direct view of an alternate key or secondary index, in the form of an
     * unmodifiable table. The rows of the table only contain the columns of the alternate key
     * or secondary index.
     *
     * @return an unjoined table, or else this table if it's not joined
     */
    protected Table<R> viewUnjoined() {
        return this;
    }

    @Override
    public void close() throws IOException {
        mSource.close();

        // Secondary indexes aren't closed immediately, and so clearing the query cache forces
        // calls to be made to the checkClosed method. Explicitly closing the old launchers
        // forces any in-progress scans to abort. Scans over sorted results aren't necessarily
        // affected, athough it would be nice if those always aborted too.

        cacheClear(value -> {
            if (value instanceof QueryLauncher launcher) {
                try {
                    launcher.closeIndexes();
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }
        });
    }

    @Override
    public boolean isClosed() {
        return mSource.isClosed();
    }

    private void checkClosed() throws IOException {
        if (isClosed()) {
            // Calling isEmpty should throw the preferred exception...
            mSource.isEmpty();
            // ...or else throw the generic one instead.
            throw new ClosedIndexException();
        }
    }

    @Override // ScanControllerFactory
    public int argumentCount() {
        return 0;
    }

    @Override // ScanControllerFactory
    public final ScanControllerFactory<R> reverse() {
        return new ScanControllerFactory<R>() {
            @Override
            public int argumentCount() {
                return 0;
            }

            @Override
            public QueryPlan plan(Object... args) {
                return planReverse(args);
            }

            @Override
            public ScanControllerFactory<R> reverse() {
                return StoredTable.this;
            }

            @Override
            public RowPredicate<R> predicate(Object... args) {
                return RowPredicate.all();
            }

            @Override
            public int characteristics() {
                return StoredTable.this.characteristics();
            }

            @Override
            public ScanController<R> scanController(Object... args) {
                return unfilteredReverse();
            }

            @Override
            public ScanController<R> scanController(RowPredicate<R> predicate) {
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
    public final ScanController<R> scanController(RowPredicate<R> predicate) {
        return unfiltered();
    }

    @Override // MultiCache
    public final Object cacheNewValue(MultiCache.Type cacheType, Object key, Object helper)
        throws IOException
    {
        factory: {
            int type;
            if (cacheType == MultiCache.TYPE_3) {
                type = PLAIN;
            } else if (cacheType == MultiCache.TYPE_4) {
                type = DOUBLE_CHECK;
            } else {
                break factory;
            }

            var queryStr = (String) key;
            var spec = (QuerySpec) helper;

            return newFilteredFactory(type, queryStr, spec);
        }

        if (cacheType == MultiCache.TYPE_1) { // see the query method
            // Short lived helper object.
            record ParsedQuery(String queryStr, RelationExpr expr) { }

            if (key instanceof TupleKey) {
                var pq = (ParsedQuery) helper;
                return StoredQueryLauncher.make(StoredTable.this, pq.queryStr(), pq.expr());
            }

            String queryStr = (String) key;
            assert helper == null;
            RelationExpr expr = StoredQueryLauncher.parse(StoredTable.this, queryStr);

            // Obtain the canonical instance and map to that.
            TupleKey canonicalKey = expr.makeKey();
            return cacheObtain(cacheType, canonicalKey, new ParsedQuery(queryStr, expr));
        }

        if (cacheType == MultiCache.TYPE_2) { // see the derive method
            return CompiledQuery.makeDerived(this, cacheType, key, helper);
        }

        throw new AssertionError();
    }

    private static MultiCache.Type toCacheType(int type) {
        return (type & ~FOR_UPDATE) == PLAIN ? MultiCache.TYPE_3 : MultiCache.TYPE_4;
    }

    /**
     * @param type PLAIN or DOUBLE_CHECK
     * @param queryStr the parsed and reduced query string; can be null initially
     */
    @SuppressWarnings("unchecked")
    private ScanControllerFactory<R> newFilteredFactory(int type, String queryStr, QuerySpec query)
        throws IOException
    {
        Class<?> rowType = rowType();
        RowInfo rowInfo = RowInfo.find(rowType);

        RowGen primaryRowGen = null;
        if (joinedPrimaryTableClass() != null) {
            // Join to the primary.
            primaryRowGen = rowInfo.rowGen();
        }

        Set<String> availableColumns = null;

        byte[] secondaryDesc = secondaryDescriptor();
        if (secondaryDesc != null) {
            rowInfo = RowStore.secondaryRowInfo(rowInfo, secondaryDesc);
            if (joinedPrimaryTableClass() == null) {
                availableColumns = rowInfo.allColumns.keySet();
            }
        }

        if (query == null) {
            query = Parser.parseQuerySpec(0, rowType, availableColumns, queryStr).reduce();
        }

        RowFilter rf = query.filter();

        if (rf instanceof FalseFilter) {
            return EmptyScanController.factory();
        }

        if (rf instanceof TrueFilter && query.projection() == null) {
            return this;
        }

        String canonical = query.toString();
        if (!canonical.equals(queryStr)) {
            // Don't actually return a specialized instance because it will be same.
            return (ScanControllerFactory<R>) cacheObtain(toCacheType(type), canonical, query);
        }

        var keyColumns = rowInfo.keyColumns.values().toArray(ColumnInfo[]::new);
        RowFilter[][] ranges = multiRangeExtract(rf, keyColumns);
        splitRemainders(rowInfo, ranges);

        if ((type & DOUBLE_CHECK) != 0 && primaryRowGen != null) {
            doubleCheckRemainder(ranges, primaryRowGen.info);
        }

        Class<? extends RowPredicate> baseClass = mIndexLock.evaluatorClass();
        RowGen rowGen = rowInfo.rowGen();

        Class<? extends RowPredicate> predClass = new RowPredicateMaker
            (rowStoreRef(), baseClass, rowType, rowGen, primaryRowGen,
             mTableManager.mPrimaryIndex.id(), mSource.id(), rf, queryStr, ranges).finish();

        if (ranges.length > 1) {
            var rangeFactories = new ScanControllerFactory[ranges.length];
            for (int i=0; i<ranges.length; i++) {
                rangeFactories[i] = newFilteredFactory
                    (rowGen, ranges[i], predClass, query.projection());
            }
            return new RangeUnionScanControllerFactory(rangeFactories);
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

        return newFilteredFactory(rowGen, range, predClass, query.projection());
    }

    private ScanControllerFactory<R> newFilteredFactory(RowGen rowGen, RowFilter[] range,
                                                        Class<? extends RowPredicate> predClass,
                                                        Map<String, ColumnInfo> projection)
    {
        SingleScanController<R> unfiltered = unfiltered();

        RowFilter lowBound = range[0];
        RowFilter highBound = range[1];
        RowFilter filter = range[2];
        RowFilter joinFilter = range[3];

        return new FilteredScanMaker<R>
            (rowStoreRef(), this, rowGen, unfiltered, predClass,
             lowBound, highBound, filter, joinFilter, projection).finish();
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
     * @see RowFilter#multiRangeExtract
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
     * @see RowFilter#multiRangeExtract
     */
    private static void doubleCheckRemainder(RowFilter[][] ranges, RowInfo rowInfo) {
        for (RowFilter[] r : ranges) {
            // Build up a complete remainder that performs fully redundant filtering. Order the
            // terms such that ones most likely to have any effect come first.
            RowFilter remainder = and(and(and(r[3], r[2]), r[0]), r[1]);
            // Remove terms that only check the primary key, because they won't change with a join.
            remainder = remainder.retain(rowInfo.valueColumns::containsKey, false, TrueFilter.THE);
            r[3] = remainder.reduceMore();
        }
    }

    private static RowFilter and(RowFilter a, RowFilter b) {
        return a != null ? (b != null ? a.and(b) : a) : (b != null ? b : TrueFilter.THE);
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
     * @param type PLAIN, DOUBLE_CHECK, etc.
     * @param selector must be provided
     * @return null if type is FOR_UPDATE but it should just be PLAIN
     */
    @SuppressWarnings("unchecked")
    QueryLauncher<R> newQueryLauncher(int type, IndexSelector selector) throws IOException {
        checkClosed();

        RowInfo rowInfo = selector.primaryInfo();

        if ((type & FOR_UPDATE) != 0) forUpdate: {
            if (selector.orderBy() != null) {
                // The Updater needs to have a sort step applied, and so it needs access to
                // the primary key. This is because the update/delete operation is performed
                // by calling Table.update or Table.delete. See WrappedUpdater.
                QuerySpec query = selector.query();
                Map<String, ColumnInfo> proj = query.projection();
                if (proj != null && !proj.keySet().containsAll(rowInfo.keyColumns.keySet())) {
                    proj = new LinkedHashMap<>(proj);
                    proj.putAll(rowInfo.keyColumns);
                    query = query.withProjection(proj);
                    selector = new IndexSelector<R>(this, rowInfo, query, true);
                    break forUpdate;
                }
            }

            if (!selector.forUpdateRuleChosen()) {
                // Don't actually return a specialized updater instance because it will be the
                // same as the scanner instance.
                return null;
            }
        }

        int num = selector.numSelected();
        QueryLauncher<R> launcher;

        if (num <= 1) {
            launcher = newSubLauncher(type, selector, 0);
        } else {
            var launchers = new QueryLauncher[num];
            for (int i=0; i<num; i++) {
                launchers[i] = newSubLauncher(type, selector, i);
            }
            launcher = new DisjointUnionQueryLauncher<R>(launchers);
        }

        OrderBy orderBy = selector.orderBy();
        if (orderBy != null) {
            launcher = new SortedQueryLauncher<R>(this, launcher, selector.projection(), orderBy);
        }

        return launcher;
    }

    /**
     * @param type PLAIN, DOUBLE_CHECK, etc.
     */
    @SuppressWarnings("unchecked")
    private QueryLauncher<R> newSubLauncher(int type, IndexSelector<R> selector, int i)
        throws IOException
    {
        StoredTable<R> subTable = selector.selectedIndexTable(i);
        QuerySpec subQuery = selector.selectedQuery(i);
        String subQueryStr = subQuery.toString();

        var subFactory = (ScanControllerFactory<R>)
            subTable.cacheObtain(toCacheType(type), subQueryStr, subQuery);

        if ((type & FOR_UPDATE) == 0 && subFactory.loadsOne()) {
            if (subTable.joinedPrimaryTableClass() != null) {
                // FIXME: This optimization doesn't work because JoinedScanController needs a
                // real Cursor. Can I obtain a different subFactory which creates controllers
                // whose evaluator validates using a double check? It also needs to combine
                // locks such that they can both be unlocked when rows are filtered out.
            } else {
                // Return an optimized launcher.
                return new LoadOneQueryLauncher<>(subTable, subFactory);
            }
        }

        if (selector.selectedReverse(i)) {
            subFactory = subFactory.reverse();
        }

        return new ScanQueryLauncher<>(subTable, subFactory);
    }

    /**
     * To be called when the set of available secondary indexes and alternate keys has changed.
     */
    void clearQueryCache() {
        cacheTraverse(value -> {
            if (value instanceof QueryLauncher launcher) {
                launcher.clearCache();
            }
        });
    }

    /**
     * Partially decodes a row from a key.
     */
    protected abstract R toRow(byte[] key);

    protected final RowStore rowStore() throws DatabaseException {
        var rs = rowStoreRef().get();
        if (rs == null) {
            throw new DatabaseException("Closed");
        }
        return rs;
    }

    protected final WeakReference<RowStore> rowStoreRef() {
        return mTableManager.mRowStoreRef;
    }

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
     * @see DecodePartialMaker
     */
    @SuppressWarnings("unchecked")
    protected final MethodHandle decodePartialHandle(byte[] spec, int schemaVersion) {
        WeakCache<TupleKey, MethodHandle, byte[]> cache = mDecodePartialCache;

        if (cache == null) {
            cache = new WeakCache<>() {
                @Override
                protected MethodHandle newValue(TupleKey key, byte[] spec) {
                    int schemaVersion = 0;
                    if (key.size() == 2) {
                        schemaVersion = key.get_int(0);
                    }
                    return makeDecodePartialHandle(spec, schemaVersion);
                }
            };

            var existing = (WeakCache<TupleKey, MethodHandle, byte[]>)
                cDecodePartialCacheHandle.compareAndExchange(this, null, cache);

            if (existing != null) {
                cache = existing;
            }
        }

        final TupleKey key = schemaVersion == 0 ?
            TupleKey.make.with(spec) : TupleKey.make.with(schemaVersion, spec);

        return cache.obtain(key, spec);
    }

    protected abstract MethodHandle makeDecodePartialHandle(byte[] spec, int schemaVersion);

    /**
     * Returns a MethodHandle suitable for writing rows from evolvable or unevolvable
     * tables. The set of row columns which are written is defined by the projection
     * specification.
     *
     * For evolvable tables, the MethodType is:
     *     void (int schemaVersion, RowWriter writer, byte[] key, byte[] value)
     *
     * For unevolvable tables, the MethodType is:
     *     void (RowWriter writer, byte[] key, byte[] value)
     *
     * @param spec can be null if all columns are projected
     */
    @SuppressWarnings("unchecked")
    protected final MethodHandle writeRowHandle(byte[] spec) {
        WeakCache<Object, MethodHandle, byte[]> cache = mWriteRowCache;

        if (cache == null) {
            cache = new WeakCache<>() {
                @Override
                protected MethodHandle newValue(Object key, byte[] spec) {
                    if (isEvolvable()) {
                        return WriteRowMaker.makeWriteRowHandle
                            (rowStoreRef(), rowType(), mSource.id(), spec);
                    }

                    RowInfo primaryRowInfo = RowInfo.find(rowType());
                    byte[] secondaryDesc = secondaryDescriptor();

                    RowInfo rowInfo;
                    if (secondaryDesc == null) {
                        rowInfo = primaryRowInfo;
                    } else {
                        rowInfo = RowStore.secondaryRowInfo(primaryRowInfo, secondaryDesc);
                    }

                    return WriteRowMaker.makeWriteRowHandle(rowInfo, spec);
                }
            };

            var existing = (WeakCache<Object, MethodHandle, byte[]>)
                cWriteRowCacheHandle.compareAndExchange(this, null, cache);

            if (existing != null) {
                cache = existing;
            }
        }

        return cache.obtain(TupleKey.make.with(spec), spec);
    }

    @Override
    public final RemoteTableProxy newRemoteProxy(byte[] descriptor) throws IOException {
        int schemaVersion;
        if (!isEvolvable()) {
            schemaVersion = 0;
        } else {
            RowInfo rowInfo = RowInfo.find(rowType());
            schemaVersion = rowStore().schemaVersion(rowInfo, false, mSource.id(), true);
        }
        return RemoteProxyMaker.make(this, schemaVersion, descriptor);
    }

    /**
     * This method should be called to replicate predicate lock acquisition before a local
     * mIndexLock.openAcquire is called. The given transaction should be nested, and it should
     * be exited after performing the local insert operation. If no local call to openAcquire
     * is made, calling redoPredicateMode is harmful because it causes the replica to acquire
     * an extra lock, which can lead to deadlock.
     *
     * <p>Note that operations which won't directly insert a new record might still trigger
     * changes to secondary indexes. This will cause secondary index records to be inserted, and
     * so a predicate lock is still required to ensure serializable isolation.
     *
     * <p>This method must be public to be accessible by code generated by
     * DynamicTableMaker.indyDoUpdate. The generated code won't be a subclass of StoredTable, and
     * so it cannot access this method if it was protected.
     */
    public final void redoPredicateMode(Transaction txn) throws IOException {
        mIndexLock.redoPredicateMode(txn);
    }

    @Override
    public final Transaction enterScope(Transaction txn) throws IOException {
        return ViewUtils.enterScope(mSource, txn);
    }

    /**
     * Called when no trigger is installed.
     */
    protected final void storeNoTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);
            try (var closer = mIndexLock.openAcquire(txn, row)) {
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
    protected final byte[] exchangeNoTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        byte[] oldValue;
        try {
            redoPredicateMode(txn);
            try (var closer = mIndexLock.openAcquire(txn, row)) {
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
    protected final boolean tryInsertNoTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        boolean result;
        try {
            redoPredicateMode(txn);
            try (var closer = mIndexLock.openAcquire(txn, row)) {
                result = source.insert(txn, key, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }

        return result;
    }

    /**
     * Called when no trigger is installed.
     */
    protected final boolean tryReplaceNoTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mSource;

        // Note that although that calling replace doesn't insert a new physical row,
        // it can insert a logical row, depending on which query predicates now match.
        // For this reason, it needs to always call openAcquire.

        // RowPredicateLock requires a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        boolean result;
        try {
            redoPredicateMode(txn);
            try (var closer = mIndexLock.openAcquire(txn, row)) {
                result = source.replace(txn, key, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }

        return result;
    }

    /**
     * Store a fully encoded row and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @see RemoteProxyMaker
     */
    final void storeAndTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        // This method must never be called on a secondary index or alternate key, and so
        // blindly assume that a trigger exists.

        // Note that this method resembles the same code that is generated by TableMaker and
        // StaticTableMaker. One major difference is that the row columns aren't marked clean.

        Index source = mSource;

        // RowPredicateLock and Trigger require a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);

            while (true) {
                Trigger<R> trigger = trigger();
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();

                    if (mode == Trigger.SKIP) {
                        try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                            source.store(txn, key, value);
                        }
                        txn.commit();
                        return;
                    }

                    if (mode != Trigger.DISABLED) {
                        try (var c = source.newCursor(txn)) {
                            try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                                c.find(key);
                            }
                            byte[] oldValue = c.value();
                            if (oldValue == null) {
                                trigger.insertP(txn, row, key, value);
                            } else {
                                trigger.storeP(txn, row, key, oldValue, value);
                            }
                            c.commit(value);
                            return;
                        }
                    }
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
    }

    /**
     * Exchange a fully encoded row and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @return the replaced value, or null if none
     * @see RemoteProxyMaker
     */
    final byte[] exchangeAndTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        // See comments in storeAndTrigger.

        Index source = mSource;

        // RowPredicateLock and Trigger require a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);

            while (true) {
                Trigger<R> trigger = trigger();
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();

                    if (mode == Trigger.SKIP) {
                        byte[] oldValue;
                        try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                            oldValue = source.exchange(txn, key, value);
                        }
                        txn.commit();
                        return oldValue;
                    }

                    if (mode != Trigger.DISABLED) {
                        try (var c = source.newCursor(txn)) {
                            try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                                c.find(key);
                            }
                            byte[] oldValue = c.value();
                            if (oldValue == null) {
                                trigger.insertP(txn, row, key, value);
                            } else {
                                trigger.storeP(txn, row, key, oldValue, value);
                            }
                            c.commit(value);
                            return oldValue;
                        }
                    }
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
    }

    /**
     * Insert a fully encoded row and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @see RemoteProxyMaker
     */
    final boolean insertAndTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        // See comments in storeAndTrigger.

        Index source = mSource;

        // RowPredicateLock and Trigger require a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);

            while (true) {
                Trigger<R> trigger = trigger();
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();

                    if (mode == Trigger.SKIP) {
                        boolean result;
                        try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                            result = source.insert(txn, key, value);
                        }
                        txn.commit();
                        return result;
                    }

                    if (mode != Trigger.DISABLED) {
                        try (var c = source.newCursor(txn)) {
                            c.autoload(false);
                            try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                                c.find(key);
                            }
                            if (c.value() != null) {
                                return false;
                            } else {
                                trigger.insertP(txn, row, key, value);
                                c.commit(value);
                                return true;
                            }
                        }
                    }
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
    }

    /**
     * Replace a fully encoded row and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @see RemoteProxyMaker
     */
    final boolean replaceAndTrigger(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        // See comments in storeAndTrigger.

        Index source = mSource;

        // Note that although that calling replace doesn't insert a new physical row,
        // it can insert a logical row, depending on which query predicates now match.
        // For this reason, it needs to always call openAcquire.

        // RowPredicateLock and Trigger require a non-null transaction.
        txn = ViewUtils.enterScope(source, txn);
        try {
            redoPredicateMode(txn);

            while (true) {
                Trigger<R> trigger = trigger();
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();

                    if (mode == Trigger.SKIP) {
                        try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                            if (source.replace(txn, key, value)) {
                                txn.commit();
                                return true;
                            }
                        }
                        return false;
                    }

                    if (mode != Trigger.DISABLED) {
                        try (var c = source.newCursor(txn)) {
                            try (var closer = mIndexLock.openAcquireP(txn, row, key, value)) {
                                c.find(key);
                            }
                            byte[] oldValue = c.value();
                            if (oldValue == null) {
                                return false;
                            }
                            c.store(value);
                            trigger.storeP(txn, row, key, oldValue, value);
                            txn.commit();
                            return true;
                        }
                    }
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
    }

    /**
     * @see #updateAndTrigger
     */
    @FunctionalInterface
    public static interface ValueUpdater {
        /**
         * Given an existing encoded row value, return a new encoded row value which has a
         * schema version encoded.
         */
        byte[] updateValue(byte[] value);
    }

    /**
     * Update a fully encoded key and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @see RemoteProxyMaker
     * @return the new value, or null if none
     */
    final byte[] updateAndTrigger(Transaction txn, R row, byte[] key, ValueUpdater updater)
        throws IOException
    {
        // See comments in storeAndTrigger.

        // Note that although that calling update doesn't insert a new physical row,
        // it can insert a logical row, depending on which query predicates now match.
        // For this reason, it needs to always call openAcquire.

        // RowPredicateLock and Trigger require a non-null transaction.
        txn = ViewUtils.enterScope(mSource, txn);
        try (var c = mSource.newCursor(txn)) {
            byte[] originalValue, newValue;

            while (true) {
                LockResult result = c.find(key);
                originalValue = c.value();
                if (originalValue == null) {
                    return null;
                }
                newValue = updater.updateValue(originalValue);
                if (tryOpenAcquireP(txn, row, key, c, result, newValue)) {
                    break;
                }
            }

            while (true) {
                Trigger<R> trigger = trigger();
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();

                    if (mode == Trigger.SKIP) {
                        c.commit(newValue);
                        return newValue;
                    }

                    if (mode != Trigger.DISABLED) {
                        c.store(newValue);
                        trigger.storeP(txn, row, key, originalValue, newValue);
                        txn.commit();
                        return newValue;
                    }
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
    }

    /**
     * Attempt to acquire the predicate lock after acquiring the row lock, which is the
     * opposite of the canonical lock acquisition order, and can lead to deadlock.
     *
     * <p>This method must be public to be accessible by code generated by
     * DynamicTableMaker.indyDoUpdate. The generated code won't be a subclass of StoredTable, and
     * so it cannot access this method if it was protected.
     *
     * @param txn non-null
     * @param row row being updated; can be partially filled in
     * @param c cursor positioned at the key
     * @param result result from finding the key
     * @param newValue the new binary value for the row
     * @return false if attempt failed and caller should start over by finding the key again
     */
    public final boolean tryOpenAcquireP(Transaction txn, R row, byte[] key,
                                         Cursor c, LockResult result, byte[] newValue)
        throws IOException
    {
        redoPredicateMode(txn);

        if (result != LockResult.ACQUIRED) {
            // The row lock was already held, and so nothing special can be done here to
            // prevent deadlock.
            mIndexLock.openAcquireP(txn, row, key, newValue).close();
            return true;
        }

        // Note that a null row instance is passed to this method because it assumes that a
        // non-null row instance is completely filled in.
        var closer = mIndexLock.tryOpenAcquire(txn, null, key, newValue);

        if (closer != null) {
            closer.close();
            return true;
        }

        // Unlock the row, acquire the predicate lock, and load the row again.

        txn.unlock();
        byte[] originalValue = c.value();

        try (var closer2 = mIndexLock.openAcquireP(txn, row, key, newValue)) {
            c.find(key);
        }

        if (Arrays.equals(originalValue, c.value())) {
            return true;
        }

        // The row changed, so rollback and start over.
        txn.rollback();
        Thread.yield();

        return false;
    }

    /**
     * Delete a fully encoded key and invoke a trigger.
     *
     * @see RemoteProxyMaker
     */
    final boolean deleteAndTrigger(Transaction txn, byte[] key) throws IOException {
        // See comments in storeAndTrigger.

        while (true) {
            Trigger<R> trigger = trigger();
            trigger.acquireShared();
            try {
                int mode = trigger.mode();

                if (mode == Trigger.SKIP) {
                    return mSource.delete(txn, key);
                }

                if (mode != Trigger.DISABLED) {
                    Index source = mSource;

                    // Trigger requires a non-null transaction.
                    txn = ViewUtils.enterScope(source, txn);
                    try (var c = source.newCursor(txn)) {
                        c.find(key);
                        byte[] oldValue = c.value();
                        if (oldValue == null) {
                            return false;
                        } else {
                            trigger.delete(txn, key, oldValue);
                            c.commit(null);
                            return true;
                        }
                    } finally {
                        txn.exit();
                    }
                }
            } finally {
                trigger.releaseShared();
            }
        }
    }

    /**
     * Store a fully encoded row and invoke a trigger. The given row instance is expected to
     * be empty, and it's never modified by this method.
     *
     * @see RemoteProxyMaker
     * @return actual key that was stored
     */
    final byte[] storeAutoAndTrigger(AutomaticKeyGenerator<R> autogen,
                                     Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        // The row object is only passed to the Trigger.insertP method, because it always
        // requires a row even if it's empty. The row is never passed to the
        // AutomaticKeyGenerator, forcing it to call the Applier.tryOpenAcquire method instead
        // of applyToRow. This ensures that the row isn't modified.

        // Note that this method resembles the same code that is generated by TableMaker and
        // StaticTableMaker. One major difference is that the row columns aren't marked clean.

        // Call enterScopex because bogus transaction doesn't work with AutomaticKeyGenerator.
        txn = ViewUtils.enterScopex(mSource, txn);
        try {
            // Enable redoPredicateMode now because the call to the AutomaticKeyGenerator will
            // acquire a predicate lock.
            redoPredicateMode(txn);

            while (true) {
                Trigger<R> trigger = trigger();

                if (trigger == null) {
                    key = autogen.store(txn, null, key, value);
                    txn.commit();
                    return key;
                }

                trigger.acquireShared();
                try {
                    int mode = trigger.mode();
                    if (mode == Trigger.SKIP) {
                        key = autogen.store(txn, null, key, value);
                    } else if (mode == Trigger.DISABLED) {
                        continue;
                    } else {
                        key = autogen.store(txn, null, key, value);
                        trigger.insertP(txn, row, key, value);
                    }
                    txn.commit();
                    return key;
                } finally {
                    trigger.releaseShared();
                }
            }
        } finally {
            txn.exit();
        }
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
    protected StoredTable<R> joinedPrimaryTable() {
        return null;
    }

    /**
     * Override if this table implements a secondary index and joins to the primary.
     */
    protected Class<?> joinedPrimaryTableClass() {
        return null;
    }

    /**
     * An evolvable table has a schema version encoded. This method is overridden by
     * StoredTableIndex to always return false;
     */
    boolean isEvolvable() {
        return isEvolvable(rowType());
    }

    static boolean isEvolvable(Class<?> rowType) {
        return rowType != Entry.class;
    }

    /**
     * Note: Is overridden by StoredTableIndex to always return false.
     */
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
    @SuppressWarnings("unchecked")
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
     * sometimes accessed from generated code which isn't a subclass of StoredTable.
     */
    @SuppressWarnings("unchecked")
    public final Trigger<R> trigger() {
        return (Trigger<R>) cTriggerHandle.getOpaque(this);
    }

    static RowFilter parseFilter(Class<?> rowType, String queryStr) {
        return Parser.parseQuerySpec(rowType, queryStr).filter();
    }
}
