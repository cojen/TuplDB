/*
 *  Copyright (C) 2022 Cojen.org
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

import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.Map;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.filter.Parser;
import org.cojen.tupl.table.filter.QuerySpec;

import static org.cojen.tupl.table.BaseTable.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class QueryLauncher<R> implements Query<R> {
    /**
     * @param row initial row; can be null
     */
    @Override
    public abstract Scanner<R> newScanner(R row, Transaction txn, Object... args)
        throws IOException;

    /**
     * @param row initial row; can be null
     */
    @Override
    public abstract Updater<R> newUpdater(R row, Transaction txn, Object... args)
        throws IOException;

    /**
     * Scan and write rows to a remote endpoint.
     */
    public abstract void scanWrite(Transaction txn, RowWriter writer, Object... args)
        throws IOException;

    @Override
    public abstract QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException;

    @Override
    public abstract QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException;

    /**
     * Is called when the BaseTable is closed, in order to close all the secondary indexes
     * referenced by this query. This method must not be public.
     */
    protected void closeIndexes() throws IOException {
    }

    /**
     * Defines a delegating launcher which lazily creates the underlying launchers.
     */
    static final class Delegate<R> extends QueryLauncher<R> {
        final BaseTable<R> mTable;
        final String mQueryStr;

        private WeakReference<QuerySpec> mQueryRef;

        private QueryLauncher<R>
            mForScanner, mForScannerDoubleCheck,
            mForUpdater, mForUpdaterDoubleCheck;

        Delegate(BaseTable<R> table, String queryStr) {
            RowInfo rowInfo = RowInfo.find(table.rowType());
            Map<String, ColumnInfo> allColumns = rowInfo.allColumns;
            QuerySpec query = new Parser(allColumns, queryStr).parseQuery(allColumns).reduce();

            mTable = table;

            String newString = query.toString();
            if (!newString.equals(queryStr)) {
                queryStr = newString;
            }

            mQueryStr = queryStr;

            mQueryRef = new WeakReference<>(query);
        }

        /**
         * @param query canonical query
         */
        Delegate(BaseTable<R> table, QuerySpec query) {
            mTable = table;
            mQueryStr = query.toString();
            mQueryRef = new WeakReference<>(query);
        }

        public String canonicalQueryString() {
            return mQueryStr;
        }

        public QuerySpec canonicalQuery() {
            return query(null);
        }

        @Override
        public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
            try {
                return forScanner(txn).newScanner(row, txn, args);
            } catch (Throwable e) {
                return retry(e).newScanner(row, txn, args);
            }
        }

        @Override
        public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
            try {
                return forUpdater(txn).newUpdater(row, txn, args);
            } catch (Throwable e) {
                return retry(e).newUpdater(row, txn, args);
            }
        }

        @Override
        public void scanWrite(Transaction txn, RowWriter writer, Object... args)
            throws IOException
        {
            // FIXME: Attempt to retry if nothing has been written yet.
            forScanner(txn).scanWrite(txn, writer, args);
        }

        /* FIXME: Override and optimize deleteAll.
        @Override
        public long deleteAll(Transaction txn, Object... args) throws IOException {
        */

        @Override
        public QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
            return forScanner(txn).scannerPlan(txn, args);
        }

        @Override
        public QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
            return forUpdater(txn).updaterPlan(txn, args);
        }

        @Override
        protected void closeIndexes() throws IOException {
            mTable.close();

            QueryLauncher<R> forScanner, forScannerDoubleCheck, forUpdater, forUpdaterDoubleCheck;

            synchronized (this) {
                forScanner = mForScanner;
                mForScanner = null;

                forScannerDoubleCheck = mForScannerDoubleCheck;
                mForScannerDoubleCheck = null;

                forUpdater = mForUpdater;
                mForUpdater = null;

                forUpdaterDoubleCheck = mForUpdaterDoubleCheck;
                mForUpdaterDoubleCheck = null;
            }

            closeIndexes(forScanner);
            closeIndexes(forScannerDoubleCheck);
            closeIndexes(forUpdater);
            closeIndexes(forUpdaterDoubleCheck);
        }

        private static void closeIndexes(QueryLauncher<?> launcher) throws IOException {
            if (launcher != null) {
                launcher.closeIndexes();
            }
        }

        /**
         * Clears the cached launcher instances.
         */
        public synchronized void clear() {
            mForScanner = null;
            mForScannerDoubleCheck = null;
            mForUpdater = null;
            mForUpdaterDoubleCheck = null;
        }

        private QueryLauncher<R> forScanner(Transaction txn) throws IOException {
            // Might need to double check the filter after joining to the primary, in case
            // there were any changes after the secondary entry was loaded.
            return !RowUtils.isUnlocked(txn) ? forScanner() : forScannerDoubleCheck();
        }

        private QueryLauncher<R> forScanner() throws IOException {
            QueryLauncher<R> forScanner = mForScanner;
            return forScanner == null ? queryLauncher(PLAIN) : forScanner;
        }

        private QueryLauncher<R> forScannerDoubleCheck() throws IOException {
            QueryLauncher<R> forScanner = mForScannerDoubleCheck;
            return forScanner == null ? queryLauncher(DOUBLE_CHECK) : forScanner;
        }

        private QueryLauncher<R> forUpdater(Transaction txn) throws IOException {
            // Might need to double check the filter after joining to the primary, in case
            // there were any changes after the secondary entry was loaded. Note that no double
            // check is needed with READ_UNCOMMITTED, because the updater still acquires locks.
            return !RowUtils.isUnsafe(txn) ? forUpdater() : forUpdaterDoubleCheck();
        }

        private QueryLauncher<R> forUpdater() throws IOException {
            QueryLauncher<R> forUpdater = mForUpdater;
            return forUpdater == null ? queryLauncher(FOR_UPDATE) : forUpdater;
        }

        private QueryLauncher<R> forUpdaterDoubleCheck() throws IOException {
            QueryLauncher<R> forUpdater = mForUpdaterDoubleCheck;
            return forUpdater == null ? queryLauncher(FOR_UPDATE_DOUBLE_CHECK) : forUpdater;
        }

        private synchronized QueryLauncher<R> queryLauncher(int type) throws IOException {
            return queryLauncher(type, null);
        }

        private synchronized QueryLauncher<R> queryLauncher(int type, IndexSelector selector)
            throws IOException
        {
            QueryLauncher<R> launcher = switch (type) {
                default -> mForScanner;
                case DOUBLE_CHECK -> mForScannerDoubleCheck;
                case FOR_UPDATE -> mForUpdater;
                case FOR_UPDATE_DOUBLE_CHECK -> mForUpdaterDoubleCheck;
            };

            if (launcher != null) {
                return launcher;
            }

            if (selector == null) {
                RowInfo rowInfo = RowInfo.find(mTable.rowType());
                selector = new IndexSelector<R>
                    (mTable, rowInfo, query(rowInfo), (type & FOR_UPDATE) != 0);
            }

            if ((type & DOUBLE_CHECK) != 0 && selector.noJoins()) {
                // Double checking is only needed when a secondary joins to a primary.
                launcher = queryLauncher(type & ~DOUBLE_CHECK, selector);
            } else {
                launcher = mTable.newQueryLauncher(type, selector);

                if (launcher == null) {
                    // No special update rule is needed.
                    assert (type & FOR_UPDATE) != 0;
                    launcher = queryLauncher(type & ~FOR_UPDATE, selector);
                }
            }

            VarHandle.storeStoreFence();

            switch (type) {
                default -> mForScanner = launcher;
                case DOUBLE_CHECK -> mForScannerDoubleCheck = launcher;
                case FOR_UPDATE -> mForUpdater = launcher;
                case FOR_UPDATE_DOUBLE_CHECK -> mForUpdaterDoubleCheck = launcher;
            }

            return launcher;
        }

        /**
         * @param rowInfo can be null
         */
        private QuerySpec query(RowInfo rowInfo) {
            QuerySpec query = mQueryRef.get();
            if (query == null) {
                if (rowInfo == null) {
                    rowInfo = RowInfo.find(mTable.rowType());
                }
                Map<String, ColumnInfo> allColumns = rowInfo.allColumns;
                query = new Parser(allColumns, mQueryStr).parseQuery(allColumns);
                var ref = new WeakReference<>(query);
                VarHandle.storeStoreFence();
                mQueryRef = ref;
            }
            return query;
        }

        /**
         * To be called when attempting to launch a new scanner or updater and an exception is
         * thrown. An index might have been dropped, so check and retry. Returns a new
         * QueryLauncher to use or else throws the original exception.
         */
        private QueryLauncher.Delegate<R> retry(Throwable cause) {
            // A ClosedIndexException could have come from the dropped index directly, and a
            // LockFailureException could be caused while waiting for the index lock. Other
            // exceptions aren't expected so don't bother trying to obtain a new launcher.
            if (cause instanceof ClosedIndexException || cause instanceof LockFailureException) {
                QueryLauncher.Delegate<R> newLauncher;
                try {
                    newLauncher = mTable.query(mQueryStr);
                    if (newLauncher != this) {
                        // Only return the launcher if it changed.
                        return newLauncher;
                    }
                } catch (Throwable e) {
                    Utils.suppress(cause, e);
                }
            }

            throw Utils.rethrow(cause);
        }
    }
}
