/*
 *  Copyright (C) 2024 Cojen.org
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

import java.lang.ref.WeakReference;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Table;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.expr.Parser;
import org.cojen.tupl.table.expr.RelationExpr;

import org.cojen.tupl.table.filter.QuerySpec;

import static org.cojen.tupl.table.StoredTable.*;

/**
 * Defines a delegating launcher which lazily creates the underlying launchers by calling into
 * a StoredTable instance. The queries supported by the StoredQueryLauncher are "foundational"
 * in nature, supporting the kinds of operations which can be optimized by index selection and
 * proper index utilization.
 *
 * @author Brian S. O'Neill
 */
final class StoredQueryLauncher<R> extends QueryLauncher<R> {
    /**
     * Parses a query against a table into the same row type, throwing a QueryException if the
     * operation fails.
     */
    static RelationExpr parse(Table<?> table, String queryStr) {
        return Parser.parse(table, table.rowType(), queryStr);
    }

    /**
     * Makes a StoredQueryLauncher from a parsed query if it's relatively simple and can be
     * represented by a QuerySpec, or else returns a CompiledQuery instance.
     *
     * @param expr the parsed query
     */
    static <R> QueryLauncher<R> make(StoredTable<R> table, String queryStr, RelationExpr expr)
        throws IOException
    {
        QuerySpec query = expr.tryQuerySpec(table.rowType());
        if (query != null) {
            return new StoredQueryLauncher<>(table, queryStr, query.reduce());
        } else {
            return expr.makeCompiledQuery(table.rowType());
        }
    }

    private final StoredTable<R> mTable;
    private final String mQueryStr;

    private volatile WeakReference<QuerySpec> mQueryRef;

    private volatile QueryLauncher<R>
        mForScanner, mForScannerDoubleCheck,
        mForUpdater, mForUpdaterDoubleCheck;

    private StoredQueryLauncher(StoredTable<R> table, String queryStr, QuerySpec query) {
        mTable = table;

        String newString = query.toString();
        if (!newString.equals(queryStr)) {
            queryStr = newString;
        }

        mQueryStr = queryStr;

        mQueryRef = new WeakReference<>(query);
    }

    @Override
    public Class<R> rowType() {
        return mTable.rowType();
    }

    @Override
    public int argumentCount() {
        try {
            return forScanner().argumentCount();
        } catch (IOException e) {
            return query().filter().maxArgument();
        }
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

        closeIndexes(mForScanner);
        closeIndexes(mForScannerDoubleCheck);
        closeIndexes(mForUpdater);
        closeIndexes(mForUpdaterDoubleCheck);

        clearCache();
    }

    private static void closeIndexes(QueryLauncher<?> launcher) throws IOException {
        if (launcher != null) {
            launcher.closeIndexes();
        }
    }

    /**
     * Clears the cached launcher instances.
     */
    @Override
    protected void clearCache() {
        mForScanner = null;
        mForScannerDoubleCheck = null;
        mForUpdater = null;
        mForUpdaterDoubleCheck = null;
    }

    private QueryLauncher<R> forScanner(Transaction txn) throws IOException {
        // Might need to double-check the filter after joining to the primary, in case
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
        // Might need to double-check the filter after joining to the primary, in case
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
            selector = new IndexSelector<R>(mTable, rowInfo, query(), (type & FOR_UPDATE) != 0);
        }

        if ((type & DOUBLE_CHECK) != 0 && selector.noJoins()) {
            // Double-checking is only needed when a secondary joins to a primary.
            launcher = queryLauncher(type & ~DOUBLE_CHECK, selector);
        } else {
            launcher = mTable.newQueryLauncher(type, selector);

            if (launcher == null) {
                // No special update rule is needed.
                assert (type & FOR_UPDATE) != 0;
                launcher = queryLauncher(type & ~FOR_UPDATE, selector);
            }
        }

        switch (type) {
            default -> mForScanner = launcher;
            case DOUBLE_CHECK -> mForScannerDoubleCheck = launcher;
            case FOR_UPDATE -> mForUpdater = launcher;
            case FOR_UPDATE_DOUBLE_CHECK -> mForUpdaterDoubleCheck = launcher;
        }

        return launcher;
    }

    private QuerySpec query() {
        QuerySpec query = mQueryRef.get();
        if (query == null) {
            query = parse(mTable, mQueryStr).querySpec();
            mQueryRef = new WeakReference<>(query);
        }
        return query;
    }

    /**
     * To be called when attempting to launch a new scanner or updater and an exception is
     * thrown. An index might have been dropped, so check and retry. Returns a new
     * QueryLauncher to use or else throws the original exception.
     */
    private QueryLauncher<R> retry(Throwable cause) {
        // A ClosedIndexException could have come from the dropped index directly, and a
        // LockFailureException could be caused while waiting for the index lock. Other
        // exceptions aren't expected so don't bother trying to obtain a new launcher.
        if (cause instanceof ClosedIndexException || cause instanceof LockFailureException) {
            QueryLauncher<R> newLauncher;
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
