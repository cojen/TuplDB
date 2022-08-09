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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Comparator;
import java.util.Set;

import java.util.function.Predicate;

import org.cojen.tupl.LockMode;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SortedQueryLauncher<R> implements QueryLauncher<R> {
    private final BaseTable<R> mTable;
    private final QueryLauncher<R> mSource;
    private final String mSpec;
    private final Comparator<R> mComparator;

    SortedQueryLauncher(BaseTable<R> table, QueryLauncher<R> source, OrderBy orderBy) {
        mTable = table;
        mSource = source;
        mSpec = orderBy.spec();
        mComparator = table.comparator(mSpec);
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, R row, Object... args) throws IOException {
        return RowSorter.sort(mTable, mSpec, mComparator, mSource, txn, args);
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, R row, Object... args) throws IOException {
        if (txn != null) {
            if (txn.lockMode() != LockMode.UNSAFE) {
                txn.enter();
            }

            RowScanner<R> scanner;
            try {
                scanner = newRowScanner(txn, row, args);
                txn.commit(); // keep the locks
            } finally {
                txn.exit();
            }

            return new WrappedRowUpdater<>(mTable, txn, scanner);
        }

        // Need to create a transaction to acquire locks, but true auto-commit behevior isn't
        // really feasible because update order won't match lock acquisition order. Instead,
        // keep the transaction open until the updater finishes and always commit.
        // Unfortunately, if the commit fails then all updates fail instead of just one.

        txn = mTable.mSource.newTransaction(null);

        RowScanner<R> scanner;
        try {
            scanner = newRowScanner(txn, row, args);
        } catch (Throwable e) {
            txn.exit();
            throw e;
        }

        return new WrappedRowUpdater<>(mTable, txn, scanner) {
            @Override
            public R step() throws IOException {
                try {
                    R row = mScanner.step();
                    if (row == null) {
                        exception(null);
                    }
                    return row;
                } catch (Throwable e) {
                    exception(e);
                    throw e;
                }
            }

            @Override
            public R step(R row) throws IOException {
                try {
                    row = mScanner.step(row);
                    if (row == null) {
                        exception(null);
                    }
                    return row;
                } catch (Throwable e) {
                    exception(e);
                    throw e;
                }
            }

            @Override
            public void close() throws IOException {
                exception(null);
                mScanner.close();
            }

            @Override
            protected void exception(Throwable e) throws IOException {
                try {
                    mTxn.commit();
                } catch (Throwable e2) {
                    if (e == null) {
                        throw e2;
                    } else {
                        RowUtils.suppress(e, e2);
                    }
                }
            }
        };
    }

    @Override
    public QueryPlan plan(Object... args) {
        return new QueryPlan.Sort(OrderBy.splitSpec(mSpec), mSource.plan(args));
    }

    @Override
    public Set<String> projection() {
        return mSource.projection();
    }

    @Override
    public int characteristics() {
        return mSource.characteristics();
    }
}
