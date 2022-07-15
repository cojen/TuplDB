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
        return new SortedRowScanner<R>(mTable, mSpec, mComparator, mSource, txn, args);
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, R row, Object... args) throws IOException {
        // FIXME: Sorted RowUpdater.
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPlan plan(Object... args) {
        return new QueryPlan.Sort(OrderBy.splitSpec(mSpec), mSource.plan(args));
    }

    @Override
    public Predicate<R> predicate(Object... args) {
        return mSource.predicate(args);
    }

    @Override
    public Set<String> projection() {
        return mSource.projection();
    }
}
