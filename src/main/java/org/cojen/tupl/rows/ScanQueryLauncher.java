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

import java.util.Set;

import java.util.function.Predicate;

import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Supports queries that only scan over one table.
 *
 * @author Brian S O'Neill
 */
final class ScanQueryLauncher<R> implements QueryLauncher<R> {
    private final BaseTable<R> mTable;
    private final ScanControllerFactory<R> mFactory;
    private final Set<String> mProjection;

    ScanQueryLauncher(BaseTable<R> table, ScanControllerFactory<R> factory,
                      Set<String> projection)
    {
        mTable = table;
        mFactory = factory;
        mProjection = projection;
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, R row, Object... args) throws IOException {
        return mTable.newRowScanner(txn, row, mFactory.scanController(args));
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, R row, Object... args) throws IOException {
        return mTable.newRowUpdater(txn, row, mFactory.scanController(args));
    }

    @Override
    public QueryPlan plan(Object... args) {
        return mFactory.plan(args);
    }

    @Override
    public Predicate<R> predicate(Object... args) {
        return mFactory.predicate(args);
    }

    @Override
    public Set<String> projection() {
        return mProjection;
    }
}
