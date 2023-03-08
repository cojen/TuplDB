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

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Supports queries that only scan over one table, although it might still perform a join.
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
    public Scanner<R> newScanner(Transaction txn, R row, Object... args) throws IOException {
        return mTable.newScanner(txn, row, mFactory.scanController(args));
    }

    @Override
    public Updater<R> newUpdater(Transaction txn, R row, Object... args) throws IOException {
        return mTable.newUpdater(txn, row, mFactory.scanController(args));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void scanWrite(Transaction txn, RowWriter writer, Object... args) throws IOException {
        // Pass the writer as if it's a row, but it's actually a RowConsumer.
        Scanner<R> scanner = newScanner(txn, (R) writer, args);
        try {
            while (scanner.step((R) writer) != null);
        } catch (Throwable e) {
            RowUtils.closeQuietly(scanner);
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public QueryPlan plan(Object... args) {
        return mFactory.plan(args);
    }

    @Override
    public Set<String> projection() {
        return mProjection;
    }

    @Override
    public void close() throws IOException {
        mTable.close();
    }
}
