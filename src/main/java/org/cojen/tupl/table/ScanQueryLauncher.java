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


import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Supports queries that only scan over one table, although it might still perform a join.
 *
 * @author Brian S O'Neill
 */
class ScanQueryLauncher<R> extends QueryLauncher<R> {
    protected final StoredTable<R> mTable;
    protected final ScanControllerFactory<R> mFactory;

    ScanQueryLauncher(StoredTable<R> table, ScanControllerFactory<R> factory) {
        mTable = table;
        mFactory = factory;
    }

    @Override
    public Class<R> rowType() {
        return mTable.rowType();
    }

    @Override
    public int argumentCount() {
        return mFactory.argumentCount();
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
        return mTable.newScanner(row, txn, mFactory.scanController(args));
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        return mTable.newUpdater(row, txn, mFactory.scanController(args));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void scanWrite(Transaction txn, RowWriter writer, Object... args) throws IOException {
        // Pass the writer as if it's a row, but it's actually a RowConsumer.
        Scanner<R> scanner = newScanner((R) writer, txn, args);
        try {
            while (scanner.step((R) writer) != null);
        } catch (Throwable e) {
            RowUtils.closeQuietly(scanner);
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) {
        return mFactory.plan(args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, Object... args) {
        return mFactory.plan(args);
    }

    @Override
    protected void closeIndexes() throws IOException {
        mTable.close();
    }

    @Override
    protected void clearCache() {
        // Nothing to do.
    }
}
