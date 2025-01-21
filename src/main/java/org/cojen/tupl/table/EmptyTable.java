/*
 *  Copyright (C) 2025 Cojen.org
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

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class EmptyTable<R> implements Table<R> {
    private final Table<R> mEmpty;

    public EmptyTable(Class<R> rowType) throws IOException {
        // Although the derived table functions as an empty table, the exceptions it throws
        // aren't consistent with how a "true" empty table should behave. Wrap it.
        mEmpty = Table.join().derive(rowType, "false");
    }

    private EmptyTable(Table<R> empty) {
        mEmpty = empty;
    }

    @Override
    public boolean hasPrimaryKey() {
        return mEmpty.hasPrimaryKey();
    }

    @Override
    public Class<R> rowType() {
        return mEmpty.rowType();
    }

    @Override
    public R newRow() {
        return mEmpty.newRow();
    }

    @Override
    public R cloneRow(R row) {
        return mEmpty.cloneRow(row);
    }

    @Override
    public void unsetRow(R row) {
        mEmpty.unsetRow(row);
    }

    @Override
    public void cleanRow(R row) {
        mEmpty.cleanRow(row);
    }

    @Override
    public void copyRow(R from, R to) {
        mEmpty.copyRow(from, to);
    }

    @Override
    public boolean isSet(R row, String name) {
        return mEmpty.isSet(row, name);
    }

    @Override
    public void forEach(R row, ColumnProcessor<? super R> action) {
        mEmpty.forEach(row, action);
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn) {
        return EmptyScanner.the();
    }

    @Override
    public Query<R> query(String query) throws IOException {
        mEmpty.query(query); // validate the query
        return new EmptyQuery<>(rowType());
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mEmpty.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean tryLoad(Transaction txn, R row) {
        return false;
    }

    @Override
    public boolean exists(Transaction txn, R row) {
        return false;
    }

    @Override
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        return new EmptyTable<>(mEmpty.derive(derivedType, query, args));
    }

    @Override
    public Table<Row> derive(String query, Object... args) throws IOException {
        return new EmptyTable<>(mEmpty.derive(query, args));
    }

    @Override
    public Table<R> distinct() {
        return this;
    }

    @Override
    public void close() {
        // Do nothing.
    }

    @Override
    public boolean isClosed() {
        return mEmpty.isClosed();
    }
}
