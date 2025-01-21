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
import org.cojen.tupl.Updater;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.table.expr.CompiledQuery;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
abstract class MultiSourceTable<R> extends BaseTable<R> {
    protected final Table<R>[] mSources;

    /**
     * @param sources must have at least one element
     */
    protected MultiSourceTable(Table<R>[] sources) {
        mSources = sources;
    }

    @Override
    public Class<R> rowType() {
        return mSources[0].rowType();
    }

    @Override
    public R newRow() {
        return mSources[0].newRow();
    }

    @Override
    public R cloneRow(R row) {
        return mSources[0].cloneRow(row);
    }

    @Override
    public void unsetRow(R row) {
        mSources[0].unsetRow(row);
    }

    @Override
    public void cleanRow(R row) {
        mSources[0].cleanRow(row);
    }

    @Override
    public void copyRow(R from, R to) {
        mSources[0].copyRow(from, to);
    }

    @Override
    public boolean isSet(R row, String name) {
        return mSources[0].isSet(row, name);
    }

    @Override
    public void forEach(R row, ColumnProcessor<? super R> action) {
        mSources[0].forEach(row, action);
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn) throws IOException {
        return newScanner(row, txn, "{*}", RowUtils.NO_ARGS);
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn) throws IOException {
        return newUpdater(row, txn, "{*}", RowUtils.NO_ARGS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Query<R> query(String query) throws IOException {
        return (Query<R>) cacheObtain(TYPE_1, query, null);
    }

    @Override
    public boolean anyRows(R row, Transaction txn) throws IOException {
        for (var source : mSources) {
            if (source.anyRows(row, txn)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean anyRows(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        // Calling the query method validates the query for all sources beforehand.
        return query(query).anyRows(row, txn, args);
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSources[0].newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        for (var source : mSources) {
            if (!source.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean tryLoad(Transaction txn, R row) throws IOException {
        throw new ViewConstraintException();
    }

    @Override
    public boolean exists(Transaction txn, R row) throws IOException {
        throw new ViewConstraintException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        // See the cacheNewValue method.
        var key = new CompiledQuery.DerivedKey(derivedType, query);
        return ((CompiledQuery<D>) cacheObtain(TYPE_2, key, this)).table(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table<Row> derive(String query, Object... args) throws IOException {
        // See the cacheNewValue method.
        return ((CompiledQuery<Row>) cacheObtain(TYPE_2, query, this)).table(args);
    }

    @Override
    public void close() {
        // Do nothing.
    }

    @Override
    public boolean isClosed() {
        for (var source : mSources) {
            if (source.isClosed()) {
                return true;
            }
        }
        return false;
    }
}
