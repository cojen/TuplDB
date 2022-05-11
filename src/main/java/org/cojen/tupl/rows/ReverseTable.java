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

import java.util.function.Predicate;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ReverseTable<R> implements Table<R> {
    protected final BaseTable<R> mSource;

    ReverseTable(BaseTable<R> source) {
        mSource = source;
    }

    @Override
    public Class<R> rowType() {
        return mSource.rowType();
    }

    @Override
    public R newRow() {
        return mSource.newRow();
    }

    @Override
    public R cloneRow(R row) {
        return mSource.cloneRow(row);
    }

    @Override
    public void unsetRow(R row) {
        mSource.unsetRow(row);
    }

    @Override
    public void copyRow(R from, R to) {
        mSource.copyRow(from, to);
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn) throws IOException {
        return mSource.newRowScanner(txn, mSource.unfilteredReverse());
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return mSource.scannerQueryLauncher(txn, filter, true).newRowScanner(txn, args);
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn) throws IOException {
        return mSource.newRowUpdater(txn, mSource.unfilteredReverse());
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException
    {
        return mSource.updaterQueryLauncher(txn, filter, true).newRowUpdater(txn, args);
    }

    @Override
    public String toString() {
        return mSource.toString();
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public boolean load(Transaction txn, R row) throws IOException {
        return mSource.load(txn, row);
    }

    @Override
    public boolean exists(Transaction txn, R row) throws IOException {
        return mSource.exists(txn, row);
    }

    @Override
    public void store(Transaction txn, R row) throws IOException {
        mSource.store(txn, row);
    }

    @Override
    public R exchange(Transaction txn, R row) throws IOException {
        return mSource.exchange(txn, row);
    }

    @Override
    public boolean insert(Transaction txn, R row) throws IOException {
        return mSource.insert(txn, row);
    }

    @Override
    public boolean replace(Transaction txn, R row) throws IOException {
        return mSource.replace(txn, row);
    }

    @Override
    public boolean update(Transaction txn, R row) throws IOException {
        return mSource.update(txn, row);
    }

    @Override
    public boolean merge(Transaction txn, R row) throws IOException {
        return mSource.merge(txn, row);
    }

    @Override
    public boolean delete(Transaction txn, R row) throws IOException {
        return mSource.delete(txn, row);
    }

    @Override
    public Comparator<R> comparator(String spec) {
        return mSource.comparator(spec);
    }

    @Override
    public Predicate<R> predicate(String filter, Object... args) {
        return mSource.predicate(filter, args);
    }

    @Override
    public Table<R> viewPrimaryKey() {
        return mSource.viewPrimaryKey().viewReverse();
    }

    @Override
    public Table<R> viewAlternateKey(String... columns) throws IOException {
        return mSource.viewAlternateKey(columns).viewReverse();
    }

    @Override
    public Table<R> viewSecondaryIndex(String... columns) throws IOException {
        return mSource.viewSecondaryIndex(columns).viewReverse();
    }

    @Override
    public Table<R> viewUnjoined() {
        Table<R> unjoined = mSource.viewUnjoined();
        return unjoined == mSource ? this : unjoined.viewReverse();
    }

    @Override
    public Table<R> viewReverse() {
        return mSource;
    }

    @Override
    public QueryPlan queryPlan(Transaction txn, String filter, Object... args) throws IOException {
        if (filter == null) {
            return mSource.planReverse(args);
        } else {
            return mSource.scannerQueryLauncher(txn, filter, true).plan(args);
        }
    }

    static class This<R> extends ReverseTable<R> {
        This(BaseTable<R> source) {
            super(source);
        }

        @Override
        public RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
            throws IOException
        {
            return mSource.newRowScannerThisTableReverse(txn, filter, args);
        }

        @Override
        public RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
            throws IOException
        {
            return mSource.newRowUpdaterThisTableReverse(txn, filter, args);
        }

        @Override
        public QueryPlan queryPlan(Transaction txn, String filter, Object... args)
            throws IOException
        {
            return mSource.queryPlanThisTableReverse(txn, filter, args);
        }
    }
}
