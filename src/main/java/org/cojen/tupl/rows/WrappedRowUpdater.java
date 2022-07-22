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

import java.util.Objects;

import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

/**
 * Wraps a RowScanner and applies updates directly against a table.
 *
 * @author Brian S O'Neill
 */
class WrappedRowUpdater<R> implements BaseRowScanner<R>, RowUpdater<R> {
    protected final Table<R> mTable;
    protected final Transaction mTxn;
    protected final RowScanner<R> mScanner;

    WrappedRowUpdater(Table<R> table, Transaction txn, RowScanner<R> scanner) {
        mTable = table;
        mTxn = txn;
        mScanner = scanner;
    }

    @Override
    public final R row() {
        return mScanner.row();
    }

    @Override
    public R step() throws IOException {
        try {
            return mScanner.step();
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
    }

    @Override
    public R step(R row) throws IOException {
        try {
            return mScanner.step(row);
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
    }

    @Override
    public final R update() throws IOException {
        try {
            mTable.update(mTxn, current());
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
        return step();
    }

    @Override
    public final R update(R row) throws IOException {
        Objects.requireNonNull(row);
        try {
            mTable.update(mTxn, current());
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
        return step(row);
    }

    @Override
    public final R delete() throws IOException {
        try {
            mTable.delete(mTxn, current());
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
        return step();
    }

    @Override
    public final R delete(R row) throws IOException {
        Objects.requireNonNull(row);
        try {
            mTable.delete(mTxn, current());
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
        return step(row);
    }

    @Override
    public final long estimateSize() {
        return mScanner.estimateSize();
    }

    @Override
    public final int characteristics() {
        return mScanner.characteristics();
    }

    @Override
    public void close() throws IOException {
        mScanner.close();
    }

    protected void exception(Throwable e) throws IOException {
    }

    private R current() {
        R current = row();
        if (current == null) {
            throw new IllegalStateException();
        }
        return current;
    }
}
