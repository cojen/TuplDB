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
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

/**
 * Wraps a Scanner and applies updates directly against a table.
 *
 * @author Brian S O'Neill
 */
class WrappedUpdater<R> implements Updater<R> {
    protected final Table<R> mTable;
    protected final Transaction mTxn;
    protected final Scanner<R> mScanner;

    WrappedUpdater(Table<R> table, Transaction txn, Scanner<R> scanner) {
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
        return mScanner.step();
    }

    @Override
    public R step(R row) throws IOException {
        return mScanner.step(row);
    }

    @Override
    public final R update() throws IOException {
        try {
            mTable.update(mTxn, current());
        } catch (Throwable e) {
            close(e);
            throw e;
        }
        return step();
    }

    @Override
    public final R update(R row) throws IOException {
        try {
            mTable.update(mTxn, current());
        } catch (Throwable e) {
            close(e);
            throw e;
        }
        return step(row);
    }

    @Override
    public final R delete() throws IOException {
        try {
            mTable.delete(mTxn, current());
        } catch (Throwable e) {
            close(e);
            throw e;
        }
        return step();
    }

    @Override
    public final R delete(R row) throws IOException {
        try {
            mTable.delete(mTxn, current());
        } catch (Throwable e) {
            close(e);
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

    protected void close(Throwable e) {
        try {
            mScanner.close();
        } catch (Throwable e2) {
            RowUtils.suppress(e, e2);
        }
    }

    private R current() {
        R current = row();
        if (current == null) {
            throw new IllegalStateException();
        }
        return current;
    }

    /**
     * Implements a WrappedUpdater which commits the transaction when the updater finishes,
     * throws an exception, or is explicitly closed.
     */
    static final class EndCommit<R> extends WrappedUpdater<R> {
        EndCommit(Table<R> table, Transaction txn, Scanner<R> scanner) {
            super(table, txn, scanner);
        }

        @Override
        public R step() throws IOException {
            R row;
            try {
                row = mScanner.step();
            } catch (Throwable e) {
                commit(e);
                throw e;
            }
            if (row == null) {
                mTxn.commit();
            }
            return row;
        }

        @Override
        public R step(R row) throws IOException {
            try {
                row = mScanner.step(row);
            } catch (Throwable e) {
                commit(e);
                throw e;
            }
            if (row == null) {
                mTxn.commit();
            }
            return row;
        }

        @Override
        public void close() throws IOException {
            try {
                mScanner.close();
            } catch (Throwable e) {
                commit(e);
                throw e;
            }

            mTxn.commit();
        }

        @Override
        protected void close(Throwable e) {
            super.close(e);
            commit(e);
        }

        private void commit(Throwable e) {
            try {
                mTxn.commit();
            } catch (Throwable e2) {
                RowUtils.suppress(e, e2);
            }
        }
    }
}
