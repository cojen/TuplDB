/*
 *  Copyright (C) 2021 Cojen.org
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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class AbstractRowScanner<R> implements RowScanner<R> {
    protected final View mView;
    protected final Transaction mTxn;

    private Cursor mCursor;

    protected AbstractRowScanner(View view, Transaction txn) {
        mView = view;
        mTxn = txn;
    }

    @Override
    public R step() throws IOException {
        Cursor c = mCursor;
        if (c == null) {
            mCursor = c = initCursor();
        }
        while (true) {
            c.next();
            byte[] key = c.key();
            if (key == null) {
                clearRow();
                c.reset();
                return null;
            }
            try {
                R row = decodeRow(key, c.value());
                if (row != null) {
                    return row;
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
        }
    }

    @Override
    public boolean step(R row) throws IOException {
        Cursor c = mCursor;
        if (c == null) {
            mCursor = c = initCursor();
        }
        while (true) {
            c.next();
            byte[] key = c.key();
            if (key == null) {
                clearRow();
                c.reset();
                return false;
            }
            try {
                if (decodeRow(row, key, c.value())) {
                    return true;
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
        }
    }

    @Override
    public void close() throws IOException {
        clearRow();
        Cursor c = mCursor;
        if (c != null) {
            c.reset();
        }
    }

    protected Cursor initCursor() throws IOException {
        Cursor c = mView.newCursor(mTxn);
        c.first();
        return c;
    }

    /**
     * @return null if row is filtered out
     */
    protected abstract R decodeRow(byte[] key, byte[] value) throws IOException;

    /**
     * @return false if row is filtered out
     */
    protected abstract boolean decodeRow(R row, byte[] key, byte[] value) throws IOException;

    protected abstract void clearRow();
}
