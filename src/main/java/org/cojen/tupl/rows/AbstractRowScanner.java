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

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class AbstractRowScanner<R> implements RowScanner<R> {
    protected final Cursor mCursor;

    protected AbstractRowScanner(Cursor cursor) {
        mCursor = cursor;
    }

    /**
     * Must be called by subclass constructor.
     */
    protected void init() throws IOException {
        // FIXME: filtering must release locks; subclass does that?
        Cursor c = mCursor;
        c.first();
        while (true) {
            byte[] key = c.key();
            if (key == null) {
                return;
            }
            try {
                if (decodeRow(key, c.value()) != null) {
                    return;
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
            c.next();
        }
    }

    @Override
    public R step() throws IOException {
        // FIXME: filtering must release locks; subclass does that?
        Cursor c = mCursor;
        while (true) {
            c.next();
            byte[] key = c.key();
            if (key == null) {
                clearRow();
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
    public R step(R row) throws IOException {
        // FIXME: filtering must release locks; subclass does that?
        Cursor c = mCursor;
        while (true) {
            c.next();
            byte[] key = c.key();
            if (key == null) {
                clearRow();
                return null;
            }
            try {
                if (decodeRow(row, key, c.value())) {
                    return row;
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
        mCursor.reset();
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
