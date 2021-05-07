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
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class AbstractRowUpdater<R> extends AbstractRowScanner<R> implements RowUpdater<R> {
    protected final AbstractRowView mView;

    /**
     * @param cursor linked transaction must not be null
     */
    AbstractRowUpdater(AbstractRowView view, Cursor cursor) {
        super(cursor);
        mView = view;
    }

    @Override
    public R update() throws IOException {
        try {
            doUpdate();
        } catch (UnpositionedCursorException e) {
            finished();
            return null;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        return step();
    }

    @Override
    public R update(R row) throws IOException {
        try {
            doUpdate();
        } catch (UnpositionedCursorException e) {
            finished();
            return null;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        return step(row);
    }

    @Override
    public R delete() throws IOException {
        try {
            mCursor.delete();
        } catch (UnpositionedCursorException e) {
            finished();
            return null;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        return step();
    }

    @Override
    public R delete(R row) throws IOException {
        try {
            mCursor.delete();
        } catch (UnpositionedCursorException e) {
            finished();
            return null;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        return step(row);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        LockResult result = c.first();
        c.register();
        return result;
    }

    /**
     * @return null if the key columns didn't change
     */
    protected abstract byte[] encodeKey();

    /**
     * @return non-null value
     */
    protected abstract byte[] encodeValue();

    protected void doUpdate() throws IOException {
        byte[] key = encodeKey();
        byte[] value = encodeValue();
        Cursor c = mCursor;
        if (key == null) {
            // Key didn't change.
            c.store(value);
        } else {
            Transaction txn = c.link();
            txn.enter();
            try {
                mView.mSource.store(txn, key, value);
                c.commit(null);
            } finally {
                txn.exit();
            }
        }
    }
}
