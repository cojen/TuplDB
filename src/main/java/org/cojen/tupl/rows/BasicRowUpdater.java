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

import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;
import org.cojen.tupl.View;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class BasicRowUpdater<R> extends BasicRowScanner<R> implements RowUpdater<R> {
    final View mView;

    /**
     * @param cursor linked transaction must not be null
     */
    BasicRowUpdater(View view, Cursor cursor, RowDecoderEncoder<R> decoder) {
        super(cursor, decoder);
        mView = view;
    }

    @Override
    public final R update() throws IOException {
        return doUpdateAndStep(null);
    }

    @Override
    public final R update(R row) throws IOException {
        Objects.requireNonNull(row);
        return doUpdateAndStep(row);
    }

    private R doUpdateAndStep(R row) throws IOException {
        try {
            R current = mRow;
            if (current == null) {
                throw new IllegalStateException("No current row");
            }
            doUpdate(current);
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        unlocked(); // prevent subclass from attempting to release the lock
        return doStep(row);
    }

    @Override
    public final R delete() throws IOException {
        return doDeleteAndStep(null);
    }

    @Override
    public final R delete(R row) throws IOException {
        Objects.requireNonNull(row);
        return doDeleteAndStep(row);
    }

    private R doDeleteAndStep(R row) throws IOException {
        try {
            mCursor.delete();
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        unlocked(); // prevent subclass from attempting to release the lock
        return doStep(row);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        LockResult result = c.first();
        c.register();
        return result;
    }

    protected void doUpdate(R row) throws IOException {
        RowDecoderEncoder<R> encoder = mDecoder;
        byte[] key = encoder.encodeKey(row);
        byte[] value = encoder.encodeValue(row);
        Cursor c = mCursor;
        if (key == null) {
            // Key didn't change.
            c.store(value);
        } else {
            Transaction txn = c.link();
            txn.enter();
            try {
                mView.store(txn, key, value);
                c.commit(null);
            } finally {
                txn.exit();
            }
        }
    }
}
