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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;

import org.cojen.tupl.views.ViewUtils;

/**
 * Expected to be used only when updating via a secondary index.
 *
 * @author Brian S O'Neill
 */
class JoinedRowUpdater<R> extends BasicRowScanner<R> implements RowUpdater<R> {
    protected final BasicRowUpdater<R> mPrimaryUpdater;

    private Cursor mPrimaryCursor;

    JoinedRowUpdater(AbstractTable<R> table, ScanController<R> controller,
                     BasicRowUpdater<R> primaryUpdater)
    {
        super(table, controller);
        mPrimaryUpdater = primaryUpdater;
    }

    @Override
    void init(Transaction txn) throws IOException {
        mPrimaryUpdater.mCursor = mPrimaryCursor = mPrimaryUpdater.mTable.mSource.newCursor(txn);
        super.init(txn);
    }

    @Override
    protected void setDecoder(RowDecoderEncoder<R> decoder) {
        super.setDecoder(decoder);
        mPrimaryUpdater.mDecoder = decoder;
    }

    @Override
    protected R decodeRow(Cursor c, LockResult result, R row) throws IOException {
        if (mPrimaryUpdater.mKeysToSkip != null && mPrimaryUpdater.mKeysToSkip.remove(c.key())) {
            return null;
        }
        return mDecoder.decodeRow(c, result, row, mPrimaryCursor);
    }

    protected LockResult toFirst(Cursor c) throws IOException {
        return mPrimaryUpdater.toFirst(c);
    }

    protected LockResult toNext(Cursor c) throws IOException {
        return mPrimaryUpdater.toNext(c);
    }

    @Override
    protected void unlocked() {
        mPrimaryUpdater.unlocked();
    }

    @Override
    protected void finished() throws IOException {
        super.finished();
        mPrimaryUpdater.close();
    }

    @Override
    public final R update() throws IOException {
        updateCurrent();
        return doStep(null);
    }

    @Override
    public final R update(R row) throws IOException {
        Objects.requireNonNull(row);
        updateCurrent();
        return doStep(row);
    }

    private void updateCurrent() throws IOException {
        final byte[] originalKey = mCursor.key();

        mPrimaryUpdater.mRow = mRow;
        mPrimaryUpdater.joinedUpdateCurrent();

        if (!mCursor.exists() && mController.predicate().test(mRow)) {
            // The secondary key changed and it's still in bounds.
            byte[] newKey = mTable.toKey(mRow);
            if (mCursor.comparator().compare(originalKey, newKey) < 0) {
                // The new key is higher, and so it must be added to the remembered set.
                mPrimaryUpdater.addKeyToSkip(newKey);
            }
        }
    }

    @Override
    public final R delete() throws IOException {
        mPrimaryUpdater.mRow = mRow;
        mPrimaryUpdater.deleteCurrent();
        return doStep(null);
    }

    @Override
    public final R delete(R row) throws IOException {
        Objects.requireNonNull(row);
        mPrimaryUpdater.mRow = mRow;
        mPrimaryUpdater.deleteCurrent();
        return doStep(row);
    }
}
