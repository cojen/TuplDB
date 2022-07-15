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

import java.util.Arrays;
import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;

/**
 * Expected to be used only when updating via a secondary index.
 *
 * @author Brian S O'Neill
 */
final class JoinedRowUpdater<R> extends BasicRowScanner<R> implements RowUpdater<R> {
    private final BasicRowUpdater<R> mPrimaryUpdater;

    private final TriggerIndexAccessor mAccessor;

    private Cursor mPrimaryCursor;

    JoinedRowUpdater(BaseTableIndex<R> table, ScanController<R> controller,
                     BasicRowUpdater<R> primaryUpdater)
    {
        super(table, controller);
        mPrimaryUpdater = primaryUpdater;

        // Although TriggerIndexAccessor could be an interface, and then JoinedRowUpdater could
        // simply implement it. This can be a problem if someone decided to attach the
        // RowUpdater to a transaction. This composition approach is safer.
        mAccessor = new TriggerIndexAccessor() {
            @Override
            public void stored(Index ix, byte[] key, byte[] value) throws IOException {
                triggerStored(ix, key, value);
            }

            @Override
            public boolean delete(Index ix, byte[] key) throws IOException {
                return triggerDelete(ix, key);
            }
        };
    }

    @Override
    void init(Transaction txn, R row) throws IOException {
        mPrimaryUpdater.mCursor = mPrimaryCursor = mPrimaryUpdater.mTable.mSource.newCursor(txn);
        super.init(txn, row);
    }

    @Override
    protected void beginBatch(R row, RowEvaluator<R> evaluator) throws IOException {
        super.beginBatch(row, evaluator);
        mPrimaryUpdater.mEvaluator = evaluator;
    }

    @Override
    protected R evalRow(Cursor c, LockResult result, R row) throws IOException {
        if (mPrimaryUpdater.mKeysToSkip != null && mPrimaryUpdater.mKeysToSkip.remove(c.key())) {
            return null;
        }
        return mEvaluator.evalRow(c, result, row, mPrimaryCursor);
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
        Transaction txn = txn();
        Object old = attachAccessor(txn);
        try {
            mPrimaryUpdater.mRow = mRow;
            mPrimaryUpdater.joinedUpdateCurrent();
        } finally {
            txn.attach(old);
        }
    }

    @Override
    public final R delete() throws IOException {
        deleteCurrent();
        return doStep(null);
    }

    @Override
    public final R delete(R row) throws IOException {
        Objects.requireNonNull(row);
        deleteCurrent();
        return doStep(row);
    }

    private void deleteCurrent() throws IOException {
        Transaction txn = txn();
        Object old = attachAccessor(txn);
        try {
            mPrimaryUpdater.mRow = mRow;
            mPrimaryUpdater.deleteCurrent();
        } finally {
            txn.attach(old);
        }
    }

    private Transaction txn() {
        return mCursor.link();
    }

    private Object attachAccessor(Transaction txn) {
        Object old = txn.attachment();
        txn.attach(mAccessor);
        return old;
    }

    private void triggerStored(Index ix, byte[] key, byte[] value) throws IOException {
        if (mTable.mSource == ix) {
            if (mController.predicate().testP(mRow, key, value)) {
                // The secondary key changed and it's still in bounds.
                if (mCursor.compareKeyTo(key) < 0) {
                    // The new key is higher, and so it must be added to the remembered set.
                    mPrimaryUpdater.addKeyToSkip(key);
                }
            }
        }
    }

    private boolean triggerDelete(Index ix, byte[] key) throws IOException {
        if (mTable.mSource == ix) {
            // Try to use the existing cursor to avoid an extra search step.
            Cursor c = mCursor;
            if (Arrays.equals(c.key(), key)) {
                c.delete();
                return true;
            }
        }
        return false;
    }
}
