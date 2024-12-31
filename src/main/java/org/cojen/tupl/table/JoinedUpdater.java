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

import java.util.Arrays;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

/**
 * Expected to be used only when updating via a secondary index.
 *
 * @author Brian S O'Neill
 */
final class JoinedUpdater<R> extends BasicScanner<R> implements Updater<R> {
    private final BasicUpdater<R> mPrimaryUpdater;

    private final TriggerIndexAccessor mAccessor;

    private Cursor mPrimaryCursor;

    JoinedUpdater(StoredTableIndex<R> table, ScanController<R> controller,
                  BasicUpdater<R> primaryUpdater)
    {
        super(table, controller);
        mPrimaryUpdater = primaryUpdater;

        // TriggerIndexAccessor could be an interface, and then JoinedUpdater could simply
        // implement it. This can be a problem if someone decided to attach the Updater to a
        // transaction, and so the composition approach is safer.
        mAccessor = new TriggerIndexAccessor() {
            @Override
            public void stored(Index ix, byte[] key, byte[] value) {
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
        // At this point, mPrimaryUpdater and mPrimaryCursor refer to the primary index, but
        // the cursor is unpositioned. The mCursor in this class refers to the secondary index,
        // and it's at the first row.
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
        // By passing mPrimaryCursor to the evalRow method, it gets positioned to the primary
        // index row as a side effect.
        return mEvaluator.evalRow(c, result, row, mPrimaryCursor);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        // This method affects locking and registration behavior, but it doesn't care what the
        // cursor refers to. In this case, the cursor refers to the secondary index.
        return mPrimaryUpdater.toFirst(c);
    }

    @Override
    protected LockResult toNext(Cursor c) throws IOException {
        // This method affects locking behavior, but it doesn't care what the cursor refers to.
        // In this case, the cursor refers to the secondary index.
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
    public R update(R row) throws IOException {
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
    public R delete(R row) throws IOException {
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

    private void triggerStored(Index ix, byte[] key, byte[] value) {
        if (mTable.mSource == ix) {
            if (mController.predicate().testP(mRow, key, value)) {
                // The secondary key changed, and it's still in bounds.
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
