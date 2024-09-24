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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

/**
 * Commits every transactional update, and exits the scope when closed. For any entry stepped
 * over, acquired locks are released. The linked transaction is automatically exited when the
 * updater is closed.
 *
 * @author Brian S O'Neill
 */
final class AutoCommitUpdater<R> extends BasicUpdater<R> {
    LockResult mLockResult;

    AutoCommitUpdater(StoredTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        LockResult result = c.first();
        c.register();
        return mLockResult = result;
    }

    @Override
    protected LockResult toNext(Cursor c) throws IOException {
        LockResult result = mLockResult;
        if (result != null && result.isAcquired()) {
            c.link().unlock();
        }
        return mLockResult = c.next();
    }

    @Override
    protected void unlocked() {
        mLockResult = null;
    }

    @Override
    protected void finished() throws IOException {
        mRow = null;
        if (mLockResult != null) {
            mLockResult = null;
            mCursor.link().exit();
        }
    }

    @Override
    protected void storeValue(Cursor c, byte[] value) throws IOException {
        c.commit(value);
        mLockResult = null;
    }

    @Override
    protected void storeValue(Trigger<R> trigger, R row, Cursor c, byte[] value)
        throws IOException
    {
        Transaction txn = c.link();
        byte[] oldValue = c.value();
        if (oldValue == null) {
            trigger.insertP(txn, row, c.key(), value);
        } else {
            trigger.storeP(txn, row, c.key(), oldValue, value);
        }
        c.commit(value);
        mLockResult = null;
    }

    @Override
    protected void postStoreKeyValue(Transaction txn) throws IOException {
        txn.commit();
        mLockResult = null;
    }

    @Override
    protected void doDelete() throws IOException {
        mCursor.commit(null);
    }

    @Override
    protected void doDelete(Trigger<R> trigger, R row) throws IOException {
        Cursor c = mCursor;
        byte[] oldValue = c.value();
        if (oldValue != null) {
            // Don't pass the row in case the key columns were modified.
            trigger.delete(c.link(), c.key(), oldValue);
            c.commit(null);
        }
    }
}
