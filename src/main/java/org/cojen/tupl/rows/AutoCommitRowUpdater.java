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
import org.cojen.tupl.Transaction;

/**
 * Commits every transactional update, and exits the scope when closed. For any entry stepped
 * over, acquired locks are released.
 *
 * @author Brian S O'Neill
 */
class AutoCommitRowUpdater<R> extends NonRepeatableRowUpdater<R> {
    AutoCommitRowUpdater(AbstractTable<R> table, ScanController<R> controller) {
        super(table, controller);
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
        c.store(value);
        trigger.store(txn, row, c.key(), oldValue, value);
        txn.commit();
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
        Transaction txn = c.link();
        byte[] oldValue = c.value();
        mCursor.delete();
        trigger.store(txn, row, c.key(), oldValue, null);
        txn.commit();
    }

    @Override
    protected void finished() throws IOException {
        mRow = null;
        if (mLockResult != null) {
            mLockResult = null;
            mCursor.link().exit();
        }
    }
}
