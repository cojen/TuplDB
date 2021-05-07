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
import org.cojen.tupl.Transaction;

/**
 * Updater which releases acquired locks for rows which are stepped over.
 *
 * @author Brian S O'Neill
 */
abstract class NonRepeatableRowUpdater<R> extends AbstractRowUpdater<R> {
    protected LockResult mLockResult;

    /**
     * @param cursor linked transaction must not be null; is exited when finished
     */
    NonRepeatableRowUpdater(AbstractRowView view, Cursor cursor) {
        super(view, cursor);
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
    protected void finished() throws IOException {
        if (mLockResult != null) {
            mLockResult = null;
            Transaction txn = mCursor.link();
            txn.commit();
            txn.exit();
        }
    }
}
