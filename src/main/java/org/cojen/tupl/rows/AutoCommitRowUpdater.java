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
import org.cojen.tupl.View;

/**
 * Commits every transactional update, and exits the scope when closed. For any entry stepped
 * over, acquired locks are released.
 *
 * @author Brian S O'Neill
 */
class AutoCommitRowUpdater<R> extends NonRepeatableRowUpdater<R> {
    /**
     * @param cursor linked transaction must not be null
     */
    AutoCommitRowUpdater(View view, Cursor cursor, RowDecoderEncoder<R> decoder) {
        super(view, cursor, decoder);
    }

    @Override
    protected void doUpdate(R row) throws IOException {
        RowDecoderEncoder<R> encoder = mDecoder;
        byte[] key = encoder.encodeKey(row);
        byte[] value = encoder.encodeValue(row);
        Cursor c = mCursor;
        if (key == null) {
            // Key didn't change.
            c.commit(value);
        } else {
            Transaction txn = c.link();
            txn.enter();
            try {
                mView.store(txn, key, value);
                c.commit(null);
            } finally {
                txn.exit();
            }
            txn.commit();
        }
        mLockResult = null;
    }

    @Override
    protected void finished() throws IOException {
        if (mLockResult != null) {
            mLockResult = null;
            mCursor.link().exit();
        }
    }
}
