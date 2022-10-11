/*
 *  Copyright (C) 2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.views;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;
import org.cojen.tupl.EntryUpdater;

import org.cojen.tupl.core.Utils;

/**
 * Commits every transactional update, and exits the scope when closed. For any entry stepped
 * over, acquired locks are released. The linked transaction is automatically exited when the
 * updater is closed.
 *
 * @author Brian S O'Neill
 */
public final class CursorAutoCommitUpdater extends CursorScanner implements EntryUpdater {
    private LockResult mLockResult;

    /**
     * @param cursor unpositioned cursor
     */
    public CursorAutoCommitUpdater(Cursor cursor) throws IOException {
        super(cursor);
        mLockResult = cursor.first();
        cursor.register();
    }

    @Override
    public boolean step() throws IOException {
        LockResult result = mLockResult;
        if (result == null) {
            return false;
        }

        Cursor c = mCursor;
        Transaction txn = c.link();

        tryStep: {
            try {
                if (result.isAcquired()) {
                    txn.unlock();
                }
                result = c.next();
            } catch (UnpositionedCursorException e) {
                break tryStep;
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
            if (c.key() != null) {
                mLockResult = result;
                return true;
            }
        }

        mLockResult = null;
        txn.exit();

        return false;
    }

    @Override
    public boolean update(byte[] value) throws IOException {
        Cursor c = mCursor;

        try {
            c.commit(value);
        } catch (UnpositionedCursorException e) {
            close();
            return false;
        } catch (Throwable e) {
            throw Utils.fail(this, e);
        }

        tryStep: {
            LockResult result;
            try {
                result = c.next();
            } catch (UnpositionedCursorException e) {
                break tryStep;
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
            if (c.key() != null) {
                mLockResult = result;
                return true;
            }
        }

        mLockResult = null;
        c.link().exit();

        return false;
    }

    @Override
    public void close() throws IOException {
        mCursor.reset();
        if (mLockResult != null) {
            mLockResult = null;
            mCursor.link().exit();
        }
    }
}
