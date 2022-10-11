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
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;
import org.cojen.tupl.EntryUpdater;

import org.cojen.tupl.core.Utils;

/**
 * EntryUpdater which releases acquired locks for entries which are stepped over.
 *
 * @author Brian S O'Neill
 */
public final class CursorNonRepeatableUpdater extends CursorScanner implements EntryUpdater {
    private LockResult mLockResult;

    /**
     * @param cursor unpositioned cursor
     */
    public CursorNonRepeatableUpdater(Cursor cursor) throws IOException {
        super(cursor);
        Transaction txn = cursor.link();
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            mLockResult = cursor.first();
            cursor.register();
        } finally {
            txn.lockMode(original);
        }
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
                    // If the transaction is being acted upon independently of this updater,
                    // then this technique might throw an IllegalStateException.
                    txn.unlock();
                }
                LockMode original = txn.lockMode();
                txn.lockMode(LockMode.UPGRADABLE_READ);
                try {
                    result = c.next();
                } finally {
                    txn.lockMode(original);
                }
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

        return false;
    }

    @Override
    public boolean update(byte[] value) throws IOException {
        Cursor c = mCursor;

        try {
            c.store(value);
        } catch (UnpositionedCursorException e) {
            close();
            return false;
        } catch (Throwable e) {
            throw Utils.fail(this, e);
        }

        tryStep: {
            LockResult result;
            try {
                Transaction txn = c.link();
                LockMode original = txn.lockMode();
                txn.lockMode(LockMode.UPGRADABLE_READ);
                try {
                    result = c.next();
                } finally {
                    txn.lockMode(original);
                }
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

        return false;
    }

    @Override
    public void close() throws IOException {
        mCursor.reset();
        mLockResult = null;
    }
}
