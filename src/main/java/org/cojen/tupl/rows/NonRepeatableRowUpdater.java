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
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicateLock;

/**
 * Updater which releases acquired locks for rows which are stepped over.
 *
 * @author Brian S O'Neill
 */
class NonRepeatableRowUpdater<R> extends BasicRowUpdater<R> {
    /**
     * Constructs a NonRepeatableRowUpdater which releases the predicate lock when the updater
     * finishes. The lock can also be released when the transaction finishes.
     */
    static <R> NonRepeatableRowUpdater<R> locked
        (AbstractTable<R> table, ScanController<R> controller, RowPredicateLock.Closer closer)
    {
        return new NonRepeatableRowUpdater<R>(table, controller) {
            @Override
            protected void finished() throws IOException {
                super.finished();
                closer.close();
            }
        };
    }

    LockResult mLockResult;

    NonRepeatableRowUpdater(AbstractTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        Transaction txn = c.link();
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            return mLockResult = super.toFirst(c);
        } finally {
            txn.lockMode(original);
        }
    }

    @Override
    protected LockResult toNext(Cursor c) throws IOException {
        Transaction txn = c.link();
        LockResult result = mLockResult;
        if (result != null && result.isAcquired()) {
            // If the transaction is being acted upon independently of this updater,
            // then this technique might throw an IllegalStateException.
            txn.unlock();
        }
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            return mLockResult = c.next();
        } finally {
            txn.lockMode(original);
        }
    }

    @Override
    protected void unlocked() {
        mLockResult = null;
    }

    @Override
    protected void finished() throws IOException {
        mRow = null;
        mLockResult = null;
    }
}
