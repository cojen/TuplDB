/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;

/**
 * Undo log and a set of exclusive locks from a transaction ready to be committed.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxn extends LockOwner {
    private final Lock mFirst;
    private Lock[] mRest;
    private int mRestSize;

    TransactionContext mContext;
    long mTxnId;
    long mCommitPos;
    UndoLog mUndoLog;
    int mHasState;
    private Object mAttachment;

    PendingTxn mPrev;

    PendingTxn(Lock first) {
        mFirst = first;
    }

    @Override
    public final LocalDatabase getDatabase() {
        return mUndoLog.getDatabase();
    }

    @Override
    public void attach(Object obj) {
        mAttachment = obj;
    }

    @Override
    public Object attachment() {
        return mAttachment;
    }

    /**
     * Add an exclusive lock into the set, retaining FIFO (queue) order.
     */
    void add(Lock lock) {
        Lock first = mFirst;
        if (first == null) {
            throw new IllegalStateException("cannot add lock");
        }
        Lock[] rest = mRest;
        if (rest == null) {
            rest = new Lock[8];
            mRest = rest;
            mRestSize = 1;
            rest[0] = lock;
        } else {
            int size = mRestSize;
            if (size >= rest.length) {
                Lock[] newRest = new Lock[rest.length << 1];
                System.arraycopy(rest, 0, newRest, 0, rest.length);
                mRest = rest = newRest;
            }
            rest[size] = lock;
            mRestSize = size + 1;
        }
    }

    /**
     * Releases all the locks and then discards the undo log. This object must be discarded
     * afterwards.
     */
    void commit(LocalDatabase db) throws IOException {
        // See Transaction.commit for more info.

        unlockAll(db);

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.truncate(true);
            mContext.unregister(undo);
        }

        if ((mHasState & LocalTransaction.HAS_TRASH) != 0) {
            db.fragmentedTrash().emptyTrash(mTxnId);
        }
    }

    /**
     * Applies the undo log, releases all the locks, and then discards the undo log. This
     * object must be discarded afterwards.
     */
    void rollback(LocalDatabase db) throws IOException {
        // See Transaction.exit for more info.

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.rollback();
        }

        unlockAll(db);

        if (undo != null) {
            mContext.unregister(undo);
        }
    }

    private void unlockAll(LocalDatabase db) {
        Lock first = mFirst;
        if (first != null) {
            LockManager manager = db.mLockManager;
            manager.unlock(this, first);
            Lock[] rest = mRest;
            if (rest != null) {
                for (Lock lock : rest) {
                    if (lock == null) {
                        return;
                    }
                    manager.unlock(this, lock);
                }
            }
        }
    }
}
