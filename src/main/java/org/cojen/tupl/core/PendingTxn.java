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

package org.cojen.tupl.core;

import java.io.IOException;

/**
 * References an UndoLog and a set of exclusive locks from a transaction ready to be committed.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxn extends Locker {
    TransactionContext mContext;
    long mTxnId;
    long mCommitPos;
    UndoLog mUndoLog;
    int mHasState;
    private Object mAttachment;

    PendingTxn mPrev;

    PendingTxn(LockManager manager, int hash) {
        super(manager, hash);
    }

    @Override
    public final LocalDatabase getDatabase() {
        UndoLog undo = mUndoLog;
        return undo == null ? super.getDatabase() : undo.getDatabase();
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
     * Releases all the locks and then discards the undo log. This object must be discarded
     * afterwards.
     */
    void commit() throws IOException {
        // See Transaction.commit for more info.

        scopeUnlockAll();

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.truncate();
            mContext.unregister(undo);
        }

        if ((mHasState & LocalTransaction.HAS_TRASH) != 0) {
            LocalDatabase db = getDatabase();
            FragmentedTrash.emptyTrash(db.fragmentedTrash(), mTxnId);
        }
    }

    /**
     * Applies the undo log, releases all the locks, and then discards the undo log. This
     * object must be discarded afterwards.
     */
    void rollback() throws IOException {
        // See Transaction.exit for more info.

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.rollback();
        }

        scopeUnlockAll();

        if (undo != null) {
            mContext.unregister(undo);
        }
    }
}
