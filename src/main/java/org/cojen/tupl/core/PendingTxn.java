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

import java.util.concurrent.RecursiveAction;

import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

import org.cojen.tupl.repl.CommitCallback;

/**
 * References an UndoLog and a set of exclusive locks from a transaction ready to be committed.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxn extends Locker implements CommitCallback {
    final Thread mThread;
    final long mCommitPos;
    final TransactionContext mContext;
    final long mTxnId;
    final UndoLog mUndoLog;
    final int mHasState;
    private Object mAttachment;

    PendingTxn(LocalTransaction from, long commitPos, UndoLog undo, int hasState) {
        super(from.mManager, from.mHash);
        mThread = Thread.currentThread();
        mCommitPos = commitPos;
        mContext = from.mContext;
        mTxnId = from.mTxnId;
        mUndoLog = undo;
        mHasState = hasState;
        mAttachment = from.attachment();
    }

    @Override
    public final LocalDatabase getDatabase() {
        UndoLog undo = mUndoLog;
        return undo == null ? super.getDatabase() : undo.getDatabase();
    }

    @Override
    public Object attachment() {
        return mAttachment;
    }

    @Override
    public long position() {
        return mCommitPos;
    }

    @Override
    public void reached(long position) {
        // See Transaction.commit and Transaction.exit for more info regarding commit and
        // rollback logic.

        if (Thread.currentThread() == mThread) {
            // Allow blocking operations in the user thread.
            finish(position);
        } else {
            // Don't block the threads responsible for replication.
            LocalDatabase db = getDatabase();
            if (db != null) {
                db.execute(new RecursiveAction() {
                    @Override
                    protected void compute() {
                        finish(position);
                    }
                });
            }
        }
    }

    /**
     * @param position negative to rollback, else commit
     */
    void finish(long position) {
        try {
            UndoLog undo = mUndoLog;

            if (position < 0) {
                if (undo != null) {
                    undo.rollback();
                }
                scopeUnlockAll();
                if (undo != null) {
                    mContext.unregister(undo);
                }
            } else {
                scopeUnlockAll();
                if (undo != null) {
                    undo.truncate();
                    mContext.unregister(undo);
                }
                if ((mHasState & LocalTransaction.HAS_TRASH) != 0) {
                    FragmentedTrash.emptyTrash(getDatabase().fragmentedTrash(), mTxnId);
                }
            }
        } catch (Throwable e) {
            LocalDatabase db = getDatabase();
            if (db != null && !db.isClosed()) {
                EventListener listener = db.eventListener();
                if (listener != null) {
                    listener.notify(EventType.REPLICATION_PANIC,
                                    "Unexpected transaction exception: %1$s", e);
                } else {
                    Utils.uncaught(e);
                }
            }
        }
    }
}
