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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.IOException;

import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

/**
 * References an UndoLog and a set of exclusive locks from a transaction ready to be committed.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxn extends Locker implements Runnable {
    final TransactionContext mContext;
    final long mTxnId;
    final UndoLog mUndoLog;
    final int mHasState;

    private Object mAttachment;

    long mCommitPos;

    volatile PendingTxn mNext;
    private static final VarHandle cNextHandle;

    static {
        try {
            cNextHandle =
                MethodHandles.lookup().findVarHandle
                (PendingTxn.class, "mNext", PendingTxn.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    PendingTxn(LocalTransaction from) {
        super(from.mManager, from.mHash);
        mContext = from.mContext;
        mTxnId = from.mTxnId;
        mUndoLog = from.mUndoLog;
        mHasState = from.mHasState;
        mAttachment = from.attachment();
    }

    /**
     * @return null if locked, else the next pending txn
     */
    PendingTxn tryLockLast() {
        return (PendingTxn) cNextHandle.compareAndExchange(this, null, this);
    }

    void unlockLast() {
        mNext = null;
    }

    void setNext(PendingTxn next) {
        while (!cNextHandle.compareAndSet(this, null, next)) {
            Thread.onSpinWait();
        }
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
    public void run() {
        // FIXME: If attachment implements a specific interface, then call it when transaction
        // finishes. Better idea: define a Database-level callback which receives the
        // transaction id, the attachment, and the status. Status of null means committed. This
        // design allows for null transactions to also receive notification. But why? What can
        // you do with the notification from a null transaction?
        try {
            if (mCommitPos < 0) {
                doRollback();
            } else {
                scopeUnlockAll();
                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.truncate();
                    mContext.unregister(undo);
                }
                if ((mHasState & LocalTransaction.HAS_TRASH) != 0) {
                    FragmentedTrash.emptyTrash(getDatabase().fragmentedTrash(), mTxnId);
                }
            }
        } catch (Throwable e) {
            uncaught(e);
        }
    }

    RuntimeException rollback(Throwable cause) {
        try {
            doRollback();
        } catch (Throwable e) {
            Utils.suppress(cause, e);
        }
        throw Utils.rethrow(cause);
    }

    private void doRollback() throws IOException {
        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.rollback();
        }
        scopeUnlockAll();
        if (undo != null) {
            mContext.unregister(undo);
        }
    }

    private void uncaught(Throwable cause) {
        LocalDatabase db = getDatabase();
        if (db != null && !db.isClosed()) {
            EventListener listener = db.eventListener();
            if (listener != null) {
                listener.notify(EventType.REPLICATION_PANIC,
                                "Unexpected transaction exception: %1$s", cause);
            } else {
                Utils.uncaught(cause);
            }
        }
    }
}
