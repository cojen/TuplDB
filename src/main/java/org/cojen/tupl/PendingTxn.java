/*
 *  Copyright 2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

    TransactionContext mTxnContext;
    long mTxnId;
    long mCommitPos;
    UndoLog mUndoLog;
    boolean mHasFragmentedTrash;
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
            mTxnContext.unregister(undo);
        }

        if (mHasFragmentedTrash) {
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
            mTxnContext.unregister(undo);
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
