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

import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

/**
 * Used by {@link ReplWriter} to queue up transactions which commit asynchronously.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxnWaiter extends Thread {
    private static final int TIMEOUT_MILLIS = 60000;

    static final int PENDING = 1, DO_COMMIT = 2, DO_ROLLBACK = 3, EXITED = 4;

    private final ReplWriter mWriter;

    private PendingTxn mBehind;
    private PendingTxn mAhead;

    private long mFlipPos;
    private boolean mExited;

    PendingTxnWaiter(ReplWriter writer) {
        mFlipPos = -1;
        mWriter = writer;
    }

    /**
     * @return PENDING, DO_COMMIT, DO_ROLLBACK, or EXITED if thread exited (was idle) and a new
     * instance is needed
     */
    synchronized int add(PendingTxn pending) {
        long flipPos = mFlipPos;
        if (flipPos >= 0) {
            return pending.mCommitPos <= flipPos ? DO_COMMIT : DO_ROLLBACK;
        }

        if (mExited) {
            return EXITED;
        }

        PendingTxn behind = mBehind;
        if (behind == null) {
            mBehind = pending;
            notify();
        } else {
            long commitPos = pending.mCommitPos;
            if (commitPos <= behind.mCommitPos) {
                pending.mPrev = behind.mPrev;
                behind.mPrev = pending;
            } else {
                PendingTxn ahead = mAhead;
                if (ahead != null && commitPos <= ahead.mCommitPos) {
                    pending.mPrev = ahead.mPrev;
                    ahead.mPrev = pending;
                } else {
                    pending.mPrev = ahead;
                    mAhead = pending;
                }
            }
        }

        return PENDING;
    }

    /**
     * Called after leadership is lost. When calling add, the return value will be DO_COMMIT or
     * DO_ROLLBACK. The given commit position determines which action should be applied.
     *
     * @throws IllegalArgumentException if commit position is negative
     */
    synchronized void flipped(long commitPos) {
        if (commitPos < 0) {
            throw new IllegalArgumentException();
        }
        mFlipPos = commitPos;
    }

    /**
     * Explicitly commit or rollback all pending transactions.
     *
     * @throws IllegalStateException if not flipped
     */
    void finishAll() {
        long commitPos;
        PendingTxn behind, ahead;
        synchronized (this) {
            commitPos = mFlipPos;
            if (commitPos < 0) {
                throw new IllegalStateException();
            }
            behind = mBehind;
            mBehind = null;
            ahead = mAhead;
            mAhead = null;
            notify();
        }
        finishAll(behind, commitPos);
        finishAll(ahead, commitPos);
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Throwable e) {
            synchronized (this) {
                mExited = true;
            }
            throw e;
        }
    }

    private void doRun() {
        while (true) {
            PendingTxn behind;
            synchronized (this) {
                if ((behind = mBehind) == null) {
                    try {
                        wait(TIMEOUT_MILLIS);
                    } catch (InterruptedException e) {
                    }
                    if ((behind = mBehind) == null) {
                        mExited = true;
                        return;
                    }
                }
            }

            if (!mWriter.confirm(behind)) {
                // Don't set the exited flag, allowing pending transactions to accumulate until
                // the flipped method is called.
                return;
            }

            synchronized (this) {
                if (mBehind == null) {
                    mExited = true;
                    return;
                }
                mBehind = mAhead;
                mAhead = null;
            }

            // Commit all the confirmed transactions.

            do {
                try {
                    behind.commit();
                } catch (Exception e) {
                    uncaught(behind, e);
                }
            } while ((behind = behind.mPrev) != null);
        }
    }

    private static void finishAll(PendingTxn pending, long commitPos) {
        while (pending != null) {
            try {
                if (pending.mCommitPos <= commitPos) {
                    pending.commit();
                } else {
                    pending.rollback();
                }
            } catch (Exception e) {
                uncaught(pending, e);
            }
            pending = pending.mPrev;
        }
    }

    private static void uncaught(PendingTxn pending, Throwable e) {
        LocalDatabase db = pending.getDatabase();
        if (db == null || db.isClosed()) {
            return;
        }
        EventListener listener = db.eventListener();
        if (listener != null) {
            listener.notify(EventType.REPLICATION_PANIC,
                            "Unexpected transaction exception: %1$s", e);
        } else {
            Utils.uncaught(e);
        }
    }
}
