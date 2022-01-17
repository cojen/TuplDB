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
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * Only supports one scan batch.
 *
 * @author Brian S O'Neill
 */
public abstract class SingleScanController<R> implements ScanController<R>, RowDecoderEncoder<R> {
    private final byte[] mLowBound, mHighBound;
    private final boolean mLowInclusive, mHighInclusive;

    protected SingleScanController(byte[] lowBound, boolean lowInclusive,
                                   byte[] highBound, boolean highInclusive)
    {
        mLowBound = lowBound;
        mLowInclusive = lowInclusive;
        mHighBound = highBound;
        mHighInclusive = highInclusive;
    }

    @Override
    public final boolean isSingleBatch() {
        return true;
    }

    @Override
    public final Cursor newCursor(View view, Transaction txn) throws IOException {
        applyBounds: {
            byte[] low = mLowBound;
            if (low != null) {
                if (low == EMPTY) {
                    view = view.viewLt(low);
                    break applyBounds;
                }
                view = mLowInclusive ? view.viewGe(low) : view.viewGt(low);
            }
            byte[] high = mHighBound;
            if (high != null) {
                view = mHighInclusive ? view.viewLe(high) : view.viewLt(high);
            }
        }

        return view.newCursor(txn);
    }

    @Override
    public RowDecoderEncoder<R> decoder() {
        return this;
    }

    @Override
    public final boolean next() {
        return false;
    }

    @Override
    public final byte[] lowBound() {
        return mLowBound;
    }

    @Override
    public final boolean lowInclusive() {
        return mLowInclusive;
    }

    @Override
    public final byte[] highBound() {
        return mHighBound;
    }

    @Override
    public final boolean highInclusive() {
        return mHighInclusive;
    }

    /**
     * Expected to be subclassed for supporting secondary index scans.
     */
    public abstract static class Joined<R> extends SingleScanController<R> {
        protected final Index mPrimaryIndex;

        protected Joined(byte[] lowBound, boolean lowInclusive,
                         byte[] highBound, boolean highInclusive,
                         Index primaryIndex)
        {
            super(lowBound, lowInclusive, highBound, highInclusive);
            mPrimaryIndex = primaryIndex;
        }

        /**
         * Given a positioned cursor over the secondary index and a decoded primary key, return
         * the associated primary value, or null if not found.
         *
         * @param secondaryCursor must have a non-null transaction
         */
        protected byte[] join(Cursor secondaryCursor, LockResult result, byte[] primaryKey)
            throws IOException
        {
            // FIXME: The double check logic must perform a full check against the primary
            // value, based on the predicate for this scan batch.

            Transaction txn = secondaryCursor.link();

            if (txn == null) {
                // FIXME: has no predicate lock, and so must double check
                throw new IllegalStateException("FIXME: txn: " + txn);
            }

            // FIXME: The filtered subclass overrides decodeRow and filters based on two levels
            // of encoding. That code won't go here, but I don't feel like moving the comment.

            // For any mode READ_COMMITTED or higher, the scanner (or updater) relies on
            // predicate locking. If a secondary key lock needs to be released (or it was
            // already released), the double check that the secondary is still valid only needs
            // to verify that the secondary entry still exists. The predicate lock prevents
            // against inserts and stores against the secondary index, but it doesn't prevent
            // deletes. If it did, then no double check would be needed at all.

            LockMode mode = txn.lockMode();
            switch (mode) {
            case UPGRADABLE_READ: case REPEATABLE_READ:
                if (result != LockResult.ACQUIRED) {
                    // The load might deadlock, but because the lock wasn't just acquired, it
                    // cannot be released anyhow. Charge ahead and hope for the best.
                    break;
                }

                // Lock without waiting, to prevent deadlock.
                LockResult result2 = mode == LockMode.UPGRADABLE_READ
                    ? mPrimaryIndex.tryLockUpgradable(txn, primaryKey, 0)
                    : mPrimaryIndex.tryLockShared(txn, primaryKey, 0);

                if (result2.isHeld()) {
                    // No deadlock. The primary lock is held, so no need to acquire it again
                    // when loading.
                    txn = Transaction.BOGUS;
                    break;
                }

                // Release the secondary key lock which was just acquired by the caller.
                txn.unlock();

                // If the load fails, the caller is expected to abort the scan without
                // attempting to release the acquired secondary key lock.
                byte[] value = mPrimaryIndex.load(txn, primaryKey);

                if (value != null) {
                    // Need to re-acquire the secondary key lock. This might also fail.
                    boolean original = secondaryCursor.autoload(false);
                    try {
                        secondaryCursor.lock();
                    } finally {
                        secondaryCursor.autoload(original);
                    }
                    if (secondaryCursor.value() != null) {
                        return value;
                    }
                }

                // Was concurrently deleted.

                if (result2 == LockResult.ACQUIRED) {
                    // The caller needs to release two locks.
                    txn.unlockCombine();
                }

                return null;

            case READ_COMMITTED:
                // FIXME: can still deadlock with a writer, but can obtain old value from undo log
                value = mPrimaryIndex.load(txn, primaryKey);
                if (value != null && !secondaryCursor.exists()) {
                    // Was concurrently deleted.
                    value = null;
                }
                return value;

            case READ_UNCOMMITTED:
                // FIXME: has no predicate lock, and so must double check
                throw new IllegalStateException("FIXME: READ_UNCOMMITTED");
            }

            return mPrimaryIndex.load(txn, primaryKey);
        }
    }
}
