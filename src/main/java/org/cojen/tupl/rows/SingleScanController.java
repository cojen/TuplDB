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

        // Subclass should implement one of these methods. The secondaryValue param is required
        // for alternate keys.
        //protected static byte[] toPrimaryKey(byte[] secondaryKey);
        //protected static byte[] toPrimaryKey(byte[] secondaryKey, byte[] secondaryValue);

        /**
         * Given a positioned cursor over the secondary index and a decoded primary key, return
         * the associated primary value, or null if not found.
         *
         * @param secondaryCursor must have a non-null transaction
         */
        protected byte[] join(Cursor secondaryCursor, LockResult result, byte[] primaryKey)
            throws IOException
        {
            Transaction txn = secondaryCursor.link();
            byte[] primaryValue = mPrimaryIndex.load(txn, primaryKey);

            if (result == LockResult.ACQUIRED && txn.lastLockedKey() == primaryKey &&
                txn.lastLockedIndex() == mPrimaryIndex.id())
            {
                // Combine the secondary and primary locks together, so that they can be
                // released together if the row is filtered out.
                txn.unlockCombine();
            }

            if (primaryValue != null && txn.lockMode() == LockMode.READ_COMMITTED) {
                // The scanner relies on predicate locking, and so validation only needs to
                // check that the secondary entry still exists. The predicate lock prevents
                // against inserts and stores against the secondary index, but it doesn't
                // prevent deletes. If it did, then no validation would be needed at all.
                if (!secondaryCursor.exists()) {
                    // Was concurrently deleted. Note that the exists call doesn't observe
                    // an uncommitted delete because it would store a ghost.
                    primaryValue = null;
                }
            }

            return primaryValue;
        }
    }
}
