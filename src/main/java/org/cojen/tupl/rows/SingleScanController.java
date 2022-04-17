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

import java.util.Comparator;

import org.cojen.tupl.Cursor;
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
    private final boolean mReverse;

    /**
     * Constructor which swaps the bounds when reverse is true.
     */
    protected SingleScanController(byte[] lowBound, boolean lowInclusive,
                                   byte[] highBound, boolean highInclusive,
                                   boolean reverse)
    {
        if (!reverse) {
            mLowBound = lowBound;
            mLowInclusive = lowInclusive;
            mHighBound = highBound;
            mHighInclusive = highInclusive;
        } else {
            mLowBound = highBound;
            mLowInclusive = highInclusive;
            mHighBound = lowBound;
            mHighInclusive = lowInclusive;
        }

        mReverse = reverse;
    }

    /**
     * Constructor which doesn't swap the bounds when reverse is true, possibly because they
     * were swapped earlier.
     */
    protected SingleScanController(boolean reverse,
                                   byte[] lowBound, boolean lowInclusive,
                                   byte[] highBound, boolean highInclusive)
    {
        mLowBound = lowBound;
        mLowInclusive = lowInclusive;
        mHighBound = highBound;
        mHighInclusive = highInclusive;
        mReverse = reverse;
    }

    /**
     * Reverse scan copy constructor.
     */
    protected SingleScanController(SingleScanController from) {
        this(true, from.mHighBound, from.mHighInclusive, from.mLowBound, from.mLowInclusive);
    }

    @Override
    public Comparator<byte[]> comparator() {
        Comparator<byte[]> cmp = RowUtils.KEY_COMPARATOR;
        if (mReverse) {
            cmp = cmp.reversed();
        }
        return cmp;
    }

    @Override
    public final boolean isSingleBatch() {
        return true;
    }

    @Override
    public final Cursor newCursor(View view, Transaction txn) throws IOException {
        if (mReverse) {
            // Must reverse before applying bounds, because they're supposed to be swapped.
            // This behavior is necessary for MergedScanController to function correctly.
            view = view.viewReverse();
        }

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

    @Override
    public final boolean isReverse() {
        return mReverse;
    }
}
