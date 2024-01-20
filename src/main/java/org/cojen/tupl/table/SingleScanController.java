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

package org.cojen.tupl.table;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import static java.util.Spliterator.*;

/**
 * Only supports one scan batch.
 *
 * @author Brian S O'Neill
 */
public abstract class SingleScanController<R> implements ScanController<R>, RowEvaluator<R> {
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
    public boolean isJoined() {
        return false;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | CONCURRENT | DISTINCT;
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
    public final RowEvaluator<R> evaluator() {
        return this;
    }

    @Override
    public final boolean next() {
        return false;
    }

    protected final byte[] lowBound() {
        return mLowBound;
    }

    protected final boolean lowInclusive() {
        return mLowInclusive;
    }

    protected final byte[] highBound() {
        return mHighBound;
    }

    protected final boolean highInclusive() {
        return mHighInclusive;
    }

    protected final boolean isReverse() {
        return mReverse;
    }

    /**
     * Returns true if the given key is lower than the low bounding range.
     *
     * @param key non-null
     */
    protected final boolean isTooLow(byte[] key) {
        byte[] low = lowBound();
        if (low == null) {
            return false;
        }
        int cmp = comparator().compare(key, low);
        return cmp < 0 || (cmp == 0 && !lowInclusive());
    }

    /**
     * Returns true if the given key is higher than the high bounding range.
     *
     * @param key non-null
     */
    protected final boolean isTooHigh(byte[] key) {
        byte[] high = highBound();
        if (high == null) {
            return false;
        }
        int cmp = comparator().compare(key, high);
        return cmp > 0 || (cmp == 0 && !highInclusive());
    }

    /**
     * Compare the low bound of this controller to another. Returns -1 is this lower bound is
     * lower than the other lower bound, etc.
     *
     * @return -1, 0, or 1
     */
    protected final int compareLow(SingleScanController other) {
        byte[] thisLow = this.lowBound();
        byte[] otherLow = other.lowBound();

        int cmp;

        if (thisLow == null) {
            cmp = otherLow == null ? 0 : -1;
        } else if (otherLow == null) {
            cmp = 1;
        } else {
            cmp = comparator().compare(thisLow, otherLow);
            if (cmp == 0) {
                cmp = -Boolean.compare(this.lowInclusive(), other.lowInclusive());
            }
        }

        return cmp;
    }

    /**
     * Compare the high bound of this controller to another. Returns -1 is this higher bound is
     * lower than the other higher bound, etc.
     *
     * @return -1, 0, or 1
     */
    protected final int compareHigh(SingleScanController other) {
        byte[] thisHigh = this.highBound();
        byte[] otherHigh = other.highBound();

        int cmp;

        if (thisHigh == null) {
            cmp = otherHigh == null ? 0 : 1;
        } else if (otherHigh == null) {
            cmp = -1;
        } else {
            cmp = comparator().compare(thisHigh, otherHigh);
            if (cmp == 0) {
                cmp = Boolean.compare(this.highInclusive(), other.highInclusive());
            }
        }

        return cmp;
    }

    protected final Comparator<byte[]> comparator() {
        Comparator<byte[]> cmp = RowUtils.KEY_COMPARATOR;
        if (mReverse) {
            cmp = cmp.reversed();
        }
        return cmp;
    }
}
