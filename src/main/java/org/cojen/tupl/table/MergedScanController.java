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
import org.cojen.tupl.LockResult;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RangeUnionScanController
 */
final class MergedScanController<R> extends SingleScanController<R> {
    /**
     * Tries to merge two single-batch ScanControllers into one. Neither can be empty, one must
     * have a lower bound than the other, and they must overlap bounds. That is, the low bound
     * of the high controller must not be higher than the high bound of the low controller.
     *
     * @return null if cannot merge
     */
    static <R> MergedScanController<R> tryMerge(Comparator<byte[]> comparator,
                                                SingleScanController<R> low,
                                                SingleScanController<R> high)
    {
        byte[] lowLow = low.lowBound();
        if (lowLow == EMPTY) {
            return null;
        }

        byte[] highLow = high.lowBound();
        if (highLow == EMPTY) {
            return null;
        }

        byte[] lowHigh = low.highBound();

        if (lowHigh != null && highLow != null) {
            int cmp = comparator.compare(lowHigh, highLow);
            if (cmp < 0 || (cmp == 0 && !(low.highInclusive() && high.lowInclusive()))) {
                return null;
            }
        }

        // Choose the higher bound.

        byte[] highBound;
        boolean highInclusive;

        if (low.compareHigh(high) < 0) {
            highBound = high.highBound();
            highInclusive = high.highInclusive();
        } else {
            highBound = lowHigh;
            highInclusive = low.highInclusive();
        }

        // Start in mode 1 (both ranges) if the lower bound of both is the same.
        int mode = low.compareLow(high) == 0 ? 1 : 0;

        boolean reverse = low.isReverse();
        if (reverse != high.isReverse()) {
            throw new AssertionError();
        }

        return new MergedScanController<>
            (lowLow, low.lowInclusive(), highBound, highInclusive, low, high, reverse, mode);
    }

    private final SingleScanController<R> mLow, mHigh;
    private final RowEvaluator<R> mLowEvaluator, mHighEvaluator;

    /* modes:
       0: in low range only (can transition to mode 1, 2 or 3)
       1: in both ranges (can transition to mode 2 or 3)
       2: in low range only, past the high range
       3: in high range only, past the low range
     */
    private int mMode;

    private MergedScanController(byte[] lowBound, boolean lowInclusive,
                                 byte[] highBound, boolean highInclusive,
                                 SingleScanController<R> low, SingleScanController<R> high,
                                 boolean reverse, int mode)
    {
        super(reverse, lowBound, lowInclusive, highBound, highInclusive);
        mLow = low;
        mHigh = high;
        mLowEvaluator = low.evaluator();
        mHighEvaluator = high.evaluator();
        mMode = mode;
    }

    @Override
    public R evalRow(Cursor c, LockResult result, R row) throws IOException {
        if (mMode == 2) {
            return mLowEvaluator.evalRow(c, result, row);
        }

        if (mMode != 3) {
            byte[] key = c.key();

            // Modes 0 and 1...
            if (mLow.isTooHigh(key)) {
                // Fallthrough to mode 3.
                mMode = 3;
            } else {
                R decoded = mLowEvaluator.evalRow(c, result, row);
                if (decoded != null) {
                    return decoded;
                }
                if (mMode == 0) {
                    if (mHigh.isTooLow(key)) {
                        return null;
                    }
                    // Fallthrough to mode 1.
                    mMode = 1;
                }
                // Mode 1.
                if (mHigh.isTooHigh(key)) {
                    // Transition to mode 2, but the row was already rejected.
                    mMode = 2;
                    return null;
                }
            }
        }

        return mHighEvaluator.evalRow(c, result, row);
    }

    @Override
    public long estimateSize() {
        try {
            long size = mLow.estimateSize();
            if (size != Long.MAX_VALUE) {
                long nextSize = mHigh.estimateSize();
                if (nextSize == Long.MAX_VALUE) {
                    return nextSize;
                }
                size = Math.addExact(size, nextSize);
            }
            return size;
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public int characteristics() {
        // Can call either controller. They should have the same characteristics.
        return mLow.characteristics();
    }

    @Override
    public long evolvableTableId() {
        // Can call either evaluator. They should be bound to the same table.
        return mLowEvaluator.evolvableTableId();
    }

    @Override
    public byte[] secondaryDescriptor() {
        // Can call either evaluator. They should be bound to the same table.
        return mLowEvaluator.secondaryDescriptor();
    }

    @Override
    public R decodeRow(R row, byte[] key, byte[] value) throws IOException {
        // Can call either evaluator. They should be bound to the same table.
        return mLowEvaluator.decodeRow(row, key, value);
    }

    @Override
    public void writeRow(RowWriter writer, byte[] key, byte[] value) throws IOException {
        // Can call either evaluator. They should be bound to the same table.
        mLowEvaluator.writeRow(writer, key, value);
    }

    @Override
    public byte[] updateKey(R row, byte[] original) throws IOException {
        // Can call either evaluator. They should be bound to the same table.
        return mLowEvaluator.updateKey(row, original);
    }

    @Override
    public byte[] updateValue(R row, byte[] original) throws IOException {
        // Can call either evaluator. They should be bound to the same table.
        return mLowEvaluator.updateValue(row, original);
    }
}
