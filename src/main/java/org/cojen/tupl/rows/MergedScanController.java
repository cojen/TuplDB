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

import java.util.Arrays;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see MultiScanController
 */
final class MergedScanController<R> extends SingleScanController<R> {
    /**
     * Tries to merge two single-batch ScanControllers into one. Neither can be empty, one must
     * have a lower bound than the other, and they must overlap bounds. That is, the low bound
     * of the high controller must not be higher than the high bound of the low controller.
     *
     * @return null if cannot merge
     */
    static <R> MergedScanController<R> tryMerge(ScanController<R> low, ScanController<R> high) {
        if (!low.isSingleBatch() || !high.isSingleBatch()) {
            return null;
        }

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
            int cmp = Arrays.compareUnsigned(lowHigh, highLow);
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

        return new MergedScanController<R>
            (lowLow, low.lowInclusive(), highBound, highInclusive, low, high, mode);
    }

    private final ScanController<R> mLow, mHigh;
    private final RowDecoderEncoder<R> mLowDecoder, mHighDecoder;

    /* modes:
       0: in low range only (can transition to mode 1, 2 or 3)
       1: in both ranges (can transition to mode 2 or 3)
       2: in low range only, past the high range
       3: in high range only, past the low range
     */
    private int mMode;

    private MergedScanController(byte[] lowBound, boolean lowInclusive,
                                 byte[] highBound, boolean highInclusive,
                                 ScanController<R> low, ScanController<R> high,
                                 int mode)
    {
        super(lowBound, lowInclusive, highBound, highInclusive);
        mLow = low;
        mHigh = high;
        mLowDecoder = low.decoder();
        mHighDecoder = high.decoder();
        mMode = mode;
    }

    @Override
    public R decodeRow(byte[] key, byte[] value, R row) throws IOException {
        if (mMode == 2) {
            return mLowDecoder.decodeRow(key, value, row);
        }

        if (mMode != 3) {
            // Modes 0 and 1...
            if (mLow.isTooHigh(key)) {
                // Fallthrough to mode 3.
                mMode = 3;
            } else {
                R decoded = mLowDecoder.decodeRow(key, value, row);
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
                    // Transaction to mode 2, but the row was already rejected.
                    mMode = 2;
                    return null;
                }
            }
        }

        return mHighDecoder.decodeRow(key, value, row);
    }

    @Override
    public byte[] encodeKey(R row) {
        // Can call either decoder. They should do the same thing.
        return mLowDecoder.encodeKey(row);
    }

    @Override
    public byte[] encodeValue(R row) {
        // Can call either decoder. They should do the same thing.
        return mLowDecoder.encodeValue(row);
    }
}
