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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ScanControllerFactory
 */
public interface ScanController<R> {
    static final byte[] EMPTY = new byte[0];

    boolean isSingleBatch();

    /**
     * Returns a new cursor for the current scan batch.
     */
    Cursor newCursor(View view, Transaction txn) throws IOException;

    /**
     * Returns the decoder for the current scan batch.
     */
    RowDecoderEncoder<R> decoder();

    /**
     * Move to the next batch, returning false if none.
     */
    boolean next();

    /**
     * Returns the low bounding range, or null if unbounded, or EMPTY if low bound is so high
     * that the scan results are empty.
     */
    byte[] lowBound();

    boolean lowInclusive();

    /**
     * Returns the high bounding range, or null if unbounded.
     */
    byte[] highBound();

    boolean highInclusive();

    /**
     * Returns true if the given key is lower than the low bounding range.
     */
    default boolean isTooLow(byte[] key) {
        int cmp = Arrays.compareUnsigned(key, lowBound());
        return cmp < 0 || (cmp == 0 && !lowInclusive());
    }

    /**
     * Returns true if the given key is higher than the high bounding range.
     */
    default boolean isTooHigh(byte[] key) {
        int cmp = Arrays.compareUnsigned(key, highBound());
        return cmp > 0 || (cmp == 0 && !highInclusive());
    }

    /**
     * Compare the low bound of this controller to another. Returns -1 is this lower bound is
     * lower than the other lower bound, etc.
     *
     * @return -1, 0, or 1
     */
    default int compareLow(ScanController other) {
        byte[] thisLow = this.lowBound();
        byte[] otherLow = other.lowBound();

        int cmp = Arrays.compareUnsigned(thisLow, otherLow);

        if (cmp == 0 && thisLow != null) {
            cmp = -Boolean.compare(this.lowInclusive(), other.lowInclusive());
        }

        return cmp;
    }

    /**
     * Compare the high bound of this controller to another. Returns -1 is this higher bound is
     * lower than the other higher bound, etc.
     *
     * @return -1, 0, or 1
     */
    default int compareHigh(ScanController other) {
        byte[] thisHigh = this.highBound();
        byte[] otherHigh = other.highBound();

        int cmp;

        if (thisHigh == null) {
            cmp = otherHigh == null ? 0 : 1;
        } else if (otherHigh == null) {
            cmp = -1;
        } else {
            cmp = Arrays.compareUnsigned(thisHigh, otherHigh);
            if (cmp == 0) {
                cmp = Boolean.compare(this.highInclusive(), other.highInclusive());
            }
        }

        return cmp;
    }
}
