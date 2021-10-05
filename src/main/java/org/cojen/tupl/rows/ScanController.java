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
    default boolean next() {
        return false;
    }

    /**
     * Returns the low bounding range, or null if unbounded, or EMPTY if low bound is so high
     * that the scan results are empty.
     */
    // FIXME: Be sure that MultiScanController checks for EMPTY result.
    default byte[] lowBound() {
        return null;
    }

    /**
     * Returns true by default.
     */
    default boolean lowInclusive() {
        return true;
    }

    /**
     * Returns the high bounding range, or null if unbounded.
     */
    default byte[] highBound() {
        return null;
    }

    /**
     * Returns false by default.
     */
    default boolean highInclusive() {
        return false;
    }
}
