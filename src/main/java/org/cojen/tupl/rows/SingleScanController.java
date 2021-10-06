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
}
