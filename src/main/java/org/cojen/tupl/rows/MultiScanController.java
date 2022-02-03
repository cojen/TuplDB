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

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class MultiScanController<R> implements ScanController<R> {
    private final ScanController<R>[] mControllers;

    private int mPosition;
    private ScanController<R> mCurrent;

    /**
     * @param controllers must be ordered by ascending lower bound
     */
    MultiScanController(ScanController<R>[] controllers) {
        mControllers = controllers;
        assignCurrent(0);
    }

    @Override
    public RowPredicate<R> predicate() {
        return mControllers[0].predicate();
    }

    @Override
    public boolean isSingleBatch() {
        return false;
    }

    @Override
    public Cursor newCursor(View view, Transaction txn) throws IOException {
        return mCurrent.newCursor(view, txn);
    }

    @Override
    public RowDecoderEncoder<R> decoder() {
        return mCurrent.decoder();
    }

    @Override
    public boolean next() {
        int pos = mPosition + 1;
        if (pos < mControllers.length) {
            assignCurrent(pos);
            return true;
        } else {
            mCurrent = null;
            return false;
        }
    }

    @Override
    public byte[] lowBound() {
        return mCurrent.lowBound();
    }

    @Override
    public boolean lowInclusive() {
        return mCurrent.lowInclusive();
    }

    @Override
    public byte[] highBound() {
        return mCurrent.highBound();
    }

    @Override
    public boolean highInclusive() {
        return mCurrent.highInclusive();
    }

    private void assignCurrent(int pos) {
        ScanController<R> current = mControllers[pos];

        while (true) {
            int nextPos = pos + 1;
            if (nextPos >= mControllers.length) {
                break;
            }
            ScanController<R> next = mControllers[nextPos];
            MergedScanController<R> merged = MergedScanController.tryMerge(current, next);
            if (merged == null) {
                break;
            }
            current = merged;
            pos = nextPos;
        }

        mPosition = pos;
        mCurrent = current;
    }

    @Override
    public QueryPlan plan() {
        var plans = new QueryPlan[mControllers.length];
        for (int i=0; i<plans.length; i++) {
            plans[i] = mControllers[i].plan();
        }
        return new QueryPlan.RangeUnion(plans);
    }
}
