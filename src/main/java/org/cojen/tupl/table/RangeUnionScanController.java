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

import org.cojen.tupl.core.RowPredicate;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RangeUnionScanControllerFactory
 */
final class RangeUnionScanController<R> implements ScanController<R> {
    private final SingleScanController<R>[] mControllers;

    private int mPosition;
    private SingleScanController<R> mCurrent;

    /**
     * @param controllers must be ordered by ascending lower bound
     */
    RangeUnionScanController(SingleScanController<R>[] controllers) {
        mControllers = controllers;
        assignCurrent(0);
    }

    @Override
    public boolean isJoined() {
        for (SingleScanController controller : mControllers) {
            if (controller.isJoined()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RowPredicate<R> predicate() {
        return mControllers[0].predicate();
    }

    @Override
    public long estimateSize() {
        try {
            long size = mControllers[0].estimateSize();
            for (int i = 1; size != Long.MAX_VALUE && i < mControllers.length; i++) {
                long nextSize = mControllers[i].estimateSize();
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
        return mControllers[0].characteristics();
    }

    @Override
    public Cursor newCursor(View view, Transaction txn) throws IOException {
        return mCurrent.newCursor(view, txn);
    }

    @Override
    public RowEvaluator<R> evaluator() {
        return mCurrent.evaluator();
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

    private void assignCurrent(int pos) {
        SingleScanController<R> current = mControllers[pos];
        Comparator<byte[]> comparator = current.comparator();

        while (true) {
            int nextPos = pos + 1;
            if (nextPos >= mControllers.length) {
                break;
            }
            SingleScanController<R> next = mControllers[nextPos];
            MergedScanController<R> merged =
                MergedScanController.tryMerge(comparator, current, next);
            if (merged == null) {
                break;
            }
            current = merged;
            pos = nextPos;
        }

        mPosition = pos;
        mCurrent = current;
    }
}
