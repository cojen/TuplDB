/*
 *  Copyright (C) 2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class DifferenceCursor extends MergeCursor {
    DifferenceCursor(Transaction txn, MergeView view, Cursor first, Cursor second) {
        super(txn, view, first, second);
    }

    @Override
    protected MergeCursor newCursor(Cursor first, Cursor second) {
        return new DifferenceCursor(mTxn, mView, first, second);
    }

    @Override
    protected LockResult select(Transaction txn) throws IOException {
        final byte[] k1 = mFirst.key();
        if (k1 == null) {
            reset();
            return LockResult.UNOWNED;
        }

        while (true) {
            final byte[] k2 = mSecond.key();
            if (k2 == null) {
                mCompare = -2 ^ mDirection; // is 1 when reversed
                return selectFirst(txn, k1);
            } else {
                final int cmp = getComparator().compare(k1, k2);
                if (cmp == 0) {
                    mCompare = 0;
                    return selectCombine(txn, k1);
                } else if ((cmp ^ mDirection) < 0) {
                    mCompare = cmp;
                    return selectFirst(txn, k1);
                } else if (mDirection == DIRECTION_FORWARD) {
                    mSecond.findNearbyGe(k1);
                } else {
                    mSecond.findNearbyLe(k1);
                }
            }
        }
    }

    @Override
    protected void doStore(byte[] key, byte[] value) throws IOException {
        if (value == null) {
            mFirst.store(null);
        } else {
            byte[][] values = mView.mCombiner.separate(key, value);

            byte[] first, second;
            check: {
                if (values != null) {
                    first = values[0];
                    second = values[1];
                    if (first != null) {
                        break check;
                    }
                }
                throw storeFail();
            }

            mFirst.store(first);
            mSecond.store(second);
        }
    }
}
