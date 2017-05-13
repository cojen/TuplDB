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
final class IntersectionCursor extends MergeCursor {
    IntersectionCursor(Transaction txn, MergeView view, Cursor first, Cursor second) {
        super(txn, view, first, second);
    }

    @Override
    protected MergeCursor newCursor(Cursor first, Cursor second) {
        return new IntersectionCursor(mTxn, mView, first, second);
    }

    @Override
    protected LockResult select(Transaction txn) throws IOException {
        while (true) {
            final byte[] k1 = mFirst.key();
            if (k1 == null) {
                reset();
                return LockResult.UNOWNED;
            }
            final byte[] k2 = mSecond.key();
            if (k2 == null) {
                reset();
                return LockResult.UNOWNED;
            }
            final int cmp = getComparator().compare(k1, k2);
            if (cmp == 0) {
                mCompare = cmp;
                return selectCombine(txn, k1);
            } else if (mDirection == DIRECTION_FORWARD) {
                if (cmp < 0) {
                    mFirst.findNearbyGe(k2);
                } else {
                    mSecond.findNearbyGe(k1);
                }
            } else {
                if (cmp > 0) {
                    mFirst.findNearbyLe(k2);
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
                    if (first != null && second != null) {
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
