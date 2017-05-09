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

import java.util.Comparator;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class IntersectionCursor extends MergeCursor {
    IntersectionCursor(MergeView view, Comparator<byte[]> comparator,
                       Cursor first, Cursor second)
    {
        super(view, comparator, first, second);
    }

    @Override
    protected MergeCursor newCursor(Cursor first, Cursor second) {
        return new IntersectionCursor(mView, mComparator, first, second);
    }

    @Override
    protected LockResult select(LockResult r1, LockResult r2) throws IOException {
        while (true) {
            byte[] k1 = mFirst.key();
            if (k1 == null) {
                reset();
                return LockResult.UNOWNED;
            }

            byte[] k2 = mSecond.key();
            if (k2 == null) {
                reset();
                return LockResult.UNOWNED;
            }

            byte[] value;
            int cmp = mComparator.compare(k1, k2);

            if (cmp == 0) {
                mKey = k1;
                value = mView.mCombiner.combine(k1, mFirst.value(), mSecond.value());
                LockResult result;
                if (value != null) {
                    result = combine(r1, r2);
                } else {
                    if (r1.isAcquired()) {
                        mFirst.link().unlock();
                    }
                    if (r2.isAcquired()) {
                        mSecond.link().unlock();
                    }
                    result = null;
                }

                mValue = value;
                mCompare = cmp;
            
                return result;
            }

            if ((cmp ^ mDirection) < 0) {
                if (r1.isAcquired()) {
                    mFirst.link().unlock();
                }
                r1 = mFirst.findNearbyGe(k2);
            } else {
                if (r2.isAcquired()) {
                    mSecond.link().unlock();
                }
                r2 = mSecond.findNearbyGe(k1);
            }
        }
    }

    @Override
    protected void doStore(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (value == null) {
            mFirst.store(null);
            mView.mSecond.touch(txn, key);
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
