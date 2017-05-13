/*
 *  Copyright (C) 2011-2017 Cojen.org
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
final class IntersectionView extends MergeView {
    IntersectionView(Combiner combiner, View first, View second) {
        super(combiner, first, second);
    }

    @Override
    protected byte[] doLoad(Transaction txn, byte[] key) throws IOException {
        byte[] v1 = mFirst.load(txn, key);
        if (v1 == null) {
            // Always need to lock the second entry too, for consistency and to avoid any odd
            // deadlocks if the store method is called.
            mSecond.touch(txn, key);
            return null;
        }
        byte[] v2 = mSecond.load(txn, key);
        return v2 == null ? null : mCombiner.combine(key, v1, v2);
    }

    @Override
    protected MergeCursor newCursor(Transaction txn, MergeView view,
                                    Cursor first, Cursor second)
    {
        return new IntersectionCursor(txn, view, first, second);
    }

    @Override
    protected String type() {
        return "intersection";
    }
}
