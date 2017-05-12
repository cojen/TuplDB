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

import java.util.HashMap;
import java.util.Map;

/**
 * Tests operations against a union which partitions the entries into separate views.
 *
 * @author Brian S O'Neill
 */
public class CrudDisjointUnionTest extends CrudNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudDisjointUnionTest.class.getName());
    }

    private final Map<View, Index[]> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix0 = mDb.openIndex(name + "-0");
        Index ix1 = mDb.openIndex(name + "-1");
        View view = ix0.viewUnion(new Disjoint(), ix1);
        mViews.put(view, new Index[] {ix0, ix1});
        return view;
    }

    @Override
    protected boolean verify(View view) throws Exception {
        boolean result = true;
        for (Index ix : mViews.get(view)) {
            result &= ix.verify(null);
        }
        return result;
    }

    static class Disjoint implements Combiner {
        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            throw new AssertionError();
        }

        @Override
        public boolean requireValues() {
            return false;
        }

        @Override
        public byte[][] separate(byte[] key, byte[] value) {
            int hash = 0;
            for (byte b : key) {
                hash = hash * 31 + b;
            }
            byte[][] pair = new byte[2][];
            pair[hash & 1] = value;
            return pair;
        }
    }
}
