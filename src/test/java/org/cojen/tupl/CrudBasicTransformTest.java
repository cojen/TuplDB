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

import java.util.HashMap;
import java.util.Map;

/**
 * Tests that operations against a basic transformed view still work.
 *
 * @author Brian S O'Neill
 */
public class CrudBasicTransformTest extends CrudNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudBasicTransformTest.class.getName());
    }

    private final Map<View, Index> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix = mDb.openIndex(name);
        View view = ix.viewTransformed(new BasicTransform());
        mViews.put(view, ix);
        return view;
    }

    @Override
    protected boolean verify(View view) throws Exception {
        return mViews.get(view).verify(null);
    }

    static class BasicTransform implements Transformer {
        @Override
        public byte[] transformValue(byte[] value, byte[] key, byte[] tkey) {
            if (value == null || value == Cursor.NOT_LOADED) {
                return value;
            }
            byte[] copy = value.clone();
            for (int i=0; i<copy.length; i++) {
                copy[i] += 1;
            }
            return copy;
        }

        @Override
        public byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey) {
            if (tvalue == null || tvalue == Cursor.NOT_LOADED) {
                return tvalue;
            }
            byte[] copy = tvalue.clone();
            for (int i=0; i<copy.length; i++) {
                copy[i] -= 1;
            }
            return copy;
        }

        @Override
        public byte[] transformKey(Cursor cursor) {
            // First part of key in underlying index is a copy of the second.
            byte[] key = cursor.key();
            byte[] tkey = new byte[key.length >> 1];
            System.arraycopy(key, 0, tkey, 0, tkey.length);
            return tkey;
        }

        @Override
        public byte[] inverseTransformKey(byte[] tkey) {
            // Store the key concatenated with itself in the underlying index.
            byte[] dup = new byte[tkey.length << 1];
            System.arraycopy(tkey, 0, dup, 0, tkey.length);
            System.arraycopy(tkey, 0, dup, tkey.length, tkey.length);
            return dup;
        }
    }
}
