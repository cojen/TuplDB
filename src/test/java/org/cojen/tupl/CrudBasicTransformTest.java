/*
 *  Copyright 2015 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
        public byte[] transformKey(byte[] key, byte[] value) {
            // First part of key in underlying index is a copy of the second.
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
