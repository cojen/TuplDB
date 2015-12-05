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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TransformerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TransformerTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected View openIndex(String name) throws Exception {
        return mDb.openIndex(name);
    }

    protected boolean verify(View ix) throws Exception {
        return ((Index) ix).verify(null);
    }

    protected Database mDb;

    @Test
    public void keyFlipped() throws Exception {
        Index ix = fill();

        View view = ix.viewTransformed(new KeyFlipper());

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.first(); c.key() != null; c.next()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result > 0);
                }
                lastKey = c.key();
            }
        }

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.last(); c.key() != null; c.previous()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result < 0);
                }
                lastKey = c.key();
            }
        }

        view = view.viewReverse();

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.first(); c.key() != null; c.next()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result > 0);
                }
                lastKey = c.key();
            }
        }

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.last(); c.key() != null; c.previous()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result < 0);
                }
                lastKey = c.key();
            }
        }
    }

    private Index fill() throws Exception {
        Index ix = mDb.openIndex("transformed");
        for (int i=20; i<=90; i+=3) {
            byte[] key = key(i);
            ix.store(null, key, key);
        }
        return ix;
    }

    private byte[] key(int n) {
        return String.valueOf(n).getBytes();
    }

    static class KeyFlipper implements Transformer {
        @Override
        public boolean requireValue() {
            return false;
        }

        @Override
        public byte[] transformValue(byte[] value, byte[] key, byte[] tkey) {
            return value;
        }

        @Override
        public byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey) {
            return tvalue;
        }

        @Override
        public byte[] transformKey(byte[] key, byte[] value) {
            return flip(key);
        }

        @Override
        public byte[] inverseTransformKey(byte[] tkey) {
            return flip(tkey);
        }

        private static byte[] flip(byte[] key) {
            byte[] flipped = key.clone();
            int len = flipped.length;
            for (int i=0, mid=len>>1, j=len-1; i<mid; i++, j--) {
                byte t = flipped[i];
                flipped[i] = flipped[j];
                flipped[j] = t;
            }
            return flipped;
        }
    }
}
