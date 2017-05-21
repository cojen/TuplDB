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

import java.nio.charset.StandardCharsets;

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
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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

    @Test
    public void skipZero() throws Exception {
        Index ix = fill();
        View view = ix.viewTransformed(new KeyFlipper());
        Cursor c = view.newCursor(null);
        c.first();
        assertEquals(LockResult.UNOWNED, c.skip(0));
        c.reset();
    }

    @Test
    public void unsupportedKeys() throws Exception {
        // Tests expected behavior when loading/storing unsupported keys.

        Index ix = fill();
        View view = ix.viewTransformed(new OddKeys());

        assertNull(view.load(null, key(20)));
        fastAssertArrayEquals(key(23), view.load(null, key(23)));

        for (int i=10; i<=20; i+=10) {
            view.store(null, key(i), null);
            try {
                view.store(null, key(i), "world".getBytes());
                fail();
            } catch (ViewConstraintException e) {
                // Expected.
            }

            assertNull(view.exchange(null, key(i), null));
            try {
                view.exchange(null, key(i), "world".getBytes());
                fail();
            } catch (ViewConstraintException e) {
                // Expected.
            }

            assertTrue(view.insert(null, key(i), null));

            try {
                view.insert(null, key(i), "world".getBytes());
                fail();
            } catch (ViewConstraintException e) {
                // Expected.
            }

            assertFalse(view.replace(null, key(i), null));
            assertFalse(view.replace(null, key(i), "world".getBytes()));

            assertFalse(view.update(null, key(i), "world".getBytes(), null));
            assertFalse(view.update(null, key(i), "world".getBytes(), "world".getBytes()));
            assertTrue(view.update(null, key(i), null, null));
            try {
                view.update(null, key(i), null, "world".getBytes());
                fail();
            } catch (ViewConstraintException e) {
                // Expected.
            }

            assertFalse(view.delete(null, key(i)));

            assertFalse(view.remove(null, key(i), "world".getBytes()));
            assertTrue(view.remove(null, key(i), null));
        }
    }

    @Test
    public void unsupportedCursorKeys() throws Exception {
        // Tests expected behavior when loading/storing unsupported keys with a cursor.

        Index ix = fill();
        View view = ix.viewTransformed(new OddKeys());

        Transaction txn = mDb.newTransaction();
        Cursor c = view.newCursor(txn);

        assertEquals(LockResult.UNOWNED, c.find(key(20)));
        fastAssertArrayEquals(key(20), c.key());
        assertNull(c.value());

        try {
            c.lock();
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            c.load();
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            c.store("hello".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            c.next();
            fail();
        } catch (UnpositionedCursorException e) {
            // Expected.
        }

        assertEquals(LockResult.ACQUIRED, c.find(key(23)));
        fastAssertArrayEquals(key(23), c.key());
        fastAssertArrayEquals(key(23), c.value());
    }

    @Test
    public void unsupportedCursorValues() throws Exception {
        // Tests expected behavior when accessing unsupported values with a cursor.

        Index ix = mDb.openIndex("test");

        byte[] key1 = "key1".getBytes();
        byte[] value1 = "maG{1".getBytes(StandardCharsets.UTF_8);

        byte[] key2 = "key2".getBytes();
        byte[] value2 = "maG{2".getBytes(StandardCharsets.UTF_8);

        byte[] key3 = "key3".getBytes();
        byte[] value3 = "hello".getBytes(StandardCharsets.UTF_8);

        ix.store(null, key1, value1);
        ix.store(null, key2, value2);
        ix.store(null, key3, value3);

        View view = ix.viewTransformed(new CrudBasicFilterTest.BasicFilter());

        Transaction txn = mDb.newTransaction();
        Cursor c = view.newCursor(txn);

        assertEquals(LockResult.UNOWNED, c.find(key1));
        fastAssertArrayEquals(key1, c.key());
        assertNull(c.value());

        assertEquals(LockResult.ACQUIRED, c.lock());
        assertEquals(LockResult.OWNED_UPGRADABLE, c.load());

        try {
            c.store(value2);
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        assertEquals(LockResult.OWNED_UPGRADABLE, ix.lockCheck(txn, c.key()));

        c.store("world".getBytes());
        assertEquals(LockResult.OWNED_EXCLUSIVE, c.skip(0));

        fastAssertArrayEquals("world".getBytes(), ix.load(txn, c.key()));

        assertEquals(LockResult.ACQUIRED, c.next());
        fastAssertArrayEquals(key3, c.key());

        c.reset();
        txn.commit();
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

    static class OddKeys implements Filter {
        @Override
        public boolean isAllowed(byte[] key, byte[] value) {
            return key.length > 0 && (key[key.length - 1] & 1) != 0;
        }

        @Override
        public byte[] inverseTransformKey(byte[] tkey) {
            return isAllowed(tkey, null) ? tkey : null;
        }
    }
}
