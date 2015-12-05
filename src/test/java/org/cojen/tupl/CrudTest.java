/*
 *  Copyright 2012-2015 Cojen.org
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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudTest.class.getName());
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
    public void loadNothing() throws Exception {
        loadNothing(null);
        loadNothing(Transaction.BOGUS);
        loadNothing(mDb.newTransaction());
    }

    private void loadNothing(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.load(txn, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        assertEquals(null, ix.load(txn, new byte[0]));
        assertEquals(null, ix.load(txn, "key".getBytes()));
    }

    @Test
    public void testStoreBasic() throws Exception {
        testStoreBasic(null);
        testStoreBasic(Transaction.BOGUS);
        testStoreBasic(mDb.newTransaction());
    }

    private void testStoreBasic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.store(txn, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        ix.store(txn, key, value);
        assertArrayEquals(value, ix.load(txn, key));

        ix.store(txn, key, value2);
        assertArrayEquals(value2, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        ix.store(txn, key, null);
        assertNull(ix.load(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(txn, key, value);
            txn.commit();
            assertArrayEquals(value, ix.load(txn, key));

            ix.store(txn, key, value2);
            txn.commit();
            assertArrayEquals(value2, ix.load(txn, key));

            ix.store(txn, key, value);
            txn.exit();
            assertArrayEquals(value2, ix.load(txn, key));
        }
    }

    @Test
    public void testExchangeBasic() throws Exception {
        View ix = openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertEquals(null, ix.exchange(null, key, value));
        assertArrayEquals(value, ix.load(null, key));

        assertArrayEquals(value, ix.exchange(null, key, value2));
        assertArrayEquals(value2, ix.load(null, key));

        assertNull(ix.load(null, key2));

        assertArrayEquals(value2, ix.exchange(null, key, null));
        assertNull(ix.load(null, key));
    }

    @Test
    public void testInsertBasic1() throws Exception {
        testInsertBasic(null);
    }

    @Test
    public void testInsertBasic2() throws Exception {
        testInsertBasic(Transaction.BOGUS);
    }

    @Test
    public void testInsertBasic3() throws Exception {
        testInsertBasic(mDb.newTransaction());
    }

    private void testInsertBasic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.insert(txn, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertTrue(ix.insert(txn, key, value));
        assertArrayEquals(value, ix.load(txn, key));

        assertFalse(ix.insert(txn, key, value2));
        assertArrayEquals(value, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        assertFalse(ix.insert(txn, key, null));
        assertArrayEquals(value, ix.load(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            assertTrue(ix.insert(txn, key2, value));
            txn.exit();
            assertNull(ix.load(txn, key2));
        }
    }

    @Test
    public void testReplaceBasic1() throws Exception {
        testReplaceBasic(null);
    }

    @Test
    public void testReplaceBasic2() throws Exception {
        testReplaceBasic(Transaction.BOGUS);
    }

    @Test
    public void testReplaceBasic3() throws Exception {
        testReplaceBasic(mDb.newTransaction());
    }

    private void testReplaceBasic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.replace(txn, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertFalse(ix.replace(txn, key, value));
        assertTrue(ix.insert(txn, key, new byte[0]));

        assertTrue(ix.replace(txn, key, value));
        assertArrayEquals(value, ix.load(txn, key));

        assertTrue(ix.replace(txn, key, value2));
        assertArrayEquals(value2, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        assertTrue(ix.replace(txn, key, null));
        assertNull(ix.load(txn, key));

        assertFalse(ix.replace(txn, key, value));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.replace(txn, key, value2));
            txn.exit();
            // Bogus transaction artifact. Undo log entry was created earler.
            assertNull(ix.load(txn, key));

            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.replace(txn, key, value2));
            txn.exit();
            assertArrayEquals(value, ix.load(txn, key));
        }
    }

    @Test
    public void testUpdateBasic1() throws Exception {
        testUpdateBasic(null);
    }

    @Test
    public void testUpdateBasic2() throws Exception {
        testUpdateBasic(Transaction.BOGUS);
    }

    @Test
    public void testUpdateBasic3() throws Exception {
        testUpdateBasic(mDb.newTransaction());
    }

    private void testUpdateBasic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.update(txn, null, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertTrue(ix.update(txn, key, null, value));
        assertArrayEquals(value, ix.load(txn, key));

        assertTrue(ix.update(txn, key, value, value2));
        assertArrayEquals(value2, ix.load(txn, key));

        assertFalse(ix.update(txn, key, value, value));
        assertArrayEquals(value2, ix.load(txn, key));

        assertFalse(ix.update(txn, key, value, value2));
        assertArrayEquals(value2, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        assertTrue(ix.update(txn, key, value2, null));
        assertNull(ix.load(txn, key));

        assertFalse(ix.update(txn, key, value, value2));
        assertNull(ix.load(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.update(txn, key, value, value2));
            txn.exit();
            // Bogus transaction artifact. Undo log entry was created earler.
            assertNull(ix.load(txn, key));

            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.update(txn, key, value, value2));
            txn.exit();
            assertArrayEquals(value, ix.load(txn, key));
        }
    }

    @Test
    public void testDeleteBasic1() throws Exception {
        testDeleteBasic(null);
    }

    @Test
    public void testDeleteBasic2() throws Exception {
        testDeleteBasic(Transaction.BOGUS);
    }

    @Test
    public void testDeleteBasic3() throws Exception {
        testDeleteBasic(mDb.newTransaction());
    }

    private void testDeleteBasic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.delete(txn, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertFalse(ix.delete(txn, key));
        assertTrue(ix.insert(txn, key, value));
        assertNull(ix.load(txn, key2));

        assertTrue(ix.delete(txn, key));
        assertNull(ix.load(txn, key));
        assertNull(ix.load(txn, key2));

        assertFalse(ix.delete(txn, key));
        assertFalse(ix.delete(txn, key2));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.delete(txn, key));
            txn.exit();
            // Bogus transaction artifact. Undo log entry was created earler.
            assertNull(ix.load(txn, key));

            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.delete(txn, key));
            txn.exit();
            assertArrayEquals(value, ix.load(txn, key));
        }
    }

    @Test
    public void testUpdateSplit() throws Exception {
        View ix = openIndex("test");

        // Fill with ordered entries to create filled nodes.
        byte[] key = new byte[4];
        byte[] value = "small".getBytes();
        for (int i=0; i<1000; i++) {
            Utils.encodeIntBE(key, 0, i);
            ix.store(Transaction.BOGUS, key, value);
        }

        // Update with larger values, forcing nodes to split.
        value = "value is much bigger now".getBytes();
        for (int i=0; i<1000; i++) {
            Utils.encodeIntBE(key, 0, i);
            ix.store(Transaction.BOGUS, key, value);
            assertTrue(verify(ix));
        }
    }

    @Test
    public void testFill() throws Exception {
        View ix = openIndex("test");
        testFill(ix, 10);
        testFill(ix, 100);
        testFill(ix, 1000);
        testFill(ix, 10000);
        testFill(ix, 100000);
    }

    private void testFill(final View ix, final int count) throws Exception {
        final long seed1 = 1860635281L + count;
        final long seed2 = 2860635281L + count;

        // Insert random entries and verify.

        LHashTable.Int skipped = new LHashTable.Int(10);
        Random rnd = new Random(seed1);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 1, 100);
            byte[] value = randomStr(rnd, 1, 100);
            byte[] existing = ix.load(Transaction.BOGUS, key);
            boolean result = ix.insert(Transaction.BOGUS, key, value);
            if (existing == null) {
                assertTrue(result);
            } else {
                assertFalse(result);
                skipped.replace(i);
            }
        }

        assertTrue(verify(ix));

        rnd = new Random(seed1);

        int expectedCount = 0;
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 1, 100);
            byte[] value = randomStr(rnd, 1, 100);
            byte[] existing = ix.load(Transaction.BOGUS, key);
            if (skipped.get(i) == null) {
                assertArrayEquals(value, existing);
                expectedCount++;
            }
        }

        assertEquals(expectedCount, count(ix));

        // Replace random entries and verify.

        Map<byte[], byte[]> replaced = new TreeMap<byte[], byte[]>(KeyComparator.THE);
        rnd = new Random(seed2);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 1, 100);
            byte[] value = randomStr(rnd, 1, 100);
            byte[] existing = ix.load(Transaction.BOGUS, key);
            boolean result = ix.replace(Transaction.BOGUS, key, value);
            if (existing != null) {
                assertTrue(result);
                replaced.put(key, value);
            } else {
                assertFalse(result);
            }
        }

        assertTrue(verify(ix));

        rnd = new Random(seed2);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 1, 100);
            byte[] value = randomStr(rnd, 1, 100);
            byte[] existing = ix.load(Transaction.BOGUS, key);
            byte[] newValue = replaced.get(key);
            if (newValue != null) {
                assertArrayEquals(newValue, existing);
            }
        }

        assertEquals(expectedCount, count(ix));

        // Remove random entries and verify.

        rnd = new Random(seed1);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 1, 100);
            byte[] value = randomStr(rnd, 1, 100);
            if (skipped.get(i) == null && !replaced.containsKey(key)) {
                byte[] existing = ix.load(Transaction.BOGUS, key);
                assertTrue(ix.remove(Transaction.BOGUS, key, existing));
                expectedCount--;
            }
        }

        assertTrue(verify(ix));

        assertEquals(expectedCount, count(ix));

        // Delete all remaining entries and verify.

        for (Map.Entry<byte[], byte[]> e : replaced.entrySet()) {
            assertTrue(ix.delete(Transaction.BOGUS, e.getKey()));
        }

        assertTrue(verify(ix));

        assertEquals(0, count(ix));
    }

    static long count(View ix) throws Exception {
        long count0 = ix.count(null, null);

        long count1 = 0;
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.autoload(false);
        for (c.first(); c.key() != null; c.next()) {
            count1++;
        }

        assertEquals(count0, count1);

        long count2 = 0;
        for (c.last(); c.key() != null; c.previous()) {
            count2++;
        }

        assertEquals(count0, count2);

        return count0;
    }
}
