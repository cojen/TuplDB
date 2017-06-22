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

        try {
            ix.exists(txn, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        assertNull(ix.load(txn, new byte[0]));
        assertNull(ix.load(txn, "key".getBytes()));
        assertFalse(ix.exists(txn, new byte[0]));
        assertFalse(ix.exists(txn, "key".getBytes()));
    }

    @Test
    public void existsLockWait() throws Exception {
        View ix = openIndex("test");

        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();

        ix.store(null, k2, "v2".getBytes());

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                Transaction txn = mDb.newTransaction();
                try {
                    ix.lockExclusive(txn, k1);
                    ix.store(txn, k2, null);
                    Thread.sleep(60_000);
                } finally {
                    txn.exit();
                }
            } catch (InterruptedException e) {
                // Expected.
            } catch (Exception e) {
                Utils.uncaught(e);
            }
        }));

        try {
            ix.exists(null, k1);
            fail();
        } catch (LockTimeoutException e) {
        }

        try {
            ix.exists(null, k2);
            fail();
        } catch (LockTimeoutException e) {
        }

        t.interrupt();
        t.join();

        assertTrue(ix.exists(null, k2));
        assertFalse(ix.exists(null, k1));
    }

    @Test
    public void testTouch() throws Exception {
        View ix = openIndex("test");

        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();

        ix.store(null, k2, "v2".getBytes());

        assertEquals(LockResult.UNOWNED, ix.touch(null, k1));
        assertEquals(LockResult.UNOWNED, ix.touch(null, k2));

        {
            LockMode[] modes = {
                LockMode.UNSAFE, LockMode.READ_UNCOMMITTED, LockMode.READ_COMMITTED
            };

            for (LockMode mode : modes) {
                Transaction txn = mDb.newTransaction();
                txn.lockMode(mode);
                assertEquals(LockResult.UNOWNED, ix.touch(txn, k1));
                assertEquals(LockResult.UNOWNED, ix.lockCheck(txn, k1));
                assertEquals(LockResult.UNOWNED, ix.touch(txn, k2));
                assertEquals(LockResult.UNOWNED, ix.lockCheck(txn, k2));
                txn.reset();
            }
        }

        {
            LockMode[] modes = {
                LockMode.REPEATABLE_READ, LockMode.UPGRADABLE_READ
            };

            for (LockMode mode : modes) {
                LockResult owned = LockResult.OWNED_SHARED;
                if (mode == LockMode.UPGRADABLE_READ) {
                    owned = LockResult.OWNED_UPGRADABLE;
                }

                Transaction txn = mDb.newTransaction();
                txn.lockMode(mode);
                assertEquals(LockResult.ACQUIRED, ix.touch(txn, k1));
                assertEquals(owned, ix.lockCheck(txn, k1));
                assertEquals(LockResult.ACQUIRED, ix.touch(txn, k2));
                assertEquals(owned, ix.lockCheck(txn, k2));
                txn.reset();
            }
        }
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
        assertTrue(ix.exists(txn, key));

        ix.store(txn, key, value2);
        assertArrayEquals(value2, ix.load(txn, key));
        assertTrue(ix.exists(txn, key));

        assertNull(ix.load(txn, key2));
        assertFalse(ix.exists(txn, key2));

        ix.store(txn, key, null);
        assertNull(ix.load(txn, key));
        assertFalse(ix.exists(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(txn, key, value);
            txn.commit();
            assertArrayEquals(value, ix.load(txn, key));
            assertTrue(ix.exists(txn, key));

            ix.store(txn, key, value2);
            txn.commit();
            assertArrayEquals(value2, ix.load(txn, key));
            assertTrue(ix.exists(txn, key));

            ix.store(txn, key, value);
            txn.exit();
            assertArrayEquals(value2, ix.load(txn, key));
            assertTrue(ix.exists(txn, key));
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
    public void testUpdate1Basic1() throws Exception {
        testUpdate1Basic(null);
    }

    @Test
    public void testUpdate1Basic2() throws Exception {
        testUpdate1Basic(Transaction.BOGUS);
    }

    @Test
    public void testUpdate1Basic3() throws Exception {
        testUpdate1Basic(mDb.newTransaction());
    }

    private void testUpdate1Basic(Transaction txn) throws Exception {
        View ix = openIndex("test");

        try {
            ix.update(txn, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();
        byte[] value = "everyone".getBytes();
        byte[] value2 = "world".getBytes();

        assertTrue(ix.update(txn, key, value));
        assertArrayEquals(value, ix.load(txn, key));

        assertTrue(ix.update(txn, key, value2));
        assertArrayEquals(value2, ix.load(txn, key));

        assertFalse(ix.update(txn, key, value2));
        assertArrayEquals(value2, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        assertTrue(ix.update(txn, key, null));
        assertNull(ix.load(txn, key));

        assertFalse(ix.update(txn, key, null));
        assertNull(ix.load(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.update(txn, key, value2));
            txn.exit();
            // Bogus transaction artifact. Undo log entry was created earler.
            assertNull(ix.load(txn, key));

            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.update(txn, key, value2));
            txn.exit();
            assertArrayEquals(value, ix.load(txn, key));
        }
    }

    @Test
    public void testUpdate2Basic1() throws Exception {
        testUpdate2Basic(null);
    }

    @Test
    public void testUpdate2Basic2() throws Exception {
        testUpdate2Basic(Transaction.BOGUS);
    }

    @Test
    public void testUpdate2Basic3() throws Exception {
        testUpdate2Basic(mDb.newTransaction());
    }

    private void testUpdate2Basic(Transaction txn) throws Exception {
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
