/*
 *  Copyright 2012 Brian S O'Neill
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
        mDb = TestUtils.newTempDatabase();
    }

    @After
    public void teardown() {
        TestUtils.deleteTempDatabases();
    }

    private Database mDb;

    @Test
    public void loadNothing() throws Exception {
        loadNothing(null);
        loadNothing(Transaction.BOGUS);
        loadNothing(mDb.newTransaction());
    }

    private void loadNothing(Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");

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
        Index ix = mDb.openIndex("test");

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
        Index ix = mDb.openIndex("test");

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
        Index ix = mDb.openIndex("test");

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
            // Bogus transaction artifact. Undo log entry created earler.
            assertNull(ix.load(txn, key));

            ix.store(Transaction.BOGUS, key, value);
            assertTrue(ix.replace(txn, key, value2));
            txn.exit();
            assertArrayEquals(value, ix.load(txn, key));
        }
    }
}
