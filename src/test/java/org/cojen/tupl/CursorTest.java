/*
 *  Copyright 2012-2013 Brian S O'Neill
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
public class CursorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorTest.class.getName());
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

    protected Database mDb;

    @Test
    public void empty() throws Exception {
        Index ix = mDb.openIndex("test");
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.first();
        assertNull(c.key());
        assertNull(c.value());
        c.last();
        assertNull(c.key());
        assertNull(c.value());

        for (int i=0; i<8; i++) {
            ix.store(Transaction.BOGUS, key(1), value(1));

            if ((i & 4) == 0) {
                c.first();
            } else {
                c.last();
            }
            fastAssertArrayEquals(key(1), c.key());
            fastAssertArrayEquals(value(1), c.value());

            ix.delete(Transaction.BOGUS, key(1));

            switch (i & 3) {
            default:
                c.first();
                break;
            case 1:
                c.last();
                break;
            case 2:
                c.next();
                break;
            case 3:
                c.previous();
                break;
            }

            assertNull(c.key());
            assertNull(c.value());
        }
    }

    @Test
    public void stubCursor() throws Exception {
        stubCursor(false);
    }

    @Test
    public void stubEviction() throws Exception {
        stubCursor(true);
    }

    private void stubCursor(boolean eviction) throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=0; i<1000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.find(key(500));
        assertNotNull(c.key());

        for (int i=0; i<1000; i++) {
            if (i != 500) {
                ix.delete(Transaction.BOGUS, key(i));
            }
        }

        // Cursor is still valid, and it references a stub parent.
        c.load();
        assertArrayEquals(key(500), c.key());
        assertArrayEquals(value(500), c.value());

        Cursor c2 = ix.newCursor(Transaction.BOGUS);
        c2.first();
        assertArrayEquals(key(500), c2.key());
        c2.next();
        assertNull(c2.key());
        c2.last();
        assertArrayEquals(key(500), c2.key());

        if (eviction) {
            // Force eviction of stub. Cannot verify directly, however.
            Index ix2 = mDb.openIndex("test2");
            c.reset();
            c2.reset();

            for (int i=0; i<1000000; i++) {
                ix2.store(Transaction.BOGUS, key(i), value(i));
            }

            c.find(key(500));
            assertArrayEquals(key(500), c.key());

            c2.last();
            assertArrayEquals(key(500), c2.key());
        }

        // Add back missing values, cursors should see them.
        for (int i=0; i<1000; i++) {
            if (i != 500) {
                ix.store(Transaction.BOGUS, key(i), value(i));
            }
        }

        for (int i=501; i<1000; i++) {
            c.next();
            assertArrayEquals(key(i), c.key());
            assertArrayEquals(value(i), c.value());
        }
        c.next();
        assertNull(c.key());

        for (int i=499; i>=0; i--) {
            c2.previous();
            assertArrayEquals(key(i), c2.key());
            assertArrayEquals(value(i), c2.value());
        }
        c2.previous();
        assertNull(c2.key());
    }

    @Test
    public void findLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        try {
            c.find(key(0));
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(0), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(0), c.value());

        c.reset();
    }

    @Test
    public void findGeLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        try {
            c.findGe(key(0));
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(0), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(0), c.value());

        c.reset();
    }

    @Test
    public void findGtLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        c.findGt(key(0));

        // Lock on key 0 didn't block the find.
        fastAssertArrayEquals(key(1), c.key());
        fastAssertArrayEquals(value(1), c.value());
    }

    @Test
    public void findLeLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        try {
            c.findLe(key(0));
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(0), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(0), c.value());

        c.reset();
    }

    @Test
    public void findLtLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 1.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(1), value(1));

        Cursor c = ix.newCursor(null);
        c.findLt(key(1));

        // Lock on key 0 didn't block the find.
        fastAssertArrayEquals(key(0), c.key());
        fastAssertArrayEquals(value(0), c.value());
    }

    @Test
    public void nextLock() throws Exception {
        nextLock(false);
    }

    @Test
    public void lastLock() throws Exception {
        nextLock(true);
    }

    private void nextLock(boolean last) throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 1.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(1), value(1));

        Cursor c = ix.newCursor(null);
        c.first();
        try {
            if (last) {
                c.last();
            } else {
                c.next();
            }
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(1), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(1), c.value());

        c.reset();
    }

    @Test
    public void previousLock() throws Exception {
        previousLock(false);
    }

    @Test
    public void firstLock() throws Exception {
        previousLock(true);
    }

    public void previousLock(boolean first) throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        c.last();
        try {
            if (first) {
                c.first();
            } else {
                c.previous();
            }
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(0), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(0), c.value());

        c.reset();
    }

    @Test
    public void nextLe() throws Exception {
        nextLe(3);
        nextLe(4);
    }

    private void nextLe(int count) throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<count; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.first();
        fastAssertArrayEquals(key(0), c.key());

        byte[] end = key(2);
        
        c.nextLe(end);
        fastAssertArrayEquals(key(1), c.key());
        fastAssertArrayEquals(value(1), c.value());

        c.nextLe(end);
        fastAssertArrayEquals(key(2), c.key());
        fastAssertArrayEquals(value(2), c.value());

        c.nextLe(end);
        assertNull(c.key());
        assertNull(c.value());
    }

    @Test
    public void nextLeLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 1.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(1), value(1));

        Cursor c = ix.newCursor(null);
        c.first();
        try {
            c.nextLe(key(1));
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(1), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(1), c.value());

        c.reset();
    }

    @Test
    public void nextLt() throws Exception {
        nextLt(3);
        nextLt(4);
    }

    private void nextLt(int count) throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<4; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.first();
        fastAssertArrayEquals(key(0), c.key());

        byte[] end = key(2);
        
        c.nextLt(end);
        fastAssertArrayEquals(key(1), c.key());
        fastAssertArrayEquals(value(1), c.value());

        c.nextLt(end);
        assertNull(c.key());
        assertNull(c.value());
    }

    @Test
    public void nextLtLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));
        ix.store(Transaction.BOGUS, key(2), value(2));

        // Lock key 2.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(2), value(2));

        Cursor c = ix.newCursor(null);
        c.first();

        fastAssertArrayEquals(key(0), c.key());
        fastAssertArrayEquals(value(0), c.value());

        c.nextLt(key(2));

        fastAssertArrayEquals(key(1), c.key());
        fastAssertArrayEquals(value(1), c.value());

        // Even though key 2 is locked, this doesn't block nextLt.
        c.nextLt(key(2));

        assertNull(c.key());
        assertNull(c.value());

        txn.exit();
    }

    @Test
    public void previousGe() throws Exception {
        previousGe(3);
        previousGe(4);
    }

    private void previousGe(int count) throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<count; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.last();
        fastAssertArrayEquals(key(count - 1), c.key());

        byte[] start = key(count - 3);
        
        c.previousGe(start);
        fastAssertArrayEquals(key(count - 2), c.key());
        fastAssertArrayEquals(value(count - 2), c.value());

        c.previousGe(start);
        fastAssertArrayEquals(key(count - 3), c.key());
        fastAssertArrayEquals(value(count - 3), c.value());

        c.previousGe(start);
        assertNull(c.key());
        assertNull(c.value());
    }

    @Test
    public void previousGeLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        c.last();
        try {
            c.previousGe(key(0));
            fail();
        } catch (LockTimeoutException e) {
        }

        txn.exit();

        // If timed out, cursor is at the desired key, but no value is available.
        fastAssertArrayEquals(key(0), c.key());
        assertTrue(c.value() == Cursor.NOT_LOADED);

        c.load();
        fastAssertArrayEquals(value(0), c.value());

        c.reset();
    }

    @Test
    public void previousGt() throws Exception {
        previousGt(3);
        previousGt(4);
    }

    private void previousGt(int count) throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<count; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.last();
        fastAssertArrayEquals(key(count - 1), c.key());

        byte[] start = key(count - 3);
        
        c.previousGt(start);
        fastAssertArrayEquals(key(count - 2), c.key());
        fastAssertArrayEquals(value(count - 2), c.value());

        c.previousGt(start);
        assertNull(c.key());
        assertNull(c.value());
    }

    @Test
    public void previousGtLock() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));
        ix.store(Transaction.BOGUS, key(2), value(2));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        c.last();

        fastAssertArrayEquals(key(2), c.key());
        fastAssertArrayEquals(value(2), c.value());

        c.previousGt(key(0));

        fastAssertArrayEquals(key(1), c.key());
        fastAssertArrayEquals(value(1), c.value());

        // Even though key 0 is locked, this doesn't block previousGt.
        c.previousGt(key(0));

        assertNull(c.key());
        assertNull(c.value());

        txn.exit();
    }

    @Test
    public void random() throws Exception {
        Index ix = mDb.openIndex("test");

        Cursor c = ix.newCursor(null);
        c.random(null, null);
        assertNull(c.key());
        assertNull(c.value());

        for (int i=0; i<10000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        int[] buckets = new int[10];

        c = ix.newCursor(null);
        for (int i=0; i<1000000; i++) {
            c.random(null, null);
            int key = Utils.decodeIntBE(c.key(), 0);
            buckets[key / 1000]++;
        }

        for (int bucket : buckets) {
            int diff = Math.abs(bucket - (1000000 / 10));
            // Allow 10% tolerance.
            assertTrue(diff < ((1000000 / 10) / 10));
        }
    }

    @Test
    public void randomNotGhost() throws Exception {
        // Verfies that ghosts are not selected.

        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));
        ix.store(Transaction.BOGUS, key(1), value(1));

        Transaction txn = mDb.newTransaction();
        ix.delete(txn, key(1));
        Cursor c = ix.newCursor(txn);
        for (int i=0; i<100; i++) {
            c.random(null, null);
            assertNotNull(c.value());
            assertTrue(c.value().length > 0);
        }

        txn.exit();
        ix.delete(txn, key(0));
        c = ix.newCursor(txn);
        for (int i=0; i<100; i++) {
            c.random(null, null);
            assertNotNull(c.value());
            assertTrue(c.value().length > 0);
        }
    }

    @Test
    public void randomRange() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        {
            int[] buckets = new int[10];

            Cursor c = ix.newCursor(null);
            byte[] lowKey = key(0);
            byte[] highKey = key(10000);
            for (int i=0; i<1000000; i++) {
                c.random(null, null);
                int key = Utils.decodeIntBE(c.key(), 0);
                buckets[key / 1000]++;
            }

            for (int bucket : buckets) {
                int diff = Math.abs(bucket - (1000000 / 10));
                // Allow 10% tolerance.
                assertTrue(diff < ((1000000 / 10) / 10));
            }
        }

        {
            int[] buckets = new int[10];

            Cursor c = ix.newCursor(null);
            byte[] lowKey = key(1000);
            byte[] highKey = key(9000);
            for (int i=0; i<10000000; i++) {
                c.random(lowKey, highKey);
                int key = Utils.decodeIntBE(c.key(), 0);
                assertTrue(key >= 1000);
                assertTrue(key < 9000);
                buckets[key / 1000]++;
            }

            for (int i=1; i<9; i++) {
                int diff = Math.abs(buckets[i] - (10000000 / 8));
                // Allow 10% tolerance, except at ends. Cannot expect perfect distribution
                // with range endpoints with a tree.
                int tolerance = (10000000 / 8) / 10;
                if (i == 1 || i == 8) {
                    tolerance *= 2;
                }
                assertTrue(diff < tolerance);
            }
        }
    }

    private byte[] key(int i) {
        byte[] key = new byte[4];
        Utils.encodeIntBE(key, 0, i);
        return key;
    }

    private byte[] value(int i) {
        return ("value-" + i).getBytes();
    }
}
