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

import java.util.Random;
import java.util.concurrent.TimeUnit;

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

    protected View openIndex(String name) throws Exception {
        return mDb.openIndex(name);
    }

    protected boolean verify(View ix) throws Exception {
        return ((Index) ix).verify(null);
    }

    protected TreeCursor treeCursor(Cursor c) {
        return (TreeCursor) c;
    }

    protected Database mDb;

    @Test
    public void empty() throws Exception {
        View ix = openIndex("test");
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
        View ix = openIndex("test");

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
            View ix2 = openIndex("test2");
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
        View ix = openIndex("test");
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
    public void loadLock() throws Exception {
        View ix = openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));

        Cursor c = ix.newCursor(null);
        c.find(key(0));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        try {
            c.load();
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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

    private void previousLock(boolean first) throws Exception {
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
        View ix = openIndex("test");
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
    public void bigSkip() throws Exception {
        View ix = openIndex("skippy");

        for (int i=0; i<1_000_000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(1));
        }

        // Force leaf pages to be clean for accumulating counts.
        mDb.checkpoint();

        Cursor c = ix.newCursor(null);

        // Force counts to be generated.
        c.first();
        c.skip(10_000);
        fastAssertArrayEquals(key(10_000), c.key());

        // Force counts to be persisted.
        mDb.checkpoint();

        c.first();
        c.skip(10_000);
        fastAssertArrayEquals(key(10_000), c.key());

        c.skip(900_000);
        fastAssertArrayEquals(key(910_000), c.key());

        // Force counts to be persisted.
        mDb.checkpoint();

        c.last();
        c.skip(-9_999);
        fastAssertArrayEquals(key(990_000), c.key());

        c.skip(-980_000);
        fastAssertArrayEquals(key(10_000), c.key());

        c.skip(Long.MIN_VALUE);
        assertNull(c.key());

        try {
            c.skip(Long.MAX_VALUE);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void bigSkipBounded() throws Exception {
        View ix = openIndex("skippy");

        for (int i=0; i<1_000_000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(1));
        }

        Cursor c = ix.newCursor(null);

        c.first();
        c.skip(100_000, key(50_000), false);
        assertNull(c.key());

        c.first();
        c.skip(100_000, key(50_000), true);
        assertNull(c.key());

        c.first();
        c.skip(100_000, key(99_999), false);
        assertNull(c.key());

        c.first();
        c.skip(100_000, key(99_999), true);
        assertNull(c.key());

        c.first();
        c.skip(100_000, key(100_000), false);
        assertNull(c.key());

        c.first();
        c.skip(100_000, key(100_000), true);
        fastAssertArrayEquals(key(100_000), c.key());

        c.first();
        c.skip(100_000, key(100_001), false);
        fastAssertArrayEquals(key(100_000), c.key());

        c.first();
        c.skip(100_000, key(100_001), true);
        fastAssertArrayEquals(key(100_000), c.key());

        // In reverse.

        c.last();
        c.skip(-100_000, key(999_999 - 50_000), false);
        assertNull(c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 50_000), true);
        assertNull(c.key());
        
        c.last();
        c.skip(-100_000, key(999_999 - 99_999), false);
        assertNull(c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 99_999), true);
        assertNull(c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 100_000), false);
        assertNull(c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 100_000), true);
        fastAssertArrayEquals(key(999_999 - 100_000), c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 100_001), false);
        fastAssertArrayEquals(key(999_999 - 100_000), c.key());

        c.last();
        c.skip(-100_000, key(999_999 - 100_001), true);
        fastAssertArrayEquals(key(999_999 - 100_000), c.key());

        c.reset();
    }

    @Test
    public void randomLock() throws Exception {
        View ix = openIndex("test");
        ix.store(Transaction.BOGUS, key(0), value(0));

        // Lock key 0.
        Transaction txn = mDb.newTransaction();
        ix.store(txn, key(0), value(0));

        Cursor c = ix.newCursor(null);
        try {
            c.random(key(0), key(1));
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
    public void findNearby() throws Exception {
        View ix = openIndex("test");

        final int count = 3000;
        final int seed = 3892476;
        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 500);
            ix.store(Transaction.BOGUS, key, key);
        }

        // Find every key using each key as a starting point.

        Cursor c1 = ix.newCursor(Transaction.BOGUS);
        for (c1.first(); c1.key() != null; c1.next()) {
            TreeCursor c2 = treeCursor(ix.newCursor(Transaction.BOGUS));
            for (c2.first(); c2.key() != null; c2.next()) {
                TreeCursor ref = treeCursor(c1.copy());
                ref.findNearby(c2.key());
                assertTrue(ref.equalPositions(c2));
                ref.reset();
            }
            c2.reset();
        }
        c1.reset();
    }

    @Test
    public void storeNearby() throws Exception {
        View ix = openIndex("test");

        final int count = 3000;
        final int seed = 3892476;
        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 500);
            ix.store(Transaction.BOGUS, key, key);
        }

        // For every key, store a new key directly lower and higher.

        Cursor scan = ix.newCursor(Transaction.BOGUS);
        for (scan.first(); scan.key() != null; ) {
            Cursor low = scan.copy();
            byte[] key = low.key().clone();
            Utils.decrement(key, 0, key.length);
            low.findNearby(key);
            low.store(key);
            low.reset();

            Cursor high = scan.copy();
            // Scan past new key.
            scan.next();
            key = high.key().clone();
            Utils.increment(key, 0, key.length);
            high.findNearby(key);
            high.store(key);
            high.reset();
        }
        scan.reset();

        assertTrue(verify(ix));

        // Verify that old and new keys exist.

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 500);
            fastAssertArrayEquals(key, ix.load(Transaction.BOGUS, key));

            byte[] low = key.clone();
            Utils.decrement(low, 0, low.length);
            fastAssertArrayEquals(low, ix.load(Transaction.BOGUS, low));

            byte[] high = key.clone();
            Utils.increment(high, 0, high.length);
            fastAssertArrayEquals(high, ix.load(Transaction.BOGUS, high));
        }

        verifyExtremities(ix);

        // Delete all and verify extremities.

        int removed = 0;
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.autoload(false);
        for (c.first(); c.key() != null; c.next()) {
            c.store(null);
            removed++;
            if (removed % 10 == 0) {
                verifyExtremities(ix);
                assertTrue(verify(ix));
            }
        }
        c.reset();

        c.first();
        assertNull(c.key());

        verifyExtremities(ix);
        assertTrue(verify(ix));

        // Ordered fill and verify.

        byte[] key = new byte[4];
        Cursor fill = ix.newCursor(Transaction.BOGUS);
        for (int i=0; i<10000; i++) {
            fill.findNearby(key);
            fill.store(key);
            key = key.clone();
            Utils.increment(key, 0, key.length);
        }
        fill.reset();

        verifyExtremities(ix);
        assertTrue(verify(ix));
    }

    protected void verifyExtremities(View ix) throws Exception {
        TreeCursor extremity = treeCursor(ix.newCursor(Transaction.BOGUS));
        assertTrue(extremity.verifyExtremities(Node.LOW_EXTREMITY));
        assertTrue(extremity.verifyExtremities(Node.HIGH_EXTREMITY));
    }

    @Test
    public void random() throws Exception {
        View ix = openIndex("test");

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

        View ix = openIndex("test");
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
        View ix = openIndex("test");
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

    @Test
    public void randomNonRange() throws Exception {
        View ix = openIndex("test");

        Cursor c = ix.newCursor(null);

        byte[] low = {10};
        byte[] high = {20};

        c.random(low, low);
        assertNull(c.key());

        c.random(high, low);
        assertNull(c.key());

        ix.store(Transaction.BOGUS, low, low);
        ix.store(Transaction.BOGUS, high, high);

        c.random(low, low);
        assertNull(c.key());

        c.random(high, low);
        assertNull(c.key());

        for (int i=1; i<=20000; i+=2) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        c.random(low, low);
        assertNull(c.key());
    
        c.random(new byte[] {0, 0, 0, 2}, new byte[] {0, 0, 0, 3});
        assertNull(c.key());

        c.random(null, new byte[] {0, 0, 0, 0});
        assertNull(c.key());
    }

    @Test
    public void lockNoLoad() throws Exception {
        lockNoLoad(false, false);
        lockNoLoad(false, true);
        lockNoLoad(true, false);
        lockNoLoad(true, true);
    }

    private void lockNoLoad(boolean ghost, boolean ge) throws Exception {
        View ix = openIndex("test");

        for (Cursor c = ix.newCursor(null); c.key() != null; c.next()) {
            c.store(null);
        }

        byte[] key1 = "hello".getBytes();
        byte[] key2 = "hello!".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "world!".getBytes();

        byte[] key = key1;
        byte[] value;

        if (ghost) {
            ix.store(null, key1, value1);
            value = null;
        } else {
            value = value1;
        }

        ix.store(null, key2, value2);

        Thread t = new Thread(() -> {
            try {
                Transaction txn = mDb.newTransaction();
                try {
                    ix.store(txn, key, value);
                    Thread.sleep(1000);
                    txn.commit();
                } finally {
                    txn.reset();
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        });

        t.start();

        // Wait for thread to lock the key.
        while (t.isAlive()) {
            Transaction txn = mDb.newTransaction();
            try {
                txn.lockTimeout(1, TimeUnit.MILLISECONDS);
                try {
                    ix.load(txn, key);
                } catch (LockTimeoutException e) {
                    // Locked.
                    break;
                }
            } finally {
                txn.reset();
            }
        }

        Transaction txn = mDb.newTransaction();
        try {
            txn.lockTimeout(10, TimeUnit.SECONDS);

            Cursor c = ix.newCursor(txn);
            try {
                c.autoload(false);

                // Even if the lock is not immediately available, the value shouldn't be loaded.
                // Ghost detection still works, as some operations depend on this behavior.

                if (ge) {
                    c.findGe(key);
                    if (ghost) {
                        assertArrayEquals(key2, c.key());
                    } else {
                        assertArrayEquals(key1, c.key());
                    }
                    assertTrue(c.value() == Cursor.NOT_LOADED);
                } else {
                    c.find(key);
                    assertArrayEquals(key1, c.key());
                    if (ghost) {
                        assertNull(c.value());
                    } else {
                        assertTrue(c.value() == Cursor.NOT_LOADED);
                    }
                }
            } finally {
                c.reset();
            }
        } finally {
            txn.reset();
        }

        t.join();
    }

    @Test
    public void stability() throws Exception {
        // Verifies that cursors are positioned properly after making structural modifications
        // to the tree.

        View ix = openIndex("test");

        Random rnd = new Random(793846);
        byte[] value = new byte[0];

        final int count = 100000;
        Cursor[] foundCursors = new Cursor[count];
        Cursor[] notFoundCursors = new Cursor[count];

        for (int i=0; i<count; i++) {
            byte[] key = key(rnd.nextInt());

            Cursor c = ix.newCursor(Transaction.BOGUS);
            c.find(key);
            c.store(value);
            foundCursors[i] = c;

            c = ix.newCursor(Transaction.BOGUS);
            key = key(rnd.nextInt());
            c.find(key);
            notFoundCursors[i] = c;
        }

        assertTrue(verify(ix));

        assertEquals(foundCursors.length + notFoundCursors.length, mDb.stats().cursorCount());

        verifyPositions(ix, foundCursors);
        verifyPositions(ix, notFoundCursors);
    }

    @Test
    public void stability2() throws Exception {
        // Checks cursor stability while inserting records in descending order. This should
        // exercise internal node rebalancing.
        
        View ix = openIndex("test");

        byte[] value = new byte[200];

        final int count = 10000;
        Cursor[] cursors = new Cursor[count];

        for (int i=count; --i>=0; ) {
            byte[] key = key(i);

            Cursor c = ix.newCursor(Transaction.BOGUS);
            c.find(key);
            c.store(value);
            cursors[i] = c;
        }

        assertTrue(verify(ix));

        verifyPositions(ix, cursors);
    }

    @Test
    public void stability3() throws Exception {
        // Checks cursor stability while inserting records in ascending order. This should
        // exercise internal node rebalancing.
        
        View ix = openIndex("test");

        byte[] value = new byte[200];

        final int count = 10000;
        Cursor[] cursors = new Cursor[count];

        for (int i=0; i<count; i++) {
            byte[] key = key(i);

            Cursor c = ix.newCursor(Transaction.BOGUS);
            c.find(key);
            c.store(value);
            cursors[i] = c;
        }

        assertTrue(verify(ix));

        verifyPositions(ix, cursors);
    }

    protected void verifyPositions(View ix, Cursor[] cursors) throws Exception {
        for (Cursor existing : cursors) {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            byte[] key = existing.key();
            c.find(key);
            assertTrue(treeCursor(c).equalPositions(treeCursor(existing)));
            c.reset();
            existing.reset();
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
