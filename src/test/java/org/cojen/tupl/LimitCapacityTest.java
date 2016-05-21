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
public class LimitCapacityTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LimitCapacityTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig().checkpointSizeThreshold(0);
        mConfig.directPageAccess(false);
        mDb = newTempDatabase(mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void basic() throws Exception {
        mDb.capacityLimit(100_000_000L);
        Index ix = mDb.openIndex("test");

        byte[] key = new byte[6];
        byte[] value = new byte[0];

        Cursor fill = ix.newCursor(Transaction.BOGUS);

        for (int i=0; i<50_000_000; i++) {
            Utils.encodeInt48BE(key, 0, i);
            fill.findNearby(key);

            try {
                fill.store(value);
            } catch (DatabaseFullException e) {
                trim(mDb, ix);
                continue;
            }
        }

        mDb.compactFile(null, 0.95);

        Database.Stats stats = mDb.stats();
        long size = stats.totalPages() * stats.pageSize();

        assertTrue(size < mDb.capacityLimit());
    }

    private static void trim(Database db, Index ix) throws Exception {
        db.capacityLimitOverride(-1);
        try {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            try {
                c.autoload(false);
                long count = 0;
                for (c.first(); c.key() != null; c.next()) {
                    c.store(null);
                    count++;
                    if (count >= 10_000_000) {
                        break;
                    }
                }
            } finally {
                c.reset();
            }
        } finally {
            db.capacityLimitOverride(0);
        }
    }

    @Test
    public void fragmentMess() throws Exception {
        // Test with direct pointers.
        fragmentMess(1_000_000, 100);
    }

    @Test
    public void fragmentMessInline() throws Exception {
        // Test with inline content.
        fragmentMess(100_000, 10);
    }

    @Test
    public void fragmentMessIndirect() throws Exception {
        // Test with indirect pointers.
        fragmentMess(4_000_000, 400);
    }

    private void fragmentMess(int size, int minFreed) throws Exception {
        mDb.capacityLimit(size);
        Index ix = mDb.openIndex("test");

        // Allocate the root node.
        ix.store(null, "hello".getBytes(), "world".getBytes());

        Database.Stats stats = mDb.stats();
        long total = stats.totalPages();

        // Value is too large.
        try {
            byte[] value = new byte[size];
            ix.store(null, "key".getBytes(), value);
            fail();
        } catch (DatabaseFullException e) {
            // Expected.
        }

        stats = mDb.stats();
        long delta = stats.totalPages() - total;
        assertTrue(delta >= minFreed);
        assertEquals(delta, stats.freePages());

        assertEquals(null, ix.load(null, "key".getBytes()));

        // Smaller value should work.
        byte[] value = new byte[size / 2];
        ix.store(null, "key".getBytes(), value);
    }

    @Test
    public void fragmentRecovery() throws Exception {
        // Tests that large value is in the redo log, even though it's not in the database.
        // The opposite behavior is worse -- in the database but not in the redo log. This can
        // happen if writing to the redo log fails. This is why redo is written first.

        mDb.capacityLimit(1_000_000L);
        Index ix = mDb.openIndex("test");

        byte[] value = randomStr(new java.util.Random(), 1_000_000);

        try {
            ix.store(null, "key".getBytes(), value);
            fail();
        } catch (DatabaseFullException e) {
            // Expected.
        }

        // Reopen without capacity limit.
        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");

        // Value was re-inserted.
        fastAssertArrayEquals(value, ix.load(null, "key".getBytes()));
    }

    @Test
    public void fragmentRollback() throws Exception {
        // Tests that large value is fully rolled back when an explicit transaction is used.

        mDb.capacityLimit(1_000_000L);
        Index ix = mDb.openIndex("test");

        byte[] value = randomStr(new java.util.Random(), 1_000_000);

        Transaction txn = mDb.newTransaction();
        try {
            ix.store(txn, "key".getBytes(), value);
            fail();
        } catch (DatabaseFullException e) {
            // Expected.
        } finally {
            txn.reset();
        }

        // Reopen without capacity limit.
        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");

        // Value is still gone.
        assertEquals(null, ix.load(null, "key".getBytes()));
    }
}
