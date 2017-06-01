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
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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

        mDb.suspendCheckpoints();

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

        mDb.resumeCheckpoints();
        mDb.checkpoint();

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
        mDb.suspendCheckpoints();
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
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");

        // Value is still gone.
        assertEquals(null, ix.load(null, "key".getBytes()));
    }

    @Test
    public void fragmentRollbackAutoCommit() throws Exception {
        // Tests that large value is fully rolled back for auto-commit transaction.

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
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");

        // Value is still gone.
        assertEquals(null, ix.load(null, "key".getBytes()));
    }
}
