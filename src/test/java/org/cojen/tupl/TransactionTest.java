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
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TransactionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TransactionTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database newTempDatabase() throws Exception {
        return TestUtils.newTempDatabase(getClass());
    }

    protected Database mDb;

    @Test
    public void wrongTxn() throws Exception {
        Database db2 = newTempDatabase();
        Index ix = mDb.openIndex("test");
        Transaction txn = db2.newTransaction();
        try {
            ix.store(txn, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (IllegalArgumentException e) {
        }
        txn.commit();
    }

    @Test
    public void basicRollback() throws Exception {
        basicRollback(mDb.newTransaction());
        basicRollback(mDb.newTransaction(DurabilityMode.NO_REDO));
        basicRollback(mDb.newTransaction(DurabilityMode.NO_FLUSH));
        basicRollback(mDb.newTransaction(DurabilityMode.NO_SYNC));
        basicRollback(mDb.newTransaction(DurabilityMode.SYNC));
    }

    private void basicRollback(Transaction txn) throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "everyone".getBytes();

        Index ix = mDb.openIndex("test");
        assertTrue(ix.insert(txn, key, value1));
        txn.exit();

        LockMode mode = txn.lockMode();
        txn.lockMode(LockMode.REPEATABLE_READ);
        assertNull(ix.load(txn, key));
        try {
            assertTrue(ix.insert(txn, key, value1));
            fail();
        } catch (IllegalUpgradeException e) {
        }
        txn.lockMode(mode);
        try {
            assertTrue(ix.insert(txn, key, value1));
            fail();
        } catch (IllegalUpgradeException e) {
        }
        txn.exit();

        assertTrue(ix.insert(txn, key, value1));
        txn.exit();
        assertNull(ix.load(txn, key));

        assertTrue(ix.insert(txn, key, value1));
        txn.commit();

        assertTrue(ix.delete(txn, key));
        txn.exit();
        assertArrayEquals(value1, ix.load(txn, key));
        txn.exit();

        ix.store(txn, key, value2);
        txn.exit();
        assertArrayEquals(value1, ix.load(txn, key));

        txn.exit();

        ix.delete(txn, key);
        txn.commit();

        txn.exit();

        // Extra exit should be harmless.
        txn.exit();
    }

    @Test
    public void nestedRollback() throws Exception {
        nestedRollback(mDb.newTransaction());
        nestedRollback(mDb.newTransaction(DurabilityMode.NO_REDO));
        nestedRollback(mDb.newTransaction(DurabilityMode.NO_FLUSH));
        nestedRollback(mDb.newTransaction(DurabilityMode.NO_SYNC));
        nestedRollback(mDb.newTransaction(DurabilityMode.SYNC));
    }

    private void nestedRollback(Transaction txn) throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "everyone".getBytes();
        byte[] value3 = "nobody".getBytes();

        Index ix = mDb.openIndex("test");
        assertTrue(ix.insert(txn, key, value1));

        assertFalse(txn.isNested());
        assertEquals(0, txn.nestingLevel());

        txn.enter();

        assertTrue(txn.isNested());
        assertEquals(1, txn.nestingLevel());

        ix.store(txn, key, value2);
        txn.exit();
        assertFalse(txn.isNested());
        assertEquals(0, txn.nestingLevel());
        txn.enter();
        assertTrue(txn.isNested());
        assertEquals(1, txn.nestingLevel());
        assertArrayEquals(value1, ix.load(txn, key));

        ix.store(txn, key, value2);
        // Commit to outer scope.
        txn.commit();

        assertArrayEquals(value2, ix.load(txn, key));
        assertTrue(ix.delete(txn, key));
        txn.exit();
        txn.enter();
        assertArrayEquals(value2, ix.load(txn, key));
        txn.exit();
        txn.enter();

        assertArrayEquals(value2, ix.load(txn, key));
        ix.store(txn, key, value3);
        txn.exit();
        txn.enter();
        assertArrayEquals(value2, ix.load(txn, key));

        txn.exit();
        txn.enter();

        txn.enter();
        assertTrue(txn.isNested());
        assertEquals(2, txn.nestingLevel());
        ix.delete(txn, key);
        // Commit to second scope.
        txn.commit();
        assertNull(ix.load(txn, key));
        txn.exit();

        assertNull(ix.load(txn, key));
        txn.exit();

        assertArrayEquals(value2, ix.load(txn, key));

        txn.exit();
        assertNull(ix.load(txn, key));

        txn.exit();

        assertFalse(txn.isNested());
        assertEquals(0, txn.nestingLevel());
    }

    @Test
    public void ghost() throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] value = "value".getBytes();

        for (int i=0; i<10; i++) {
            ix.store(Transaction.BOGUS, ("key-" + i).getBytes(), value);
        }

        Transaction txn1 = mDb.newTransaction();
        byte[] key = "key-2".getBytes();
        ix.delete(txn1, key);

        Transaction txn2 = mDb.newTransaction();
        Cursor c = ix.newCursor(txn2);
        c.first();
        c.next();
        try {
            c.next();
            fail();
        } catch (LockTimeoutException e) {
            // Entry has been deleted, but lock remains.
        }

        // Deleted is skipped when no lock is requested.
        txn2.lockMode(LockMode.READ_UNCOMMITTED);
        c.next();
        assertArrayEquals(c.key(), "key-3".getBytes());
        c.previous();
        assertArrayEquals(c.key(), "key-1".getBytes());

        // Direct load is also locked out.
        Transaction txn3 = mDb.newTransaction();
        try {
            ix.load(txn3, "key-2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
        }

        txn3.lockMode(LockMode.READ_UNCOMMITTED);
        assertNull(ix.load(txn3, "key-2".getBytes()));
        txn3.exit();

        // Commit first transaction, and no lock is required for deleted entry.
        txn1.commit();
        txn1.exit();
        c.next();
        assertArrayEquals(c.key(), "key-3".getBytes());
        c.reset();
        txn2.reset();
    }

    @Test
    public void largeRollback() throws Exception {
        largeTxn(false);
    }

    @Test
    public void largeCommit() throws Exception {
        largeTxn(true);
    }

    private void largeTxn(boolean commit) throws Exception {
        Index ix1 = mDb.openIndex("test1");
        Index ix2 = mDb.openIndex("test2");

        final int seed = 8384712;
        Random rnd = new Random(seed);

        Transaction txn = mDb.newTransaction();
        for (int i=0; i<100000; i++) {
            {
                byte[] key = randomStr(rnd, 10, 100);
                byte[] value = randomStr(rnd, 10, 100);
                ix1.store(txn, key, value);
            }
            {
                byte[] key = randomStr(rnd, 10, 100);
                byte[] value = randomStr(rnd, 10, 100);
                ix2.store(txn, key, value);
            }
        }

        long count1 = CrudTest.count(ix1);
        long count2 = CrudTest.count(ix2);

        txn.enter();
        for (int i=0; i<100000; i++) {
            {
                byte[] key = randomStr(rnd, 100);
                byte[] value = randomStr(rnd, 100);
                ix1.store(txn, key, value);
            }
            {
                byte[] key = randomStr(rnd, 100);
                byte[] value = randomStr(rnd, 100);
                ix2.store(txn, key, value);
            }
        }
        if (commit) {
            txn.commit();
        }
        txn.exit();

        if (!commit) {
            assertEquals(count1, CrudTest.count(ix1));
            assertEquals(count2, CrudTest.count(ix2));
        }

        if (commit) {
            txn.commit();
        }
        txn.exit();

        if (!commit) {
            assertEquals(0, CrudTest.count(ix1));
            assertEquals(0, CrudTest.count(ix2));
        }

        if (commit) {
            rnd = new Random(seed);

            for (int i=0; i<100000; i++) {
                {
                    byte[] key = randomStr(rnd, 10, 100);
                    byte[] value = randomStr(rnd, 10, 100);
                    assertArrayEquals(value, ix1.load(null, key));
                }
                {
                    byte[] key = randomStr(rnd, 10, 100);
                    byte[] value = randomStr(rnd, 10, 100);
                    assertArrayEquals(value, ix2.load(null, key));
                }
            }

            for (int i=0; i<100000; i++) {
                {
                    byte[] key = randomStr(rnd, 100);
                    byte[] value = randomStr(rnd, 100);
                    assertArrayEquals(value, ix1.load(null, key));
                }
                {
                    byte[] key = randomStr(rnd, 100);
                    byte[] value = randomStr(rnd, 100);
                    assertArrayEquals(value, ix2.load(null, key));
                }
            }
        }
    }

    @Test
    public void storeCommit() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "everyone".getBytes();
        byte[] value3 = "everyone!!!".getBytes();

        Index ix = mDb.openIndex("test");

        {
            Transaction txn = mDb.newTransaction();

            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value1);
            c.reset();
            txn.reset();

            assertArrayEquals(value1, ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.store(value2);
            c.commit(value3);
            c.reset();
            txn.reset();

            assertArrayEquals(value3, ix.load(null, key));
        }

        // Delete...

        {
            Transaction txn = mDb.newTransaction();

            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(null);
            c.reset();
            txn.reset();

            assertNull(ix.load(null, key));
        }
    }

    @Test
    public void storeCommitNested() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "everyone".getBytes();
        byte[] value3 = "everyone!!!".getBytes();

        Index ix = mDb.openIndex("test");

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value1);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.reset(); // rollback

            assertNull(ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value1);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.exit();
            txn.commit();
            assertEquals(0, txn.nestingLevel());
            txn.reset();

            assertArrayEquals(value1, ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value2);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.reset(); // rollback

            assertArrayEquals(value1, ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            ix.store(txn, key, value2);
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value3);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.reset(); // rollback

            assertArrayEquals(value1, ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            ix.store(txn, key, value2);
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(value3);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.exit();
            txn.commit();
            assertEquals(0, txn.nestingLevel());
            txn.reset();

            assertArrayEquals(value3, ix.load(null, key));
        }

        // Delete...

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(null);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.reset(); // rollback

            assertArrayEquals(value3, ix.load(null, key));
        }

        {
            Transaction txn = mDb.newTransaction();

            txn.enter();
            Cursor c = ix.newCursor(txn);
            c.find(key);
            c.commit(null);
            c.reset();
            assertEquals(1, txn.nestingLevel());
            txn.exit();
            txn.commit();
            assertEquals(0, txn.nestingLevel());
            txn.reset();

            assertNull(ix.load(null, key));
        }
    }

    @Test
    public void bogusSettings() throws Exception {
        final Transaction txn = Transaction.BOGUS;

        // Nothing to commit, so safe.
        txn.commit();

        try {
            txn.lockMode(LockMode.UPGRADABLE_READ);
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            txn.lockTimeout(0, null);
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            txn.durabilityMode(DurabilityMode.SYNC);
            fail();
        } catch (IllegalStateException e) {
        }

        Index ix = mDb.openIndex("test");

        Cursor c = ix.newCursor(txn);
        c.find("test".getBytes());
        // Nothing to commit, so safe.
        c.commit("value".getBytes());

        try {
            txn.lockExclusive(ix.getId(), "test".getBytes());
            fail();
        } catch (IllegalStateException e) {
        }

        txn.exit();
    }
}
