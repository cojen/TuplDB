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

import java.io.File;

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
public class RecoverTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RecoverTest.class.getName());
    }

    protected void decorate(DatabaseConfig config) throws Exception {
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(mConfig);
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void interruptOnClose() throws Exception {
        final byte[] key = "hello".getBytes();
        final byte[] value = "world".getBytes();

        final Index ix = mDb.openIndex("test");
        Transaction txn1 = mDb.newTransaction();
        ix.store(txn1, key, value);

        class Waiter implements Runnable {
            volatile Throwable ex;

            public void run() {
                try {
                    Transaction txn2 = mDb.newTransaction();
                    txn2.lockTimeout(60, TimeUnit.SECONDS);
                    assertEquals(null, ix.load(txn2, key));
                } catch (Throwable e) {
                    ex = e;
                }
            }
        };

        Waiter w = new Waiter();
        Thread t = new Thread(w);
        startAndWaitUntilBlocked(t);

        Thread.sleep(1000);
        mDb.close();

        t.join();

        assertTrue(w.ex instanceof LockInterruptedException);

        // Any exception should be suppressed.
        txn1.exit();

        Transaction txn2 = mDb.newTransaction();
        try {
            assertEquals(null, ix.load(txn2, key));
            fail();
        } catch (LockTimeoutException e) {
            // Still held as a result of stuck txn1.
        }
    }

    @Test
    public void txnBogus() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        Index ix = mDb.openIndex("test");
        ix.store(Transaction.BOGUS, key, value);

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertNull(ix.load(Transaction.BOGUS, key));
        ix.store(Transaction.BOGUS, key, value);
        mDb.checkpoint();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertArrayEquals(value, ix.load(Transaction.BOGUS, key));
    }

    @Test
    public void txnNoLog() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        Index ix = mDb.openIndex("test");
        Transaction txn = mDb.newTransaction(DurabilityMode.NO_REDO);
        ix.store(txn, key, value);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        txn = mDb.newTransaction(DurabilityMode.NO_REDO);
        assertNull(ix.load(txn, key));
        ix.store(txn, key, value);
        txn.commit();
        mDb.checkpoint();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertArrayEquals(value, ix.load(null, key));
    }

    @Test
    public void deleteGhost() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        Index ix = mDb.openIndex("test");

        Transaction txn = mDb.newTransaction();
        ix.store(txn, key, value);
        txn.commit();
        mDb.checkpoint();

        txn = mDb.newTransaction();
        ix.store(txn, key, null);

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertArrayEquals(value, ix.load(null, key));

        txn = mDb.newTransaction();
        ix.store(txn, key, null);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertEquals(null, ix.load(null, key));
    }

    @Test
    public void recoverDeleteGhost() throws Exception {
        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] value = "value".getBytes();

        Index ix = mDb.openIndex("test");

        Transaction txn = mDb.newTransaction();
        ix.store(txn, k2, value);
        txn.commit();

        txn = mDb.newTransaction();
        ix.store(txn, k2, null);
        mDb.checkpoint();
        ix.store(txn, k1, value);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");
        assertArrayEquals(value, ix.load(null, k1));
        assertEquals(null, ix.load(null, k2));
    }

    @Test
    public void scopeRollback1() throws Exception {
        scopeRollback(0, false);
    }

    @Test
    public void scopeRollback2() throws Exception {
        scopeRollback(1, false);
    }

    @Test
    public void scopeRollback3() throws Exception {
        scopeRollback(2, false);
    }

    @Test
    public void scopeRollback4() throws Exception {
        scopeRollback(3, false);
    }

    @Test
    public void scopeRollback5() throws Exception {
        scopeRollback(0, true);
    }

    @Test
    public void scopeRollback6() throws Exception {
        scopeRollback(1, true);
    }

    @Test
    public void scopeRollback7() throws Exception {
        scopeRollback(2, true);
    }

    @Test
    public void scopeRollback8() throws Exception {
        scopeRollback(3, true);
    }

    private void scopeRollback(int chkpnt, boolean commit) throws Exception {
        // Checkpoint forces intermediate log data to be flushed, which should
        // not interfere with recovery. Make sure outcome is same no matter
        // where the checkpoint occurs.

        Index ix = mDb.openIndex("test");

        Transaction txn = mDb.newTransaction(); {
            ix.store(txn, "a".getBytes(), "v1".getBytes());
            txn.enter(); {
                ix.store(txn, "b".getBytes(), "v2".getBytes());
                if (commit) {
                    txn.commit();
                }
                if ((chkpnt & 1) == 0) {
                    mDb.checkpoint();
                }
            } txn.exit();

            if ((chkpnt & 2) == 0) {
                mDb.checkpoint();
            }

            ix.store(txn, "c".getBytes(), "v3".getBytes());

            txn.commit();
        } txn.exit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        ix = mDb.openIndex("test");

        assertArrayEquals("v1".getBytes(), ix.load(null, "a".getBytes()));
        if (commit) {
            assertArrayEquals("v2".getBytes(), ix.load(null, "b".getBytes()));
        } else {
            assertEquals(null, ix.load(null, "b".getBytes()));
        }
        assertArrayEquals("v3".getBytes(), ix.load(null, "c".getBytes()));
    }

    @Test
    public void smallUndo() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10, false, false, chkpnt);
        }
    }

    @Test
    public void smallUndoExit() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10, false, true, chkpnt);
        }
    }

    @Test
    public void smallRedo() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10, true, false, chkpnt);
        }
    }

    @Test
    public void smallRedoExit() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10, true, true, chkpnt);
        }
    }

    @Test
    public void largeUndo() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10000, false, false, chkpnt);
        }
    }

    @Test
    public void largeUndoExit() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            /* FIXME
               largeUndoExit(org.cojen.tupl.RecoverTest)  Time elapsed: 2.916 s  <<< FAILURE!
               java.lang.AssertionError: expected:<0> but was:<1>
               at org.junit.Assert.fail(Assert.java:88)
               at org.junit.Assert.failNotEquals(Assert.java:743)
               at org.junit.Assert.assertEquals(Assert.java:118)
               at org.junit.Assert.assertEquals(Assert.java:555)
               at org.junit.Assert.assertEquals(Assert.java:542)
               at org.cojen.tupl.RecoverTest.testRecover(RecoverTest.java:419)
               at org.cojen.tupl.RecoverTest.largeUndoExit(RecoverTest.java:321)
            */
            testRecover(10000, false, true, chkpnt);
        }
    }

    @Test
    public void largeRedo() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10000, true, false, chkpnt);
        }
    }

    @Test
    public void largeRedoExit() throws Exception {
        for (int chkpnt = 0; chkpnt <= 4; chkpnt++) {
            testRecover(10000, true, true, chkpnt);
        }
    }

    private void testRecover(int count, boolean commit, boolean exit, int chkpnt)
        throws Exception
    {
        Index ix1 = mDb.openIndex("test1");
        Index ix2 = mDb.openIndex("test2");

        final int seed = 5334519;
        Random rnd = new Random(seed);

        Transaction txn = mDb.newTransaction();
        for (int i=0; i<count; i++) {
            {
                byte[] key = randomStr(rnd, 10, 100);
                byte[] value = randomStr(rnd, 10, 100);
                ix1.store(txn, key, value);
            }
            if (chkpnt == 1 && i == (count / 2)) {
                mDb.checkpoint();
            }
            {
                byte[] key = randomStr(rnd, 10, 100);
                byte[] value = randomStr(rnd, 10, 100);
                ix2.store(txn, key, value);
            }
        }

        if (chkpnt == 2) {
            mDb.checkpoint();
        }

        long count1 = CrudTest.count(ix1);
        long count2 = CrudTest.count(ix2);

        txn.enter();
        for (int i=0; i<count; i++) {
            {
                byte[] key = randomStr(rnd, 100);
                byte[] value = randomStr(rnd, 100);
                ix1.store(txn, key, value);
            }
            if (chkpnt == 3 && i == (int) (count * 0.8)) {
                mDb.checkpoint();
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
        if (exit) {
            txn.exit();
        }

        if (chkpnt == 4) {
            mDb.checkpoint();
        }

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertTrue(mDb.verify(null));

        ix1 = mDb.openIndex("test1");
        assertTrue(ix1.verify(null));
        ix2 = mDb.openIndex("test2");
        assertTrue(ix2.verify(null));

        if (!commit) {
            assertEquals(0, CrudTest.count(ix1));
            assertEquals(0, CrudTest.count(ix2)); // FIXME: failed here
        }

        if (commit) {
            rnd = new Random(seed);

            for (int i=0; i<count; i++) {
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

            for (int i=0; i<count; i++) {
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
    public void lostRedo() throws Exception {
        Index ix1 = mDb.openIndex("test1");
        Index ix2 = mDb.openIndex("test2");

        final int seed = 1234;
        Random rnd = new Random(seed);

        ix1.store(null, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));
        ix2.store(null, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));

        Transaction txn = mDb.newTransaction();
        ix1.store(txn, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));
        ix2.store(txn, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));
        txn.commit();
        txn.reset();

        txn = mDb.newTransaction(DurabilityMode.NO_SYNC);
        ix1.store(txn, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));

        // Don't commit this one.
        Transaction txn2 = mDb.newTransaction(DurabilityMode.NO_SYNC);
        ix2.store(txn2, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));

        mDb.checkpoint();

        ix1.store(txn, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));

        ix1.store(null, randomStr(rnd, 10, 100), randomStr(rnd, 10, 100));

        txn.commit();
        txn.reset();

        // Reopen, but delete the redo logs first.
        mDb = reopenTempDatabase(getClass(), mDb, mConfig, true);

        ix1 = mDb.openIndex("test1");
        ix2 = mDb.openIndex("test2");

        rnd = new Random(seed);

        // Verify that fully checkpointed records exist.
        for (int i=0; i<2; i++) {
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

        // All other records must not exist.

        for (Index ix : new Index[] {ix1, ix2}) {
            int count = 0;
            Cursor c = ix.newCursor(null);
            for (c.first(); c.key() != null; c.next()) {
                count++;
            }
            c.reset();
            assertEquals(2, count);
        }

        // No leftover locks from rolled back partial transactions.

        ix1.load(null, randomStr(rnd, 10, 100));
        randomStr(rnd, 10, 100); // skip value

        ix2.load(null, randomStr(rnd, 10, 100));
        randomStr(rnd, 10, 100); // skip value
    }

    @Test
    public void dropIndex() throws Exception {
        Index ix = mDb.openIndex("drop");

        ix.store(null, "hello".getBytes(), "world".getBytes());
        ix.delete(null, "hello".getBytes());
        mDb.checkpoint();
        ix.drop();
        assertNull(mDb.findIndex("drop"));

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        // Verify drop redo.
        assertNull(mDb.findIndex("drop"));

        // Test again, but this time with NO_REDO.
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        ix = mDb.openIndex("drop2");
        ix.store(null, "hello".getBytes(), "world".getBytes());

        mDb.checkpoint();

        // No-redo delete.
        Transaction txn = mDb.newTransaction(DurabilityMode.NO_REDO);
        try {
            ix.delete(txn, "hello".getBytes());
            txn.commit();
        } finally {
            txn.reset();
        }

        ix.drop();

        assertNull(mDb.findIndex("drop2"));

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        // Even though the delete operation was no-redo, the drop always is. It will ensure
        // everything is deleted first.
        ix = mDb.findIndex("drop2");
        assertNull(ix);
    }

    @Test
    public void noRedo() throws Exception {
        // Verifies that transaction which makes no modifications generates no redo entries.

        Index ix = mDb.openIndex("noredo");

        Transaction txn = mDb.newTransaction(DurabilityMode.NO_SYNC);
        try {
            for (int i=0; i<10; i++) {
                ix.store(txn, ("hello-" + i).getBytes(), "world".getBytes());
            }
            txn.commit();
        } finally {
            txn.reset();
        }

        File baseFile = baseFileForTempDatabase(getClass(), mDb);

        File redoFile = null;
        for (File f : baseFile.getParentFile().listFiles()) {
            if (f.getName().startsWith(baseFile.getName())) {
                if (f.getName().contains("redo")) {
                    assertNull(redoFile);
                    redoFile = f;
                }
            }
        }

        assertNotNull(redoFile);

        final long redoLength = redoFile.length();
        final long txnsCreated = mDb.stats().transactionsCreated();

        assertTrue(txnsCreated > 0);

        // Simple locking transaction, no modifications.

        for (int i=0; i<2; i++) {
            txn = mDb.newTransaction();
            try {
                byte[] value = ix.load(txn, ("hello-0").getBytes());
                fastAssertArrayEquals("world".getBytes(), value);
                if (i == 0) {
                    txn.commit();
                }
            } finally {
                txn.reset();
            }
        }

        // No growth.
        assertEquals(redoLength, redoFile.length());

        // Nested scopes, no modifications.

        for (int i=0; i<4; i++) {
            txn = mDb.newTransaction();
            try {
                byte[] value = ix.load(txn, ("hello-0").getBytes());
                fastAssertArrayEquals("world".getBytes(), value);

                txn.enter();
                try {
                    value = ix.load(txn, ("hello-1").getBytes());
                    fastAssertArrayEquals("world".getBytes(), value);

                    if ((i & 2) == 0) {
                        txn.commit();
                    }
                } finally {
                    txn.exit();
                }

                if ((i & 1) == 0) {
                    txn.commit();
                }
            } finally {
                txn.reset();
            }
        }

        // No growth.
        assertEquals(redoLength, redoFile.length());

        // All locks released.
        Cursor c = ix.newCursor(null);
        for (c.first(); c.key() != null; c.next());
        c.reset();

        // A transaction is only truly created if it modifies anything.
        assertEquals(txnsCreated, mDb.stats().transactionsCreated());
    }

    @Test
    public void cachePriming() throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=0; i<1_000_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            ix.store(null, key, value);
        }

        Index ix2 = mDb.openIndex("test2");
        ix2.store(null, "hello".getBytes(), "world".getBytes());

        File primer = new File(baseFileForTempDatabase(getClass(), mDb).getPath() + ".primer");
        assertFalse(primer.exists());

        mDb.close();
        assertFalse(primer.exists());

        mConfig.cachePriming(true);

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertFalse(primer.exists());

        mDb.close();
        assertTrue(primer.exists());

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertFalse(primer.exists());
    }

    @Test
    public void trashDelete() throws Exception {
        Index ix = mDb.openIndex("trash");
        ix.store(null, "hello".getBytes(), "world".getBytes());
        mDb.checkpoint();

        // Write after the checkpoint.
        ix.store(null, "goodbye".getBytes(), "world".getBytes());

        // Moves ix into the trash
        mDb.deleteIndex(ix);

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertNull(mDb.findIndex("trash"));
    }

    @Test
    public void trashDeleteEmpty() throws Exception {
        final String ixname = "empty";
        Index ix = mDb.openIndex(ixname);
        mDb.deleteIndex(ix);
        mDb.checkpoint();

        // Re-opening will start background job to delete trashed index.
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertNull(mDb.findIndex(ixname));
    }

    @Test
    public void rollbackDeadlock() throws Exception {
        // Rollbacks must release their locks before any other commits appear in the log, or
        // else recovery can deadlock or timeout.

        Index ix = mDb.openIndex("trash");

        Transaction t1 = mDb.newTransaction(DurabilityMode.NO_FLUSH);
        ix.store(t1, "hello".getBytes(), "world".getBytes());
        // Force a flush of TransactionContext with a big value.
        ix.store(t1, "xxx".getBytes(), new byte[100_000]);
        t1.exit();

        Transaction t2 = mDb.newTransaction(DurabilityMode.SYNC);
        ix.store(t2, "hello".getBytes(), "world!!!".getBytes());
        t2.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        ix = mDb.openIndex("trash");
        assertArrayEquals("world!!!".getBytes(), ix.load(null, "hello".getBytes()));
        assertNull(ix.load(null, "xxx".getBytes()));
    }

    @Test
    public void lostRollback() throws Exception {
        // A transaction which auto-resets due to an exception must alway issue a rollback
        // operation into the redo log.

        byte[] k1 = "key1".getBytes();
        byte[] k2 = "key2".getBytes();

        Index ix = mDb.openIndex("test");

        Transaction t1 = mDb.newTransaction();
        // lock k1 and write to redo log
        ix.store(t1, k1, k1);

        Transaction t2 = mDb.newTransaction();
        // lock k2 and write to redo log
        ix.store(t2, k2, k2);

        // Perform an operation which auto-resets the transaction on lock timeout. The default
        // Cursor.commit method calls ViewUtils.commit, which resets the transaction if an
        // exception is thrown. In case the implementation ever changes, the original code is
        // copied here.
        Cursor c = ix.newCursor(t2);
        t2.lockMode(LockMode.UNSAFE);
        c.find(k1);
        t2.lockMode(LockMode.UPGRADABLE_READ);
        byte[] value = "v2".getBytes();
        try {
            // Same as ViewUtils.commit (except with test assertions added).
            try {
                c.store(value);
            } catch (Throwable e) {
                Transaction txn = c.link();
                if (txn != null) {
                    txn.reset(e);
                } else {
                    fail("no linked transaction");
                }
                throw e;
            }

            fail("should not be reached");
        } catch (LockTimeoutException e) {
            // Expected.
        }

        // Transaction t1 can write k2, since t1 has released all of its locks.
        ix.store(t1, k2, k2);
        t1.commit();

        // If t1 didn't issue a rollback, then recovery will deadlock or fail on the second
        // attempt to lock k2. A replicated log will deadlock, but a local redo log throws a
        // LockTimeoutException and aborts recovery.
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        ix = mDb.openIndex("test");
        assertArrayEquals(k1, ix.load(null, k1));
        assertArrayEquals(k2, ix.load(null, k2));
    }

    @Test
    public void manyOpenTransactions() throws Exception {
        // Test which ensures that the master undo log can properly track all active
        // transactions, even when multiple undo log nodes are required to encode them all.

        Index ix = mDb.openIndex("test");

        Transaction[] txns = new Transaction[1000];

        for (int i=0; i<txns.length; i++) {
            txns[i] = mDb.newTransaction();
            ix.store(txns[i], ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        mDb.checkpoint();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        // Everything should have rolled back.
        ix = mDb.openIndex("test");
        assertEquals(0, ix.count(null, null));
    }

    @Test
    public void largeUndoMidCheckpoint() throws Exception {
        // Test commit of a transaction with a large undo log, with a checkpoint in the middle
        // of it. This exersizes handling of the OP_COMMIT_TRUNCATE operation during recovery.

        final Index ix = mDb.openIndex("test");
        Random rnd = new Random(3494847);

        Transaction txn = mDb.newTransaction();
        for (int i=0; i<100_000; i++) {
            byte[] key = randomStr(rnd, 10, 5000);
            byte[] value = randomStr(rnd, 10, 50);
            ix.store(txn, key, value);
        }

        Thread checkpointer = new Thread(() -> {
            try {
                Thread.sleep(100);

                // Start another write, for the the commit lock to indicate that it has queued
                // waiters when the checkpoint is waiting to acquire the exclusive commit lock.
                new Thread(() -> {
                    try {
                        Thread.sleep(100);
                        ix.store(null, "hello".getBytes(), "world".getBytes());
                    } catch (Exception e) {
                    }
                }).start();

                mDb.checkpoint();
            } catch (Exception e) {
                Utils.rethrow(e);
            }
        });

        checkpointer.start();

        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        // Everything should have committed.

        Index ix2 = mDb.openIndex("test");

        rnd = new Random(3494847);

        for (int i=0; i<100_000; i++) {
            byte[] key = randomStr(rnd, 10, 5000);
            byte[] value = randomStr(rnd, 10, 50);
            fastAssertArrayEquals(value, ix2.load(null, key));
        }
    }

    @Test
    public void testUndoNonReplicatedTransaction() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
                .directPageAccess(false)
                .checkpointRate(-1, null)
                .durabilityMode(DurabilityMode.SYNC);

        decorate(config);

        // open database with NonReplicationManager
        NonReplicationManager replMan = new NonReplicationManager();
        config.replicate(replMan);
        Database db = newTempDatabase(getClass(), config);

        // open index
        replMan.asLeader();
        Thread.yield();
        Index ix = null;
        for (int i=0; i<10; i++) {
            try {
                ix = db.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                Thread.sleep(100);
            }
        }
        assertTrue(ix != null);

        db.checkpoint();

        // switch to replica
        replMan.asReplica();
        try {
            ix.store(null, "somekey".getBytes(), "someval".getBytes());
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        // checkpoint with a live open transaction
        Transaction txn = db.newTransaction(DurabilityMode.NO_REDO);
        ix.store(txn, "key1".getBytes(), "val1".getBytes());
        db.checkpoint();
        db.close();

        // reopen database as replica; use new ReplicationManager as the existing one is closed
        replMan = new NonReplicationManager();
        replMan.asReplica();
        Database db2 = Database.open(config.replicate(replMan));

        // FIXME: must wait till caught up

        // assert no lingering locks exist on the key after recovery
        Index ix2 = db2.findIndex("test");
        assertTrue(ix2 != null);
        Transaction txn2 = db2.newTransaction();
        assertTrue(ix2.tryLockExclusive(txn2, "key1".getBytes(), 1).isHeld());

        db2.close();
    }
}
