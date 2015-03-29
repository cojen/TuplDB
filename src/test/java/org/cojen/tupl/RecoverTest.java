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

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        mDb = newTempDatabase(mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
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
                    txn2.lockTimeout(10, TimeUnit.SECONDS);
                    assertEquals(null, ix.load(txn2, key));
                } catch (Throwable e) {
                    ex = e;
                }
            }
        };

        Waiter w = new Waiter();
        Thread t = new Thread(w);
        t.start();

        Thread.sleep(1000);
        mDb.close();

        t.join();

        assertTrue(w.ex instanceof LockInterruptedException);

        try {
            txn1.exit();
            fail();
        } catch (DatabaseException e) {
        }

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

        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");
        assertNull(ix.load(Transaction.BOGUS, key));
        ix.store(Transaction.BOGUS, key, value);
        mDb.checkpoint();

        mDb = reopenTempDatabase(mDb, mConfig);
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

        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");
        txn = mDb.newTransaction(DurabilityMode.NO_REDO);
        assertNull(ix.load(txn, key));
        ix.store(txn, key, value);
        txn.commit();
        mDb.checkpoint();

        mDb = reopenTempDatabase(mDb, mConfig);
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

        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");
        assertArrayEquals(value, ix.load(null, key));

        txn = mDb.newTransaction();
        ix.store(txn, key, null);
        txn.commit();

        mDb = reopenTempDatabase(mDb, mConfig);
        ix = mDb.openIndex("test");
        assertEquals(null, ix.load(null, key));
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

        mDb = reopenTempDatabase(mDb, mConfig);
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

        int count1 = CrudTest.count(ix1);
        int count2 = CrudTest.count(ix2);

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

        mDb = reopenTempDatabase(mDb, mConfig);
        assertTrue(mDb.verify(null));

        ix1 = mDb.openIndex("test1");
        assertTrue(ix1.verify(null));
        ix2 = mDb.openIndex("test2");
        assertTrue(ix2.verify(null));

        if (!commit) {
            assertEquals(0, CrudTest.count(ix1));
            assertEquals(0, CrudTest.count(ix2));
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
        mDb = reopenTempDatabase(mDb, mConfig, true);

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

        mDb = reopenTempDatabase(mDb, mConfig);

        // Verify drop redo.
        assertNull(mDb.findIndex("drop"));

        // Test again, but this time with NO_REDO.
        mDb = reopenTempDatabase(mDb, mConfig);

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

        mDb = reopenTempDatabase(mDb, mConfig);

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

        File baseFile = baseFileForTempDatabase(mDb);

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

        File primer = new File(baseFileForTempDatabase(mDb).getPath() + ".primer");
        assertFalse(primer.exists());

        mDb.close();
        assertFalse(primer.exists());

        mConfig.cachePriming(true);

        mDb = reopenTempDatabase(mDb, mConfig);
        assertFalse(primer.exists());

        mDb.close();
        assertTrue(primer.exists());

        mDb = reopenTempDatabase(mDb, mConfig);
        assertFalse(primer.exists());
    }
}
