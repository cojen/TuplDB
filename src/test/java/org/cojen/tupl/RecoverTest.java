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

    // FIXME: Add test to confirm no redo if transaction does not modify anything.

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
}
