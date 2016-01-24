/*
 *  Copyright 2016 Cojen.org
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
 * Tests the slow path modes when loading a single index entry.
 *
 * @author Brian S O'Neill
 */
public class ContendedLockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ContendedLockTest.class.getName());
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

    protected Database mDb;

    @Test
    public void timedOutUpdate() throws Exception {
        View ix = openIndex("foo");

        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "world!!!".getBytes();

        ix.store(null, key, value1);

        Transaction txn = mDb.newTransaction();
        ix.store(txn, key, value2);

        fastAssertArrayEquals(value2, ix.load(Transaction.BOGUS, key));

        try {
            ix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        Transaction txn2 = mDb.newTransaction();
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.UPGRADABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.REPEATABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_COMMITTED);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_UNCOMMITTED);
        fastAssertArrayEquals(value2, ix.load(txn2, key));

        txn2.lockMode(LockMode.UNSAFE);
        fastAssertArrayEquals(value2, ix.load(txn2, key));

        txn.commit();

        fastAssertArrayEquals(value2, ix.load(null, key));
    }

    @Test
    public void timedOutDelete() throws Exception {
        View ix = openIndex("foo");

        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();

        ix.store(null, key, value1);

        Transaction txn = mDb.newTransaction();
        ix.store(txn, key, null);

        assertEquals(null, ix.load(Transaction.BOGUS, key));

        try {
            ix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        Transaction txn2 = mDb.newTransaction();
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.UPGRADABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.REPEATABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_COMMITTED);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_UNCOMMITTED);
        assertEquals(null, ix.load(txn2, key));

        txn2.lockMode(LockMode.UNSAFE);
        assertEquals(null, ix.load(txn2, key));

        txn.commit();

        assertEquals(null, ix.load(null, key));
    }

    @Test
    public void timedOutDeleteNoGhost() throws Exception {
        View ix = openIndex("foo");

        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();

        ix.store(null, key, value1);

        Transaction txn = mDb.newTransaction();
        ix.lockExclusive(txn, key);
        ix.store(Transaction.BOGUS, key, null);

        assertEquals(null, ix.load(Transaction.BOGUS, key));

        try {
            ix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        Transaction txn2 = mDb.newTransaction();
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.UPGRADABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.REPEATABLE_READ);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_COMMITTED);
        try {
            ix.load(txn2, key);
            fail();
        } catch (LockTimeoutException e) {
        }

        txn2.lockMode(LockMode.READ_UNCOMMITTED);
        assertEquals(null, ix.load(txn2, key));

        txn2.lockMode(LockMode.UNSAFE);
        assertEquals(null, ix.load(txn2, key));

        txn.commit();

        assertEquals(null, ix.load(null, key));
    }

    @Test
    public void delayedUpdate() throws Exception {
        View ix = openIndex("foo");

        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();

        ix.store(null, key, value1);

        int i = 0;
        Updater u = start(ix, key, ("world-" + (i++)).getBytes());

        fastAssertArrayEquals(u.mValue, ix.load(null, key));

        u = start(ix, key, ("world-" + (i++)).getBytes());
        Transaction txn2 = mDb.newTransaction();
        fastAssertArrayEquals(u.mValue, ix.load(txn2, key));
        txn2.reset();

        u = start(ix, key, ("world-" + (i++)).getBytes());
        txn2.lockMode(LockMode.UPGRADABLE_READ);
        fastAssertArrayEquals(u.mValue, ix.load(txn2, key));
        txn2.reset();

        u = start(ix, key, ("world-" + (i++)).getBytes());
        txn2.lockMode(LockMode.REPEATABLE_READ);
        fastAssertArrayEquals(u.mValue, ix.load(txn2, key));
        txn2.reset();

        u = start(ix, key, ("world-" + (i++)).getBytes());
        txn2.lockMode(LockMode.READ_COMMITTED);
        fastAssertArrayEquals(u.mValue, ix.load(txn2, key));
        txn2.reset();
    }

    @Test
    public void delayedDelete() throws Exception {
        delayedDelete(false);
    }

    @Test
    public void delayedDeleteNoGhost() throws Exception {
        delayedDelete(true);
    }

    private void delayedDelete(boolean noGhost) throws Exception {
        View ix = openIndex("foo");

        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();

        ix.store(null, key, value1);

        Updater u = start(ix, key, null, noGhost);

        assertEquals(null, ix.load(null, key));

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        Transaction txn2 = mDb.newTransaction();
        assertEquals(null, ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.UPGRADABLE_READ);
        assertEquals(null, ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.REPEATABLE_READ);
        assertEquals(null, ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.READ_COMMITTED);
        assertEquals(null, ix.load(txn2, key));
        txn2.reset();
    }

    private Updater start(View ix, byte[] key, byte[] value) throws Exception {
        return start(ix, key, value, false);
    }

    private Updater start(View ix, byte[] key, byte[] value, boolean noGhost) throws Exception {
        Updater u = new Updater(ix, key, value, noGhost);
        u.start();
        u.waitToSleep();
        return u;
    }

    class Updater extends Thread {
        final View mIx;
        final byte[] mKey;
        final byte[] mValue;
        final boolean mNoGhost;

        private boolean mSleeping;

        Updater(View ix, byte[] key, byte[] value, boolean noGhost) {
            mIx = ix;
            mKey = key;
            mValue = value;
            mNoGhost = noGhost;
        }

        public void run() {
            try {
                Transaction txn = mDb.newTransaction();

                if (mNoGhost) {
                    mIx.lockExclusive(txn, mKey);
                    mIx.store(Transaction.BOGUS, mKey, mValue);
                } else {
                    mIx.store(txn, mKey, mValue);
                }

                synchronized (this) {
                    mSleeping = true;
                    notify();
                }
                Thread.sleep(500);
                txn.commit();
            } catch (Exception e) {
            }
        }

        synchronized void waitToSleep() throws Exception {
            while (!mSleeping) {
                wait();
            }
        }
    }
}
