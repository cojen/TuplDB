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

package org.cojen.tupl.core;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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
        var config = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .lockTimeout(5, TimeUnit.SECONDS);
        mDb = newTempDatabase(getClass(), config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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

        assertNull(ix.load(Transaction.BOGUS, key));

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
        assertNull(ix.load(txn2, key));

        txn2.lockMode(LockMode.UNSAFE);
        assertNull(ix.load(txn2, key));

        txn.commit();

        assertNull(ix.load(null, key));
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

        assertNull(ix.load(Transaction.BOGUS, key));

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
        assertNull(ix.load(txn2, key));

        txn2.lockMode(LockMode.UNSAFE);
        assertNull(ix.load(txn2, key));

        txn.commit();

        assertNull(ix.load(null, key));
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

        assertNull(ix.load(null, key));

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        Transaction txn2 = mDb.newTransaction();
        assertNull(ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.UPGRADABLE_READ);
        assertNull(ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.REPEATABLE_READ);
        assertNull(ix.load(txn2, key));
        txn2.reset();

        ix.store(null, key, value1);
        u = start(ix, key, null, noGhost);
        txn2.lockMode(LockMode.READ_COMMITTED);
        assertNull(ix.load(txn2, key));
        txn2.reset();
    }

    private Updater start(View ix, byte[] key, byte[] value) throws Exception {
        return start(ix, key, value, false);
    }

    private Updater start(View ix, byte[] key, byte[] value, boolean noGhost) throws Exception {
        var u = new Updater(ix, key, value, noGhost);
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
