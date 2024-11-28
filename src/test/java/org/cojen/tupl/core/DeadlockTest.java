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

import java.util.*;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.DeadlockInfo;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DeadlockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DeadlockTest.class.getName());
    }

    volatile LockManager mManager;

    private List<Task> mTasks;

    @Before
    public void setup() {
        mManager = new LockManager(null, null, -1);
        mTasks = new ArrayList<Task>();
    }

    @Test
    public void test_1() throws Throwable {
        // Create a deadlock among three threads and a victim thread.

        final long timeout = 10L * 1000 * 1000 * 1000;
        final byte[][] keys = {"k0".getBytes(), "k1".getBytes(), "k2".getBytes()};

        var tasks = new TestTask[3];
        var cb = new CyclicBarrier(tasks.length + 1);

        var numDeadlocks = new AtomicInteger();

        // Culprit threads.
        for (int i=0; i<tasks.length; i++) {
            final byte[] k1 = keys[i];
            final byte[] k2 = keys[(i + 1) % keys.length];

            tasks[i] = startTestTask(() -> {
                var locker = new Locker(mManager);
                try {
                    locker.doLockShared(1, k1, timeout);
                    cb.await();
                    locker.doLockExclusive(1, k2, timeout);
                } catch (DeadlockException e) {
                    numDeadlocks.getAndAdd(1);
                    // This thread helped create the deadlock.
                    assertTrue(e.isGuilty());
                } catch (LockTimeoutException e) {
                    // Lost the race to detect the deadlock.
                } catch (Exception e) {
                    Utils.rethrow(e);
                } finally {
                    locker.scopeUnlockAll();
                }
            });
        }

        cb.await();

        // Victim thread.
        var victim = startTestTask(() -> {
            var locker = new Locker(mManager);
            try {
                // The first lock doesn't participate in deadlock.
                locker.doLockExclusive(1, "xxx".getBytes(), timeout / 2);
                locker.doLockExclusive(1, keys[0], timeout / 2);
                fail(); // FIXME: fails under load
            } catch (DeadlockException e) {
                // Deadlock observed, but this thread didn't create it.
                assertFalse(e.isGuilty());
            } catch (Exception e) {
                Utils.rethrow(e);
            } finally {
                locker.scopeUnlockAll();
            }
        });

        victim.join();

        for (TestTask task : tasks) {
            task.join();
        }

        assertTrue(numDeadlocks.get() > 0);
    }

    @Test
    public void test_2() throws Throwable {
        try {
            doTest_2();
        } catch (AssertionError e) {
            // Time-sensitive test, so try again.
            setup();
            doTest_2();
        }
    }

    private void doTest_2() throws Throwable {
        // Deadlock caused by three threads and three keys.

        final long timeout = 10L * 1000 * 1000 * 1000;

        class TheTask extends Task {
            private final long mTimeout;
            private final boolean mExpectDeadlock;
            private final byte[][] mKeys;

            TheTask(long timeout, boolean expectDeadlock, String... keys) {
                mTimeout = timeout;
                mExpectDeadlock = expectDeadlock;
                mKeys = new byte[keys.length][];
                for (int i=0; i<keys.length; i++) {
                    mKeys[i] = keys[i].getBytes();
                }
            }

            void doRun() throws Throwable {
                try {
                    var locker = new Locker(mManager);
                    try {
                        for (byte[] key : mKeys) {
                            locker.doLockUpgradable(1, key, mTimeout);
                            sleep(100);
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                    assertFalse("task assert 1", mExpectDeadlock);
                } catch (DeadlockException e) {
                    assertTrue("task assert 2", mExpectDeadlock);
                }
            }
        }

        // All three threads must be waiting for a deadlock to occur. As soon
        // as one times out, the rest can proceed because the dependency cycle
        // is broken.

        mTasks.add(new TheTask(timeout,      true,  "k1", "k2"));
        mTasks.add(new TheTask(timeout * 10, false, "k2", "k3"));
        mTasks.add(new TheTask(timeout * 10, false, "k3", "k1"));

        startTasks();
        joinTasks();
    }

    @Test
    public void deadlockInfo() throws Throwable {
        Database db = Database.open(new DatabaseConfig()
                                    .lockUpgradeRule(LockUpgradeRule.UNCHECKED));

        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key = "hello".getBytes();

        txn1.lockUpgradable(ix.id(), key);
        txn2.lockShared(ix.id(), key);

        try {
            txn2.lockExclusive(ix.id(), key);
            fail();
        } catch (DeadlockException e) {
            assertTrue(e.getMessage().indexOf("indexName: test") > 0);
            assertTrue(e.getMessage().indexOf("owner attachment: txn1") > 0);
            assertEquals("txn1", e.ownerAttachment());
            assertEquals("txn1", e.deadlockSet().iterator().next().ownerAttachment());
        }

        // Deadlock detection works with zero timeout, except with the tryLock variant.
        txn2.lockTimeout(0, null);
        try {
            txn2.lockExclusive(ix.id(), key);
            fail();
        } catch (DeadlockException e) {
            assertEquals(0, e.timeout());
            assertTrue(e.getMessage().indexOf("indexName: test") > 0);
            assertTrue(e.getMessage().indexOf("owner attachment: txn1") > 0);
            assertEquals("txn1", e.ownerAttachment());
            assertEquals("txn1", e.deadlockSet().iterator().next().ownerAttachment());
        }

        // No deadlock detected here.
        assertEquals(LockResult.TIMED_OUT_LOCK, txn2.tryLockExclusive(ix.id(), key, 0));

        db.close();
    }

    @Test
    public void deadlockAttachments() throws Throwable {
        Database db = Database.open(new DatabaseConfig());

        Index ix = db.openIndex("test");

        // Create a deadlock among three threads.

        var threads = new Thread[3];
        var cb = new CyclicBarrier(threads.length + 1);

        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        for (int i=0; i<threads.length; i++) {
            final int fi = i;

            threads[i] = new Thread(() -> {
                try {
                    Transaction txn = db.newTransaction();
                    try {
                        txn.lockTimeout(10, TimeUnit.SECONDS);

                        if (fi == 0) {
                            txn.attach("txn1");
                            ix.lockExclusive(txn, k1);
                            cb.await();
                            ix.lockShared(txn, k2);
                        } else if (fi == 1) {
                            txn.attach("txn2");
                            ix.lockExclusive(txn, k2);
                            cb.await();
                            ix.lockUpgradable(txn, k3);
                        } else {
                            txn.attach("txn3");
                            ix.lockExclusive(txn, k3);
                            cb.await();
                            ix.lockUpgradable(txn, k1);
                        }
                    } finally {
                        txn.reset();
                    }
                } catch (Exception e) {
                    // Ignore.
                }
            });

            threads[i].start();
        }

        cb.await();

        waitForDeadlock: {
            check: for (int i=0; i<100; i++) {
                for (int j=0; j<threads.length; j++) {
                    if (threads[j].getState() != Thread.State.TIMED_WAITING) {
                        Thread.sleep(100);
                        continue check;
                    }
                }
                break waitForDeadlock;
            }
            fail("no deadlock after waiting");
        }

        Transaction txn = db.newTransaction();

        try {
            ix.lockShared(txn, k1);
            fail("no deadlock");
        } catch (DeadlockException e) {
            assertFalse(e.isGuilty());
            assertEquals("txn1", e.ownerAttachment());

            Set<DeadlockInfo> set = e.deadlockSet();
            assertEquals(3, set.size());

            var expect = new HashSet<>(Set.of("txn1", "txn2", "txn3"));

            for (DeadlockInfo info : set) {
                Object att = info.ownerAttachment();
                if (!expect.remove(att)) {
                    fail("Unknown attachments: " + att);
                }
            }
        }

        db.close();

        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void selfDeadlock() throws Throwable {
        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        txn1.lockUpgradable(ix.id(), key1);
        txn2.lockUpgradable(ix.id(), key2);

        try {
            txn2.lockUpgradable(ix.id(), key1);
            fail();
        } catch (DeadlockException e) {
            // Not expected to work.
            throw e;
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.ownerAttachment());
        }

        try {
            txn1.lockUpgradable(ix.id(), key2);
            fail();
        } catch (DeadlockException e) {
            // Not expected to work.
            throw e;
        } catch (LockTimeoutException e) {
            assertEquals("txn2", e.ownerAttachment());
        }

        // Verify owner attachment when not using an explicit transaction.
        try {
            ix.store(null, key1, key1);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.ownerAttachment());
        }

        db.close();
    }

    @Test
    public void sharedOwner() throws Throwable {
        // Not really a deadlock test. Checks for shared lock owner attachments.

        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key = "key".getBytes();

        txn1.lockShared(ix.id(), key);

        // No conflict.
        txn2.lockUpgradable(ix.id(), key);
        txn2.unlock();

        try {
            txn2.lockExclusive(ix.id(), key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.ownerAttachment());
        }

        txn2.lockShared(ix.id(), key);
 
        Transaction txn3 = db.newTransaction();
        try {
            txn3.lockExclusive(ix.id(), key);
            fail();
        } catch (LockTimeoutException e) {
            Object att = e.ownerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }

        // Can still get attachment even when not waited.
        txn3.lockTimeout(0, null);
        try {
            txn3.lockExclusive(ix.id(), key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals(0, e.timeout());
            Object att = e.ownerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }

        // Verify owner attachment when not using an explicit transaction.
        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            Object att = e.ownerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }

        db.close();
    }

    @Test
    public void deleteTimeout() throws Throwable {
        // Regression test. Deleting an entry within a transaction would cause the attachment
        // check code to fail with a ClassCastException.

        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");

        byte[] key = "key".getBytes();

        ix.store(null, key, key);

        Transaction txn = db.newTransaction();
        ix.store(txn, key, null);

        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            assertNull(e.ownerAttachment());
        }

        // Also make sure that attachments can be retrieved.
        txn.attach("foo");
        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("foo", e.ownerAttachment());
        }

        db.close();
    }

    @Test
    public void trivialShared() throws Throwable {
        trivialShared(false);
    }

    @Test
    public void trivialSharedWithQueue() throws Throwable {
        trivialShared(true);
    }

    private void trivialShared(boolean withQueue) throws Throwable {
        // Detect a trivial deadlock when acquiring a shared lock.

        final byte[] key1 = "key1".getBytes();
        final byte[] key2 = "key2".getBytes();

        final long timeout = 100L * 1000 * 1000 * 1000;

        var locker1 = new Locker(mManager);
        locker1.doLockExclusive(1, key1, timeout);

        var task1 = startTestTaskAndWaitUntilBlocked(() -> {
            var locker = new Locker(mManager);
            try {
                locker.doLockExclusive(1, key2, timeout);
                locker.doLockShared(1, key1, timeout);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            } finally {
                locker.scopeUnlockAll();
            }
        });

        TestTask<?> task2 = null;

        if (withQueue) {
            // Another thread is stuck waiting, as a victim.
            task2 = startTestTaskAndWaitUntilBlocked(() -> {
                var locker = new Locker(mManager);
                try {
                    locker.doLockShared(1, key2, timeout);
                } catch (Throwable e) {
                    throw Utils.rethrow(e);
                } finally {
                    locker.scopeUnlockAll();
                }
            });
        }

        try {
            locker1.doLockShared(1, key2, timeout);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.scopeUnlockAll();

        task1.join();

        if (task2 != null) {
            task2.join();
        }
    }

    @Test
    public void trivialUpgradable() throws Throwable {
        trivialUpgradable(false);
    }

    @Test
    public void trivialUpgradableWithQueue() throws Throwable {
        trivialUpgradable(true);
    }

    private void trivialUpgradable(boolean withQueue) throws Throwable {
        // Detect a trivial deadlock when acquiring an upgradable lock.

        final byte[] key1 = "key1".getBytes();
        final byte[] key2 = "key2".getBytes();

        final long timeout = 100L * 1000 * 1000 * 1000;

        var locker1 = new Locker(mManager);
        locker1.doLockExclusive(1, key1, timeout);

        var task1 = startTestTaskAndWaitUntilBlocked(() -> {
            var locker = new Locker(mManager);
            try {
                locker.doLockExclusive(1, key2, timeout);
                locker.doLockUpgradable(1, key1, timeout);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            } finally {
                locker.scopeUnlockAll();
            }
        });

        TestTask<?> task2 = null;

        if (withQueue) {
            // Another thread is stuck waiting, as a victim.
            task2 = startTestTaskAndWaitUntilBlocked(() -> {
                var locker = new Locker(mManager);
                try {
                    locker.doLockUpgradable(1, key2, timeout);
                } catch (Throwable e) {
                    throw Utils.rethrow(e);
                } finally {
                    locker.scopeUnlockAll();
                }
            });
        }

        try {
            locker1.doLockUpgradable(1, key2, timeout);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.scopeUnlockAll();

        task1.join();

        if (task2 != null) {
            task2.join();
        }
    }

    @Test
    public void trivialExclusive() throws Throwable {
        // Detect a trivial deadlock when acquiring an exclusive lock.

        final byte[] key1 = "key1".getBytes();
        final byte[] key2 = "key2".getBytes();

        final long timeout = 100L * 1000 * 1000 * 1000;

        var locker1 = new Locker(mManager);
        locker1.doLockShared(1, key1, timeout);

        var task1 = startTestTaskAndWaitUntilBlocked(() -> {
            var locker = new Locker(mManager);
            try {
                locker.doLockShared(1, key2, timeout);
                locker.doLockExclusive(1, key1, timeout);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            } finally {
                locker.scopeUnlockAll();
            }
        });

        try {
            locker1.doLockExclusive(1, key2, timeout);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.scopeUnlockAll();

        task1.join();
    }

    private void startTasks() {
        for (Task t : mTasks) {
            t.start();
        }
    }

    private void joinTasks() throws Throwable {
        for (Task t : mTasks) {
            t.join();
        }
        for (Task t : mTasks) {
            t.check();
        }
    }

    static abstract class Task extends Thread {
        private volatile Throwable mFailure;

        @Override
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                mFailure = t;
            }
        }

        void check() throws Throwable {
            Throwable t = mFailure;
            if (t != null) {
                throw t;
            }
        }

        abstract void doRun() throws Throwable;
    }
}
