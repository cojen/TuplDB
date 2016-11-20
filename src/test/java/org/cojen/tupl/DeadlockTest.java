/*
 *  Copyright 2011-2015 Cojen.org
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

import static org.cojen.tupl.TestUtils.sleep;

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
        // Simple deadlock caused by two threads and two keys. Another thread
        // is a victim which timed out.

        final long timeout = 5L * 1000 * 1000 * 1000;
        final byte[][] keys = {"k0".getBytes(), "k1".getBytes(), "k2".getBytes()};

        // Current thread locks k0, and helps cause the deadlock.
        Locker locker = new Locker(mManager);
        locker.lockShared(1, keys[0], timeout);

        // Victim thread.
        mTasks.add(new Task() {
                void doRun() throws Throwable {
                    Locker locker = new Locker(mManager);
                    try {
                        // Lock k2 has does not participate in deadlock.
                        locker.lockExclusive(1, keys[2], timeout / 2);
                        sleep(1000);
                        try {
                            locker.lockExclusive(1, keys[0], timeout / 2);
                            fail();
                        } catch (DeadlockException e) {
                            // Deadlock observed, but this thread didn't create it.
                            assertFalse(e.isGuilty());
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                }
            });

        // Culprit thread.
        mTasks.add(new Task() {
                void doRun() throws Throwable {
                    Locker locker = new Locker(mManager);
                    try {
                        // Lock k1 and then k0, which is the opposite order of main thread.
                        locker.lockShared(1, keys[1], timeout);
                        sleep(500);
                        try {
                            locker.lockExclusive(1, keys[0], timeout);
                            fail();
                        } catch (DeadlockException e) {
                            // This thread helped create the deadlock.
                            assertTrue(e.isGuilty());
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                }
            });

        startTasks();

        sleep(250);

        // Lock k1, creating a deadlock. Timeout is longer, and so deadlock
        // will not be detected here.
        try {
            locker.lockExclusive(1, keys[1], timeout * 2);
        } finally {
            locker.scopeUnlockAll();
        }

        joinTasks();
    }

    @Test
    public void test_2() throws Throwable {
        // Deadlock caused by three threads and three keys.

        final long timeout = 3L * 1000 * 1000 * 1000;

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
                    Locker locker = new Locker(mManager);
                    try {
                        for (byte[] key : mKeys) {
                            locker.lockUpgradable(1, key, mTimeout);
                            sleep(100);
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                    assertFalse(mExpectDeadlock);
                } catch (DeadlockException e) {
                    assertTrue(mExpectDeadlock);
                }
            }
        };

        // All three threads must be waiting for a deadlock to occur. As soon
        // as one times out, the rest can proceed because the dependency cycle
        // is broken.

        mTasks.add(new TheTask(timeout,     true,  "k1", "k2"));
        mTasks.add(new TheTask(timeout * 2, false, "k2", "k3"));
        mTasks.add(new TheTask(timeout * 2, false, "k3", "k1"));

        startTasks();
        joinTasks();
    }

    @Test
    public void deadlockInfo() throws Throwable {
        Database db = Database.open(new DatabaseConfig()
                                    .directPageAccess(false)
                                    .lockUpgradeRule(LockUpgradeRule.UNCHECKED));

        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key = "hello".getBytes();

        txn1.lockUpgradable(ix.getId(), key);
        txn2.lockShared(ix.getId(), key);

        try {
            txn2.lockExclusive(ix.getId(), key);
            fail();
        } catch (DeadlockException e) {
            assertTrue(e.getMessage().indexOf("indexName: test") > 0);
            assertTrue(e.getMessage().indexOf("owner attachment: txn1") > 0);
            assertEquals("txn1", e.getOwnerAttachment());
            assertEquals("txn1", e.getDeadlockSet().getOwnerAttachment(0));
        }

        // Deadlock detection works with zero timeout, except with the tryLock variant.
        txn2.lockTimeout(0, null);
        try {
            txn2.lockExclusive(ix.getId(), key);
            fail();
        } catch (DeadlockException e) {
            assertEquals(0, e.getTimeout());
            assertTrue(e.getMessage().indexOf("indexName: test") > 0);
            assertTrue(e.getMessage().indexOf("owner attachment: txn1") > 0);
            assertEquals("txn1", e.getOwnerAttachment());
            assertEquals("txn1", e.getDeadlockSet().getOwnerAttachment(0));
        }

        // No deadlock detected here.
        assertEquals(LockResult.TIMED_OUT_LOCK, txn2.tryLockExclusive(ix.getId(), key, 0));
    }

    @Test
    @Ignore
    public void deadlockAttachments() throws Throwable {
        Database db = Database.open(new DatabaseConfig().directPageAccess(false));

        Index ix = db.openIndex("test");

        // Create a deadlock with two threads.
        for (int i=0; i<2; i++) {
            final int fi = i;

            new Thread(() -> {
                try {
                    Transaction txn = db.newTransaction();
                    try {
                        txn.lockTimeout(2, TimeUnit.SECONDS);

                        byte[] k1 = "k1".getBytes();
                        byte[] k2 = "k2".getBytes();

                        if (fi == 0) {
                            txn.attach("txn1");
                            ix.lockExclusive(txn, k1);
                            ix.lockShared(txn, k2);
                        } else {
                            txn.attach("txn2");
                            ix.lockExclusive(txn, k2);
                            ix.lockUpgradable(txn, k1);
                        }
                    } finally {
                        txn.reset();
                    }
                } catch (Exception e) {
                    // Ignore.
                }
            }).start();
        }

        byte[] k1 = "k1".getBytes();
        Transaction txn = db.newTransaction();

        boolean deadlocked = false;

        for (int i=0; i<100; i++) {
            try {
                try {
                    ix.lockShared(txn, k1);
                    // Wait and try again.
                    Thread.sleep(100);
                } finally {
                    txn.reset();
                }
            } catch (DeadlockException e) {
                deadlocked = true;

                assertFalse(e.isGuilty());
                assertEquals("txn1", e.getOwnerAttachment());

                DeadlockSet set = e.getDeadlockSet();
                assertEquals(2, set.size());

                Object att1 = set.getOwnerAttachment(0);
                Object att2 = set.getOwnerAttachment(1);

                assertTrue(att1 != null && att2 != null);

                if (att1.equals("txn1")) {
                    assertEquals("txn2", att2);
                } else if (att1.equals("txn2")) {
                    assertEquals("txn1", att2);
                } else {
                    fail("Unknown attachments: " + att1 + ", " + att2);
                }

                break;
            }
        }

        assertTrue(deadlocked);

        db.close();
    }

    @Test
    public void selfDeadlock() throws Throwable {
        Database db = Database.open(new DatabaseConfig().directPageAccess(false));
        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        txn1.lockUpgradable(ix.getId(), key1);
        txn2.lockUpgradable(ix.getId(), key2);

        try {
            txn2.lockUpgradable(ix.getId(), key1);
            fail();
        } catch (DeadlockException e) {
            // Not expected to work.
            throw e;
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.getOwnerAttachment());
        }

        try {
            txn1.lockUpgradable(ix.getId(), key2);
            fail();
        } catch (DeadlockException e) {
            // Not expected to work.
            throw e;
        } catch (LockTimeoutException e) {
            assertEquals("txn2", e.getOwnerAttachment());
        }

        // Verify owner attachment when not using an explicit transaction.
        try {
            ix.store(null, key1, key1);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.getOwnerAttachment());
        }
    }

    @Test
    public void sharedOwner() throws Throwable {
        // Not really a deadlock test. Checks for shared lock owner attachments.

        Database db = Database.open(new DatabaseConfig().directPageAccess(false));
        Index ix = db.openIndex("test");

        Transaction txn1 = db.newTransaction();
        txn1.attach("txn1");
        Transaction txn2 = db.newTransaction();
        txn2.attach("txn2");

        byte[] key = "key".getBytes();

        txn1.lockShared(ix.getId(), key);

        // No conflict.
        txn2.lockUpgradable(ix.getId(), key);
        txn2.unlock();

        try {
            txn2.lockExclusive(ix.getId(), key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("txn1", e.getOwnerAttachment());
        }

        txn2.lockShared(ix.getId(), key);
 
        Transaction txn3 = db.newTransaction();
        try {
            txn3.lockExclusive(ix.getId(), key);
            fail();
        } catch (LockTimeoutException e) {
            Object att = e.getOwnerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }

        // Can still get attachment even when not waited.
        txn3.lockTimeout(0, null);
        try {
            txn3.lockExclusive(ix.getId(), key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals(0, e.getTimeout());
            Object att = e.getOwnerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }

        // Verify owner attachment when not using an explicit transaction.
        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            Object att = e.getOwnerAttachment();
            assertTrue("txn1".equals(att) || "txn2".equals(att));
        }
    }

    @Test
    public void deleteTimeout() throws Throwable {
        // Regression test. Deleting an entry within a transaction would cause the attachment
        // check code to fail with a ClassCastException.

        Database db = Database.open(new DatabaseConfig().directPageAccess(false));
        Index ix = db.openIndex("test");

        byte[] key = "key".getBytes();

        ix.store(null, key, key);

        Transaction txn = db.newTransaction();
        ix.store(txn, key, null);

        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            assertNull(e.getOwnerAttachment());
        }

        // Also make sure that attachments can be retrieved.
        txn.attach("foo");
        try {
            ix.store(null, key, key);
            fail();
        } catch (LockTimeoutException e) {
            assertEquals("foo", e.getOwnerAttachment());
        }
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
