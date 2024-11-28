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

import java.io.IOException;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ReplicationTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ReplicationTest.class.getName());
    }

    protected DatabaseConfig decorate(DatabaseConfig config) throws Exception {
        config.maxReplicaThreads(8);
        return config;
    }

    @Before
    public void createTempDbs() throws Exception {
        mReplicaRepl = new SocketReplicator(null, 0);
        mLeaderRepl = new SocketReplicator("localhost", mReplicaRepl.getPort());

        mReplicaHandler = new Handler();
        mLeaderHandler = new Handler();

        var config = new DatabaseConfig()
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .customHandlers(Map.of("TestHandler", mLeaderHandler))
            .prepareHandlers(Map.of("TestHandler", mLeaderHandler))
            .replicate(mLeaderRepl);

        config = decorate(config);

        mLeader = newTempDatabase(getClass(), config);
        waitToBecomeLeader(mLeader, 10);

        config.customHandlers(Map.of("TestHandler", mReplicaHandler));
        config.prepareHandlers(Map.of("TestHandler", mReplicaHandler));
        config.replicate(mReplicaRepl);
        mReplica = newTempDatabase(getClass(), config);

        mLeaderWriter = mLeader.customWriter("TestHandler");
        mLeaderWriter2 = mLeader.prepareWriter("TestHandler");
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mReplicaRepl = null;
        mLeaderRepl = null;
        mReplica = null;
        mLeader = null;
    }

    private SocketReplicator mReplicaRepl, mLeaderRepl;
    private Database mReplica, mLeader;
    private Handler mReplicaHandler, mLeaderHandler;
    private CustomHandler mLeaderWriter;
    private PrepareHandler mLeaderWriter2;

    @Test
    public void hello() throws Exception {
        try {
            Index test = mReplica.openIndex("test");
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        final Index test = mLeader.openIndex("test");
        long id = test.id();
        test.store(null, "hello".getBytes(), "world".getBytes());

        fence();

        Index rtest = mReplica.openIndex("test");
        assertEquals(id, rtest.id());
        fastAssertArrayEquals("world".getBytes(), rtest.load(null, "hello".getBytes()));

        // Test various forms of delete. First, insert a few more records...
        test.store(null, "hello1".getBytes(), "world1".getBytes());
        test.store(null, "hello2".getBytes(), "world2".getBytes());
        test.store(null, "hello3".getBytes(), "world3".getBytes());
        fence();
        fastAssertArrayEquals("world1".getBytes(), rtest.load(null, "hello1".getBytes()));
        fastAssertArrayEquals("world2".getBytes(), rtest.load(null, "hello2".getBytes()));
        fastAssertArrayEquals("world3".getBytes(), rtest.load(null, "hello3".getBytes()));

        // ...now delete them.
        test.store(null, "hello".getBytes(), null);
        assertTrue(test.delete(null, "hello1".getBytes()));
        Cursor c = test.newCursor(null);
        c.find("hello2".getBytes());
        c.store(null);
        c.close();
        Transaction txn = mLeader.newTransaction();
        test.store(txn, "xxx".getBytes(), "xxx".getBytes());
        c = test.newCursor(txn);
        c.find("hello3".getBytes());
        c.store(null);
        txn.commit();
        c.close();

        fence();
        assertNull(rtest.load(null, "hello".getBytes()));
        assertNull(rtest.load(null, "hello1".getBytes()));
        assertNull(rtest.load(null, "hello2".getBytes()));
        assertNull(rtest.load(null, "hello3".getBytes()));
        fastAssertArrayEquals("xxx".getBytes(), rtest.load(null, "xxx".getBytes()));
    }

    @Test
    public void largeStore() throws Exception {
        final Index test = mLeader.openIndex("test");

        byte[] message = new byte[1_000_000];
        new Random().nextBytes(message);

        test.store(null, "hello".getBytes(), message);
        test.store(null, "hello2".getBytes(), "world".getBytes());

        fence();

        Index rtest = mReplica.openIndex("test");
        fastAssertArrayEquals(message, rtest.load(null, "hello".getBytes()));
        fastAssertArrayEquals("world".getBytes(), rtest.load(null, "hello2".getBytes()));
    }

    @Test
    public void lockRelease() throws Exception {
        // Test that shared and upgradable locks are released while waiting for a transaction
        // to fully commit.

        Index lix = mLeader.openIndex("test");
        for (int i=0; i<3; i++) {
            lix.store(null, ("k" + i).getBytes(), ("v" + i).getBytes());
        }

        Transaction txn = mLeader.newTransaction();
        lix.store(txn, "k0".getBytes(), "!".getBytes());
        lix.load(txn, "k1".getBytes()); // upgradable lock
        txn.lockMode(LockMode.REPEATABLE_READ);
        lix.load(txn, "k2".getBytes()); // read lock
        mLeaderRepl.suspendCommit(true);
        txn.commit();

        // Can obtain exclusive locks only for the unmodified keys.
        Transaction txn2 = mLeader.newTransaction();
        try {
            txn2.lockExclusive(lix.id(), "k0".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }
        txn2.lockExclusive(lix.id(), "k1".getBytes());
        txn2.lockExclusive(lix.id(), "k2".getBytes());
        txn2.exit();

        mLeaderRepl.suspendCommit(false);
        fence();
        Index rix = mReplica.openIndex("test");

        fastAssertArrayEquals("!".getBytes(), rix.load(null, "k0".getBytes()));
    }

    @Test
    public void interruptCommit() throws Exception {
        Index lix = mLeader.openIndex("test");
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        byte[] hello = "hello".getBytes();
        lix.store(null, key, value);

        var exRef = new AtomicReference<Throwable>();

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                Transaction txn = mLeader.newTransaction();
                txn.durabilityMode(DurabilityMode.SYNC);
                lix.store(txn, key, hello);
                txn.flush();
                mLeaderRepl.suspendCommit(true);
                txn.commit();
            } catch (Throwable e) {
                exRef.set(e);
            }
        }));

        sleep(1000);
        t.interrupt();
        t.join();

        assertTrue(exRef.get() instanceof ConfirmationInterruptedException);

        try {
            lix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
            // Still pending.
        }

        mLeaderRepl.suspendCommit(false);

        // Commit should complete and not have rolled back.
        fastAssertArrayEquals(hello, lix.load(null, key));

        // Replica should see the same thing.
        Index rix = mReplica.openIndex("test");
        fastAssertArrayEquals(hello, rix.load(null, key));
    }

    @Test
    public void unsafe() throws Exception {
        // Replication with unsafe locking is supported, but if the application hasn't
        // performed it's own locking on the index entry, the outcome is undefined. Race
        // conditions cannot be avoided.

        final Index test = mLeader.openIndex("test");

        Transaction txn = mLeader.newTransaction();
        txn.lockMode(LockMode.UNSAFE);
        test.store(txn, "hello".getBytes(), "world".getBytes());
        txn.commit();

        fence();

        Index rtest = mReplica.openIndex("test");
        fastAssertArrayEquals("world".getBytes(), rtest.load(null, "hello".getBytes()));

        txn = mLeader.newTransaction();
        txn.lockMode(LockMode.UNSAFE);
        test.store(txn, "hello".getBytes(), null);
        txn.commit();

        fence();

        assertNull(rtest.load(null, "hello".getBytes()));
    }

    @Test
    public void renameIndex() throws Exception {
        Index lix = mLeader.openIndex("test");
        lix.store(null, "hello".getBytes(), "world".getBytes());
        
        fence();

        Index rix = mReplica.openIndex("test");

        mLeader.renameIndex(lix, "newname");

        fence();

        assertEquals("newname", rix.nameString());
        rix.close();
        assertNull(mReplica.findIndex("test"));
        rix = mReplica.openIndex("newname");
        assertEquals("newname", rix.nameString());
        fastAssertArrayEquals("world".getBytes(), rix.load(null, "hello".getBytes()));
    }

    @Test
    public void interruptRenameIndex() throws Exception {
        Index lix = mLeader.openIndex("test");
        lix.store(null, "hello".getBytes(), "world".getBytes());
        
        fence();

        Index rix = mReplica.openIndex("test");

        mLeaderRepl.suspendCommit(true);
        var exRef = new AtomicReference<Throwable>();

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                mLeader.renameIndex(lix, "newName");
            } catch (Throwable e) {
                exRef.set(e);
            }
        }));

        sleep(1000);
        t.interrupt();
        t.join();

        assertTrue(exRef.get() instanceof ConfirmationInterruptedException);

        mLeaderRepl.suspendCommit(false);
        fence();

        assertEquals("newName", rix.nameString());

        for (int i=10; --i>=0; ) {
            try {
                assertEquals("newName", lix.nameString());
                break;
            } catch (AssertionError e) {
                if (i == 0) {
                    throw e;
                }
            }
            // Wait for background confirmation task to finish.
            sleep(1000);
        }
    }

    @Test
    public void deleteIndex() throws Exception {
        Index lix = mLeader.openIndex("test");
        lix.store(null, "hello".getBytes(), "world".getBytes());
        
        fence();

        Index rix = mReplica.openIndex("test");

        mLeader.deleteIndex(lix);

        fence();

        try {
            assertNull(rix.load(null, "hello".getBytes()));
            fail();
        } catch (DeletedIndexException e) {
            // Expected.
        }

        try {
            lix.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (DeletedIndexException e) {
            // Expected.
        }

        try {
            rix.store(null, "hello".getBytes(), "world".getBytes());
        } catch (DeletedIndexException e) {
            // Expected.
        }
    }

    @Test
    public void replicaLock() throws Exception {
        // Verifies that a replica can lock a record and stall replication processing.

        Index lix = mLeader.openIndex("test");
        fence();
        Index rix = mReplica.openIndex("test");
        Transaction rtxn = mReplica.newTransaction();

        long start = System.currentTimeMillis();

        var stuck = new Thread(() -> {
            try {
                try {
                    rix.lockShared(rtxn, "hello".getBytes());
                    sleep(1000);
                } finally {
                    rtxn.exit();
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        });

        stuck.start();
        while (stuck.getState() == Thread.State.RUNNABLE);

        lix.store(null, "hello".getBytes(), "world".getBytes());

        fence();
        long end = System.currentTimeMillis();

        assertTrue((end - start) > 900);

        fastAssertArrayEquals("world".getBytes(), rix.load(null, "hello".getBytes()));
    }

    @Test
    public void deleteLock() throws Exception {
        // Verifies that an unconditional delete of a non-existent record replicates a lock.

        final byte[] key = "hello".getBytes();
        final Index lix = mLeader.openIndex("test");
        fence();
        final Index rix = mReplica.openIndex("test");

        // With a cursor.
        {
            Transaction ltxn = mLeader.newTransaction();
            Cursor c = lix.newCursor(ltxn);
            c.find(key);
            c.store(null);
            mustLock(ltxn, rix, key);
        }

        // With a direct store.
        {
            Transaction ltxn = mLeader.newTransaction();
            lix.store(ltxn, key, null);
            mustLock(ltxn, rix, key);
        }

        // With an insert.
        {
            Transaction ltxn = mLeader.newTransaction();
            assertTrue(lix.insert(ltxn, key, null));
            mustLock(ltxn, rix, key);
        }

        // Remaining operations must not replicate a lock, because the condition failed.

        // With an update.
        {
            Transaction ltxn = mLeader.newTransaction();
            assertFalse(lix.update(ltxn, key, key, null));
            mustNotLock(ltxn, rix, key);
        }

        // With a delete.
        {
            Transaction ltxn = mLeader.newTransaction();
            assertFalse(lix.delete(ltxn, key));
            mustNotLock(ltxn, rix, key);
        }

        // With a remove.
        {
            Transaction ltxn = mLeader.newTransaction();
            assertFalse(lix.remove(ltxn, key, key));
            mustNotLock(ltxn, rix, key);
        }
    }

    // Used by deleteLock test.
    private void mustLock(Transaction ltxn, Index rix, byte[] key)
        throws IOException, InterruptedException
    {
        assertEquals(LockResult.OWNED_EXCLUSIVE, ltxn.lockCheck(rix.id(), key));

        ltxn.flush();
        fence();

        try {
            rix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        // Rollback and release the lock.
        ltxn.exit();
        assertNull(rix.load(null, key));
    }

    // Used by deleteLock test.
    private void mustNotLock(Transaction ltxn, Index rix, byte[] key)
        throws IOException, InterruptedException
    {
        assertEquals(LockResult.OWNED_UPGRADABLE, ltxn.lockCheck(rix.id(), key));

        ltxn.flush();
        fence();

        assertNull(rix.load(null, key));

        ltxn.exit();
    }

    @Test
    public void explicitLock() throws Exception {
        // Verifies that explicit lock acquisitions are replicated.

        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        Index ix = mLeader.openIndex("test");
        Transaction txn = mLeader.newTransaction();
        assertEquals(LockResult.ACQUIRED, ix.lockShared(txn, k1));
        assertEquals(LockResult.ACQUIRED, ix.lockUpgradable(txn, k2));
        assertEquals(LockResult.ACQUIRED, ix.lockExclusive(txn, k3));

        txn.flush();
        fence();

        Index rix = mReplica.openIndex("test");
        Transaction rtxn = mReplica.newTransaction();

        // Shared lock isn't replicated.
        assertEquals(LockResult.ACQUIRED, rix.lockExclusive(rtxn, k1));

        // Upgradable lock isn't replicated.
        assertEquals(LockResult.ACQUIRED, rix.tryLockUpgradable(rtxn, k2, 0));
        rtxn.unlock();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k3, 0));

        assertEquals(LockResult.UPGRADED, ix.lockExclusive(txn, k2));

        txn.flush();
        fence();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k2, 0));

        txn.exit();
        fence();

        assertEquals(LockResult.OWNED_EXCLUSIVE, rix.lockExclusive(rtxn, k1));
        assertEquals(LockResult.ACQUIRED, rix.lockExclusive(rtxn, k2));
        assertEquals(LockResult.ACQUIRED, rix.lockExclusive(rtxn, k3));
    }

    @Test
    public void explicitLockScoped() throws Exception {
        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        Index ix = mLeader.openIndex("test");
        Transaction txn = mLeader.newTransaction();
        assertEquals(LockResult.ACQUIRED, ix.lockExclusive(txn, k1));
        txn.enter();
        assertEquals(LockResult.ACQUIRED, ix.lockExclusive(txn, k2));
        txn.enter();
        assertEquals(LockResult.ACQUIRED, ix.lockExclusive(txn, k3));

        txn.flush();
        fence();

        Index rix = mReplica.openIndex("test");
        Transaction rtxn = mReplica.newTransaction();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k1, 0));
        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k2, 0));
        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k3, 0));

        txn.exit();
        fence();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k1, 0));
        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k2, 0));
        assertEquals(LockResult.ACQUIRED, rix.lockShared(rtxn, k3));

        txn.exit();
        fence();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k1, 0));
        assertEquals(LockResult.ACQUIRED, rix.tryLockShared(rtxn, k2, 0));
        assertEquals(LockResult.OWNED_SHARED, rix.lockShared(rtxn, k3));

        txn.exit();
        fence();

        assertEquals(LockResult.ACQUIRED, rix.tryLockShared(rtxn, k1, 0));
        assertEquals(LockResult.OWNED_SHARED, rix.lockShared(rtxn, k2));
        assertEquals(LockResult.OWNED_SHARED, rix.lockShared(rtxn, k3));
    }

    @Test
    public void explicitLockScoped2() throws Exception {
        // Create a bunch of empty parent scopes first and then commit.

        byte[] k1 = "k1".getBytes();

        Index ix = mLeader.openIndex("test");
        Transaction txn = mLeader.newTransaction();
        txn.enter();
        txn.enter();
        txn.enter();
        assertEquals(LockResult.ACQUIRED, ix.lockExclusive(txn, k1));

        txn.flush();
        fence();

        Index rix = mReplica.openIndex("test");
        Transaction rtxn = mReplica.newTransaction();

        assertEquals(LockResult.TIMED_OUT_LOCK, rix.tryLockShared(rtxn, k1, 0));

        txn.commitAll();
        fence();

        assertEquals(LockResult.ACQUIRED, rix.tryLockShared(rtxn, k1, 0));
    }

    @Test
    public void readUncommitted() throws Exception {
        // Verifies that a replica can read an uncommitted change.

        // Large value to ensure that the TransactionContext flushes.
        var value = new byte[10000];

        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();
        lix.store(ltxn, "hello".getBytes(), "world".getBytes());
        lix.store(ltxn, "k2".getBytes(), value);
        fence();

        Index rix = mReplica.openIndex("test");

        try {
            rix.load(null, "hello".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("world".getBytes(), rix.load(Transaction.BOGUS, "hello".getBytes()));
        fastAssertArrayEquals(value, rix.load(Transaction.BOGUS, "k2".getBytes()));

        // Rollback.
        ltxn.exit();
        fence();

        assertNull(rix.load(null, "hello".getBytes()));
        assertNull(rix.load(null, "k2".getBytes()));
    }

    @Test
    public void readUncommittedWithFlush() throws Exception {
        // Verifies that a replica can read an uncommitted change.

        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();
        lix.store(ltxn, "hello".getBytes(), "world".getBytes());
        // Explicit flush of the TransactionContext.
        ltxn.flush();
        fence();

        Index rix = mReplica.openIndex("test");

        try {
            rix.load(null, "hello".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("world".getBytes(), rix.load(Transaction.BOGUS, "hello".getBytes()));

        // Rollback.
        ltxn.exit();
        fence();

        assertNull(rix.load(null, "hello".getBytes()));
    }

    @Test
    public void consistency() throws Exception {
        // Stream a bunch of changes over and verify consistency on the replica.

        final long seed = new Random().nextLong();
        final int count = 200_000;

        Index lix = mLeader.openIndex("test");
        var rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 100);
            byte[] value = randomStr(rnd, 10, 10000);
            lix.store(null, key, value);
        }

        fence();

        Index rix = mReplica.openIndex("test");
        assertEquals(count, rix.count(null, null));

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 100);
            byte[] value = randomStr(rnd, 10, 10000);
            fastAssertArrayEquals(value, rix.load(null, key));
        }
    }

    @Test
    public void lockOrdering() throws Exception {
        // Update the same key many times and verify that the final outcome is correct.

        Index lix = mLeader.openIndex("test");
        byte[] key = "hello".getBytes();
        var value = new byte[8];

        for (int i=0; i<200_000; i++) {
            Utils.increment(value, 0, value.length);
            Transaction txn = mLeader.newTransaction();
            txn.lockTimeout(10, TimeUnit.SECONDS);
            lix.store(txn, key, value);
            txn.commit();
        }

        fence();

        Index rix = mReplica.openIndex("test");
        assertEquals(1, rix.count(null, null));

        fastAssertArrayEquals(value, rix.load(null, key));
    }

    @Test
    public void custom() throws Exception {
        // Send a custom message.

        Transaction ltxn = mLeader.newTransaction();
        mLeaderWriter.redo(ltxn, "hello".getBytes(), 0, null);
        ltxn.commit();
        fence();

        fastAssertArrayEquals("hello".getBytes(), mReplicaHandler.mMessage);

        // Again with stronger durability.

        ltxn = mLeader.newTransaction(DurabilityMode.SYNC);
        mLeaderWriter.redo(ltxn, "hello!!!".getBytes(), 0, null);
        ltxn.commit();
        fence();

        fastAssertArrayEquals("hello!!!".getBytes(), mReplicaHandler.mMessage);
    }

    @Test
    public void customWithLock() throws Exception {
        // Send a custom message with a lock.

        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();

        try {
            mLeaderWriter.redo(ltxn, "hello".getBytes(), lix.id(), "key".getBytes());
            fail();
        } catch (IllegalStateException e) {
            // Expected because lock isn't held.
        }
        
        ltxn.lockExclusive(lix.id(), "key".getBytes());
        mLeaderWriter.redo(ltxn, "hello".getBytes(), lix.id(), "key".getBytes());

        ltxn.flush();
        fence();

        fastAssertArrayEquals("hello".getBytes(), mReplicaHandler.mMessage);
        assertEquals(lix.id(), mReplicaHandler.mIndexId);
        fastAssertArrayEquals("key".getBytes(), mReplicaHandler.mKey);

        // Key is locked.
        Index rix = mReplica.openIndex("test");
        try {
            rix.load(null, "key".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        ltxn.commit();
        fence();

        assertNull(rix.load(null, "key".getBytes()));
    }

    @Test
    public void nestedRollback() throws Exception {
        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();
        lix.store(ltxn, "k1".getBytes(), "v1".getBytes());
        ltxn.enter();
        lix.store(ltxn, "k2".getBytes(), "v2".getBytes());
        // Explicit flush of the TransactionContext.
        ltxn.flush();
        fence();

        Index rix = mReplica.openIndex("test");

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("v1".getBytes(), rix.load(Transaction.BOGUS, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), rix.load(Transaction.BOGUS, "k2".getBytes()));

        // Rollback one scope.
        ltxn.exit();
        fence();

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("v1".getBytes(), rix.load(Transaction.BOGUS, "k1".getBytes()));
        assertNull(rix.load(null, "k2".getBytes()));

        ltxn.commit();
        fence();

        fastAssertArrayEquals("v1".getBytes(), rix.load(null, "k1".getBytes()));
        assertNull(rix.load(null, "k2".getBytes()));
    }

    @Test
    public void nestedCommit() throws Exception {
        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();
        lix.store(ltxn, "k1".getBytes(), "v1".getBytes());
        ltxn.enter();
        lix.store(ltxn, "k2".getBytes(), "v2".getBytes());
        // Explicit flush of the TransactionContext.
        ltxn.flush();
        fence();

        Index rix = mReplica.openIndex("test");

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("v1".getBytes(), rix.load(Transaction.BOGUS, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), rix.load(Transaction.BOGUS, "k2".getBytes()));

        // Commit one scope.
        ltxn.commit();
        ltxn.exit();
        fence();

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals("v1".getBytes(), rix.load(Transaction.BOGUS, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), rix.load(Transaction.BOGUS, "k2".getBytes()));

        ltxn.commit();
        fence();

        fastAssertArrayEquals("v1".getBytes(), rix.load(null, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), rix.load(null, "k2".getBytes()));
    }

    @Test
    public void nestedStoreCommit() throws Exception {
        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();

        ltxn.enter();
        lix.store(ltxn, "k1".getBytes(), "v1".getBytes());
        Cursor c = lix.newCursor(ltxn);
        c.find("k2".getBytes());
        c.commit("v2".getBytes());

        // Need explicit flush because nested commits don't flush immediately.
        ltxn.flush();

        fence();
        Index rix = mReplica.openIndex("test");

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        ltxn.exit();
        ltxn.commit();
        fence();

        fastAssertArrayEquals("v1".getBytes(), rix.load(null, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), rix.load(null, "k2".getBytes()));
    }

    @Test
    public void autoCommitUpdate() throws Exception {
        Index lix = mLeader.openIndex("test");

        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        byte[] v2 = "v2".getBytes();

        lix.store(null, k1, v1);
        assertTrue(lix.update(null, k1, v2));
        fence();

        fastAssertArrayEquals(v2, lix.load(null, k1));

        Index rix = mReplica.openIndex("test");
        fastAssertArrayEquals(v2, rix.load(null, k1));
    }

    @Test
    public void autoCommitRollback() throws Exception {
        Index lix = mLeader.openIndex("test");

        lix.store(null, "k1".getBytes(), "v1".getBytes());
        disableLeader();
        try {
            // Auto-commit will force a flush, even when using NO_FLUSH mode.
            lix.store(null, "k2".getBytes(), "v2".getBytes());
        } catch (UnmodifiableReplicaException e) {
            // Ignore.
        }

        fastAssertArrayEquals("v1".getBytes(), lix.load(null, "k1".getBytes()));
        assertNull(lix.load(null, "k2".getBytes()));

        Index rix = mReplica.openIndex("test");

        fastAssertArrayEquals("v1".getBytes(), rix.load(null, "k1".getBytes()));
        assertNull(rix.load(null, "k2".getBytes()));
    }

    @Test
    public void autoCommitRollbackWithCursor() throws Exception {
        Index lix = mLeader.openIndex("test");
        Transaction ltxn = mLeader.newTransaction();

        Cursor c = lix.newCursor(ltxn);
        c.find("k1".getBytes());
        c.commit("v1".getBytes());

        disableLeader();

        c = lix.newCursor(ltxn);
        c.find("k2".getBytes());
        try {
            // Commit will force a flush, even when using NO_FLUSH mode.
            c.commit("v2".getBytes());
        } catch (UnmodifiableReplicaException e) {
            // Ignore.
        }

        fastAssertArrayEquals("v1".getBytes(), lix.load(null, "k1".getBytes()));
        assertNull(lix.load(null, "k2".getBytes()));

        Index rix = mReplica.openIndex("test");

        fastAssertArrayEquals("v1".getBytes(), rix.load(null, "k1".getBytes()));
        assertNull(rix.load(null, "k2".getBytes()));
    }

    @Test
    public void cursorRegister() throws Exception {
        // Basic testing of registered cursors.

        Index leader = mLeader.openIndex("test");
        fence();
        Index replica = mReplica.openIndex("test");

        Cursor c = leader.newCursor(null);
        assertTrue(c.register());
        c.reset();

        Transaction txn = mLeader.newTransaction();
        c = leader.newCursor(txn);
        assertTrue(c.register());
        c.reset();
        txn.reset();

        c = replica.newCursor(null);
        assertFalse(c.register());
        // Replicas don't redo.
        c.reset();

        for (int q=0; q<2; q++) {
            String prefix;
            if (q == 0) {
                txn = null;
                prefix = "null-";
            } else {
                txn = mLeader.newTransaction();
                prefix = "txn-";
            }

            c = leader.newCursor(txn);
            c.register();
            for (int i=0; i<1000; i++) {
                c.findNearby((prefix + "key-" + i).getBytes());
                c.store((prefix + "value-" + i).getBytes());
            }
            c.reset();

            if (txn != null) {
                txn.commit();
            }

            fence();

            c = replica.newCursor(null);
            for (int i=0; i<1000; i++) {
                c.findNearby((prefix + "key-" + i).getBytes());
                fastAssertArrayEquals((prefix + "value-" + i).getBytes(), c.value());
            }
            c.reset();

            // Now scan and update.
            c = leader.newCursor(txn);
            for (c.first(), c.register(); c.key() != null; c.next()) {
                c.store((new String(c.value()) + "-updated").getBytes());
            }
            c.reset();

            if (txn != null) {
                txn.commit();
            }

            fence();

            c = replica.newCursor(null);
            for (int i=0; i<1000; i++) {
                c.findNearby((prefix + "key-" + i).getBytes());
                fastAssertArrayEquals((prefix + "value-" + i + "-updated").getBytes(), c.value());
            }
            c.reset();
        }
    }

    @Test
    public void cursorRegisterNoUse() throws Exception {
        // Test a registered cursor which isn't actually used for anything.

        Index leader = mLeader.openIndex("test");
        fence();
        Index replica = mReplica.openIndex("test");

        Transaction txn = mLeader.newTransaction();
        Cursor c = leader.newCursor(txn);
        assertTrue(c.register());
        c.reset();
        leader.store(txn, "hello".getBytes(), "world".getBytes());
        txn.flush();
        txn.commit();

        fence();

        fastAssertArrayEquals("world".getBytes(), replica.load(null, "hello".getBytes()));
    }

    @Test
    public void cursorRegisterTxnSwitch() throws Exception {
        // Test a registered cursor which switches transaction linkage. This can cause another
        // worker to be assigned.

        Index leader = mLeader.openIndex("test");
        fence();
        Index replica = mReplica.openIndex("test");

        // Locks records on the replica side to ensure that the workers get stalled, causing
        // tasks to be enqueued in all of them.
        Transaction holder = mReplica.newTransaction();
        holder.lockMode(LockMode.REPEATABLE_READ);
        for (int i=0; i<1000; i++) {
            replica.load(holder, ("key-" + i).getBytes());
        }

        Cursor c = leader.newCursor(null);
        c.register();

        var txns = new Transaction[1000];
        for (int i=0; i<txns.length; i++) {
            txns[i] = mLeader.newTransaction();
            leader.store(txns[i], ("key-" + i).getBytes(), ("value-" + i).getBytes());
            c.link(txns[i]);
            c.findNearby(("akey-" + i).getBytes());
            c.store(("value-" + i).getBytes());
        }

        for (int i=0; i<txns.length; i++) {
            txns[i].flush();
        }

        for (Transaction txn : txns) {
            txn.commit();
        }

        // All worker threads should be stalled at this point. Wait to be sure, and then
        // release the locks.
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                holder.reset();
            } catch (Exception e) {
                // Ignore.
            }
        }).start();

        fence();

        for (int i=0; i<1000; i++) {
            fastAssertArrayEquals(("value-" + i).getBytes(),
                                  replica.load(null, ("key-" + i).getBytes()));
            fastAssertArrayEquals(("value-" + i).getBytes(),
                                  replica.load(null, ("akey-" + i).getBytes()));
        }
    }

    @Test
    public void valueWrite() throws Exception {
        // Basic test of writing to a value in chunks with an auto-commit transaction.

        Index test = mLeader.openIndex("test");

        long seed = 98250983;
        var rnd = new Random(seed);

        final int chunk = 100;
        final int length = 1000 * chunk;
        final var b = new byte[chunk];

        Cursor c = test.newAccessor(null, "key1".getBytes());
        for (int i=0; i<length; i+=chunk) {
            rnd.nextBytes(b);
            c.valueWrite(i, b, 0, b.length);
        }
        c.reset();

        c = test.newAccessor(null, "key2".getBytes());
        for (int i=0; i<length; i+=chunk) {
            rnd.nextBytes(b);
            c.valueWrite(i, b, 0, b.length);
        }
        c.reset();

        fence();

        Index replica = mReplica.openIndex("test");
        rnd = new Random(seed);
        final var buf = new byte[chunk];

        for (String key : new String[] {"key1", "key2"}) {
            c = replica.newAccessor(null, key.getBytes());
            assertEquals(length, c.valueLength());
            int i = 0;
            for (; i<length; i+=chunk) {
                rnd.nextBytes(b);
                int amt = c.valueRead(i, buf, 0, buf.length);
                assertEquals(chunk, amt);
                fastAssertArrayEquals(b, buf);
            }
            assertEquals(0, c.valueRead(i, buf, 0, buf.length));
            c.reset();
        }
    }

    @Test
    public void valueClear() throws Exception {
        // Basic test of clearing a value range.

        Index test = mLeader.openIndex("test");

        byte[] key = "key".getBytes();
        var value = new byte[100_000];
        new Random(2923578).nextBytes(value);

        test.store(null, key, value);

        Cursor c = test.newAccessor(null, key);
        c.valueClear(5000, 50_000);
        c.close();

        fence();

        Arrays.fill(value, 5000, 5000 + 50_000, (byte) 0);
        fastAssertArrayEquals(value, test.load(null, key));

        Index replica = mReplica.openIndex("test");
        fastAssertArrayEquals(value, replica.load(null, key));
    }

    @Test
    public void valueSetLength() throws Exception {
        Index test = mLeader.openIndex("test");

        byte[] key = "key".getBytes();
        var value = new byte[100_000];
        new Random(2923578).nextBytes(value);

        test.store(null, key, value);

        fence();

        Index replica = mReplica.openIndex("test");
        fastAssertArrayEquals(value, replica.load(null, key));

        Cursor c = test.newAccessor(null, key);
        c.valueLength(5000);
        c.load();
        value = c.value();
        c.close();
        assertEquals(5000, value.length);

        fence();

        fastAssertArrayEquals(value, replica.load(null, key));
    }

    @Test
    public void scopeRollbackLeadershipLost() throws Exception {
        Index ix = mLeader.openIndex("test");
        Transaction txn = mLeader.newTransaction();
        ix.store(txn, "k1".getBytes(), "v1".getBytes());
        txn.enter();
        ix.store(txn, "k2".getBytes(), "v2".getBytes());
        txn.flush();

        disableLeader();

        // Force early internal detection of unmodifiable state. Auto-commit will force a
        // flush, even when using NO_FLUSH mode.
        try {
            ix.store(null, "x".getBytes(), null);
        } catch (UnmodifiableReplicaException e) {
            // Ignore.
        }

        // Rollback the scopes, which will attempt to write to the redo log.
        txn.exit();
        txn.exit();

        assertNull(ix.load(null, "k1".getBytes()));
        assertNull(ix.load(null, "k2".getBytes()));

        Index rix = mReplica.openIndex("test");

        // Locks cannot be obtained because without a leader, lingering transactions cannot be
        // rolled back in the log.

        try {
            rix.load(null, "k1".getBytes());
            fail();
        } catch (LockTimeoutException e) {
        }

        try {
            rix.load(null, "k2".getBytes());
            fail();
        } catch (LockTimeoutException e) {
        }
    }

    @Test
    public void cursorAutoCommit() throws Exception {
        Index ix = mLeader.openIndex("test");
        Cursor c = ix.newCursor(null);
        c.find("hello".getBytes());
        c.commit("world".getBytes());

        fence();

        Index rix = mReplica.openIndex("test");
        fastAssertArrayEquals("world".getBytes(), rix.load(null, "hello".getBytes()));
    }

    @Test
    public void storeCommitNoUndo() throws Exception {
        Index ix = mLeader.openIndex("test");
        fence();
        Index rix = mReplica.openIndex("test");

        rix.store(Transaction.BOGUS, "hello".getBytes(), "world".getBytes());

        Transaction txn = mLeader.newTransaction();
        Cursor c = ix.newCursor(txn);
        c.find("hello".getBytes());
        // Commit of a delete which does nothing generates no undo log entry. A redo log entry
        // is always generated, however.
        c.commit(null);

        fence();

        // Verify that redo log entry was generated.
        assertNull(rix.load(null, "hello".getBytes()));
    }

    @Test
    public void assignTransactionId() throws Exception {
        // Simple test which improves code coverage. Storing against a registered cursor should
        // assign a transaction id if necessary, but this only happens when storing null
        // against a non-existent entry. Otherwise, writing to the undo log would create the
        // transaction id first.

        Index ix = mLeader.openIndex("test");
        fence();
        Index rix = mReplica.openIndex("test");

        rix.store(Transaction.BOGUS, "hello".getBytes(), "world".getBytes());

        Transaction txn = mLeader.newTransaction();
        Cursor c = ix.newCursor(txn);
        c.find("hello".getBytes());
        c.register();
        // Store of a delete which does nothing generates no undo log entry. A redo log entry
        // is always generated, however.
        c.store(null);
        txn.commit();

        fence();

        // Verify that redo log entry was generated.
        assertNull(rix.load(null, "hello".getBytes()));
    }

    @Test
    public void prepareRollback() throws Exception {
        // Tests that a prepared transaction cannot be immediately rolled back when leadership
        // is lost.

        Index ix = mLeader.openIndex("test");
        fence();
        Index rix = mReplica.openIndex("test");

        Transaction txn = mLeader.newTransaction();
        byte[] key = "hello".getBytes();
        ix.store(txn, key, "world".getBytes());
        byte[] key2 = "hello2".getBytes();
        ix.load(txn, key2);
        assertEquals(LockResult.OWNED_UPGRADABLE, txn.lockCheck(ix.id(), key2));
        mLeaderWriter2.prepare(txn, null);

        // Cannot wait for ReplController.doSwitchToReplica task to finish, because it hangs
        // waiting for the prepared transaction to exit (awaitPreparedTransactions).
        disableLeader(false);

        try {
            txn.exit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        try {
            ix.load(null, key);
            fail();
        } catch (LockTimeoutException e) {
            // Exclusive lock is still held.
        }

        {
            // Verify that upgradable lock for key2 was released.
            Transaction txn2 = mLeader.newTransaction();
            assertNull(ix.load(txn2, key2));
            txn2.reset();
        }

        fastAssertArrayEquals("world".getBytes(), ix.load(Transaction.BOGUS, key));

        try {
            txn.check();
            fail();
        } catch (InvalidTransactionException e) {
            // Transaction should be borked because all state was transferred away.
        }
    }

    @Test
    public void anonymousIndex() throws Exception {
        // Tests that an anonymous index is created and replicated as expected.

        Transaction txn = mLeader.newTransaction();
        var ids = new long[1];
        ((LocalDatabase) mLeader).createSecondaryIndexes(txn, 0, ids, () -> {});
        fence();

        Index leaderIx = mLeader.indexById(ids[0]);
        Index replicaIx = mReplica.indexById(ids[0]);

        leaderIx.store(null, "hello".getBytes(), "world".getBytes());
        fence();
        fastAssertArrayEquals("world".getBytes(), replicaIx.load(null, "hello".getBytes()));
    }

    @Test
    public void redoListener() throws Exception {
        Index leaderIx = mLeader.openIndex("test");
        fence();
        Index replicaIx = mReplica.openIndex("test");

        class Listener implements RedoListener {
            volatile boolean gotIt;
            DurabilityMode mode;
            Index ix;
            byte[] key, value;

            @Override
            public synchronized void store(Transaction txn, Index ix, byte[] key, byte[] value) {
                this.mode = txn.durabilityMode();
                this.ix = ix;
                this.key = key;
                this.value = value;
                gotIt = true;
                notifyAll();
            }

            synchronized void waitForIt() throws InterruptedException {
                while (!gotIt) {
                    wait();
                }
                gotIt = false;
            }
        }

        var listener1 = new Listener();

        assertTrue(((LocalDatabase) mReplica).addRedoListener(listener1));
        assertFalse(((LocalDatabase) mReplica).addRedoListener(listener1));

        leaderIx.store(null, "hello".getBytes(), "world".getBytes());

        listener1.waitForIt();
        assertEquals(DurabilityMode.NO_REDO, listener1.mode);
        assertEquals(replicaIx, listener1.ix);
        fastAssertArrayEquals("hello".getBytes(), listener1.key);
        fastAssertArrayEquals("world".getBytes(), listener1.value);

        try (Cursor c = leaderIx.newCursor(null)) {
            c.register();
            c.findNearby("hello2".getBytes());
            c.store("world2".getBytes());
        }

        listener1.waitForIt();
        assertEquals(DurabilityMode.NO_REDO, listener1.mode);
        assertEquals(replicaIx, listener1.ix);
        fastAssertArrayEquals("hello2".getBytes(), listener1.key);
        fastAssertArrayEquals("world2".getBytes(), listener1.value);

        var listener2 = new Listener();

        assertTrue(((LocalDatabase) mReplica).addRedoListener(listener2));

        leaderIx.store(null, "hello3".getBytes(), "world3".getBytes());

        for (Listener l : new Listener[] {listener1, listener2}) {
            l.waitForIt();
            assertEquals(DurabilityMode.NO_REDO, l.mode);
            assertEquals(replicaIx, l.ix);
            fastAssertArrayEquals("hello3".getBytes(), l.key);
            fastAssertArrayEquals("world3".getBytes(), l.value);
        }

        assertTrue(((LocalDatabase) mReplica).removeRedoListener(listener1));
        assertFalse(((LocalDatabase) mReplica).removeRedoListener(listener1));

        leaderIx.store(null, "hello4".getBytes(), "world4".getBytes());
        listener2.waitForIt();
        fastAssertArrayEquals("hello4".getBytes(), listener2.key);
        fastAssertArrayEquals("world4".getBytes(), listener2.value);
        assertFalse(listener1.gotIt);

        assertTrue(((LocalDatabase) mReplica).removeRedoListener(listener2));

        leaderIx.store(null, "hello5".getBytes(), "world5".getBytes());
        fence();
        assertFalse(listener1.gotIt);
        assertFalse(listener2.gotIt);

        assertTrue(((LocalDatabase) mReplica).addRedoListener(listener1));
        leaderIx.store(null, "hello6".getBytes(), "world6".getBytes());
        listener1.waitForIt();
        fastAssertArrayEquals("hello6".getBytes(), listener1.key);
        fastAssertArrayEquals("world6".getBytes(), listener1.value);
    }

    /**
     * Writes a fence to the leader and waits for the replica to catch up.
     */
    private void fence() throws IOException, InterruptedException {
        byte[] message = ("fence:" + System.nanoTime()).getBytes();
        mLeaderRepl.writeControl(message);
        mReplicaRepl.waitForControl(message);
    }

    private void disableLeader() throws Exception {
        disableLeader(true);
    }

    private void disableLeader(boolean wait) throws Exception {
        fence();
        mLeaderRepl.disableWrites();

        if (wait) {
            // Wait for ReplController.doSwitchToReplica task to finish.
            for (int i=0; i<100; i++) {
                if (!mLeader.isLeader()) {
                    return;
                }
                Thread.sleep(100);
            }

            fail("still the leader");
        }
    }

    private static class Handler implements CustomHandler, PrepareHandler {
        volatile byte[] mMessage;
        volatile long mIndexId;
        volatile byte[] mKey;

        @Override
        public void redo(Transaction txn, byte[] message) {
            mMessage = message;
        }

        @Override
        public void redo(Transaction txn, byte[] message, long indexId, byte[] key) {
            mMessage = message;
            mIndexId = indexId;
            mKey = key;
        }

        @Override
        public void undo(Transaction txn, byte[] message) {
        }

        @Override
        public void prepare(Transaction txn, byte[] message) {
        }

        @Override
        public void prepareCommit(Transaction txn, byte[] message) {
        }
    }
}
