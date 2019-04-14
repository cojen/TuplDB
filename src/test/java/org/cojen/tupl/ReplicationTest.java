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

import java.io.IOException;

import java.util.Arrays;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.ext.TransactionHandler;

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
        config.directPageAccess(false).maxReplicaThreads(8);
        return config;
    }

    @Before
    public void createTempDbs() throws Exception {
        mReplicaMan = new SocketReplicationManager(null, 0);
        mLeaderMan = new SocketReplicationManager("localhost", mReplicaMan.getPort());

        mReplicaHandler = new Handler();
        mLeaderHandler = new Handler();

        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .customTransactionHandler(mLeaderHandler)
            .replicate(mLeaderMan);

        config = decorate(config);

        mLeader = newTempDatabase(getClass(), config);

        config.customTransactionHandler(mReplicaHandler);
        config.replicate(mReplicaMan);
        mReplica = newTempDatabase(getClass(), config);

        mLeaderMan.waitForLeadership();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mReplicaMan = null;
        mLeaderMan = null;
        mReplica = null;
        mLeader = null;
    }

    private SocketReplicationManager mReplicaMan, mLeaderMan;
    private Database mReplica, mLeader;
    private Handler mReplicaHandler, mLeaderHandler;

    @Test
    public void hello() throws Exception {
        try {
            Index test = mReplica.openIndex("test");
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        final Index test = mLeader.openIndex("test");
        long id = test.getId();
        test.store(null, "hello".getBytes(), "world".getBytes());

        fence();

        Index rtest = mReplica.openIndex("test");
        assertEquals(id, rtest.getId());
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

        assertEquals("newname", rix.getNameString());
        rix.close();
        assertNull(mReplica.findIndex("test"));
        rix = mReplica.openIndex("newname");
        assertEquals("newname", rix.getNameString());
        fastAssertArrayEquals("world".getBytes(), rix.load(null, "hello".getBytes()));
    }

    @Test
    public void deleteIndex() throws Exception {
        Index lix = mLeader.openIndex("test");
        lix.store(null, "hello".getBytes(), "world".getBytes());
        
        fence();

        Index rix = mReplica.openIndex("test");

        mLeader.deleteIndex(lix);

        fence();

        assertEquals(null, rix.load(null, "hello".getBytes()));

        try {
            lix.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (ClosedIndexException e) {
            // Expected.
        }

        try {
            rix.store(null, "hello".getBytes(), "world".getBytes());
        } catch (ClosedIndexException e) {
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

        Thread stuck = new Thread(() -> {
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
        assertEquals(LockResult.OWNED_EXCLUSIVE, ltxn.lockCheck(rix.getId(), key));

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
        assertEquals(LockResult.OWNED_UPGRADABLE, ltxn.lockCheck(rix.getId(), key));

        ltxn.flush();
        fence();

        assertNull(rix.load(null, key));

        ltxn.exit();
    }

    @Test
    public void readUncommitted() throws Exception {
        // Verifies that a replica can read an uncommitted change.

        // Large value to ensure that the TransactionContext flushes.
        byte[] value = new byte[10000];

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
        Random rnd = new Random(seed);
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
        byte[] value = new byte[8];

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
        ltxn.customRedo("hello".getBytes(), 0, null);
        ltxn.commit();
        fence();

        fastAssertArrayEquals("hello".getBytes(), mReplicaHandler.mMessage);

        // Again with stronger durability.

        ltxn = mLeader.newTransaction(DurabilityMode.SYNC);
        ltxn.customRedo("hello!!!".getBytes(), 0, null);
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
            ltxn.customRedo("hello".getBytes(), lix.getId(), "key".getBytes());
            fail();
        } catch (IllegalStateException e) {
            // Expected because lock isn't held.
        }
        
        ltxn.lockExclusive(lix.getId(), "key".getBytes());
        ltxn.customRedo("hello".getBytes(), lix.getId(), "key".getBytes());

        ltxn.flush();
        fence();

        fastAssertArrayEquals("hello".getBytes(), mReplicaHandler.mMessage);
        assertEquals(lix.getId(), mReplicaHandler.mIndexId);
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
        fence();
        mLeaderMan.disableWrites();
        lix.store(null, "k2".getBytes(), "v2".getBytes());

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

        fence();
        mLeaderMan.disableWrites();

        c = lix.newCursor(ltxn);
        c.find("k2".getBytes());
        c.commit("v2".getBytes());

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

        Transaction[] txns = new Transaction[1000];
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
        Random rnd = new Random(seed);

        final int chunk = 100;
        final int length = 1000 * chunk;
        final byte[] b = new byte[chunk];

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
        final byte[] buf = new byte[chunk];

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
        byte[] value = new byte[100_000];
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
        byte[] value = new byte[100_000];
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

    /**
     * Writes a fence to the leader and waits for the replica to catch up.
     */
    private void fence() throws IOException, InterruptedException {
        byte[] message = ("fence:" + System.nanoTime()).getBytes();
        long pos = mLeaderMan.writeControl(message);
        mReplicaMan.waitForControl(pos, message);
    }

    private static class Handler implements TransactionHandler {
        volatile byte[] mMessage;
        volatile long mIndexId;
        volatile byte[] mKey;

        @Override
        public void init(Database db) {
        }

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
        public void undo(byte[] message) {
        }
    }
}
