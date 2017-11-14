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

import java.util.Random;

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
        config.directPageAccess(false);
        return config;
    }

    @Before
    public void createTempDbs() throws Exception {
        mReplicaMan = new SocketReplicationManager(null, 0);
        mLeaderMan = new SocketReplicationManager("localhost", mReplicaMan.getPort());

        mReplicaHandler = new Handler();
        mLeaderHandler = new Handler();

        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(100_000_000)
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

        Index test = mLeader.openIndex("test");
        long id = test.getId();
        test.store(null, "hello".getBytes(), "world".getBytes());

        fence();

        test = mReplica.openIndex("test");
        assertEquals(id, test.getId());
        fastAssertArrayEquals("world".getBytes(), test.load(null, "hello".getBytes()));
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
            /* FIXME: caused by pending commit waiter backlog
[ERROR] lockOrdering(org.cojen.tupl.ReplicationTest)  Time elapsed: 24.77 s  <<< ERROR!
org.cojen.tupl.LockTimeoutException: Waited 1 second
        at org.cojen.tupl.Locker.failed(Locker.java:494)
        at org.cojen.tupl.Locker.lock(Locker.java:137)
        at org.cojen.tupl.TreeCursor.doLoad(TreeCursor.java:2469)
        at org.cojen.tupl.TreeCursor.find(TreeCursor.java:2006)
        at org.cojen.tupl.TreeCursor.doFind(TreeCursor.java:1778)
        at org.cojen.tupl.TxnTree.txnStore(TxnTree.java:55)
        at org.cojen.tupl.TxnTree.store(TxnTree.java:45)
        at org.cojen.tupl.ReplicationTest.lockOrdering(ReplicationTest.java:292)
        */
            lix.store(null, key, value);
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
    public void pendingTrash() throws Exception {
        // When a transaction is re-used, the pending transaction mustn't empty the trash of
        // the second usage. This verifies that a new transaction id is assigned when re-using
        // a transaction which had deleted fragmented values.

        Index test = mLeader.openIndex("test");

        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        Random rnd = new Random(76894235);

        byte[] big1 = new byte[100_000];
        rnd.nextBytes(big1);

        byte[] big2 = new byte[100_000];
        rnd.nextBytes(big2);

        test.store(null, key1, big1);
        test.store(null, key2, big2);
        fence();

        // Commit pending, with an item in the trash.
        mLeaderMan.suspendConfirmation(Long.MAX_VALUE);
        Transaction txn = mLeader.newTransaction();
        test.store(txn, key1, null);
        txn.commit();

        // Re-use the transaction, and delete another big value, but don't commit.
        test.store(txn, key2, null);
        assertNull(test.load(txn, key2));

        // Allow the pending transaction to finish, and then catch up.
        mLeaderMan.suspendConfirmation(0);
        fence();

        // Wait for leader pending transaction to complete and verify.
        assertNull(test.load(null, key1));

        // Rollback the re-used transaction, and the second big value should come back. The
        // first should be gone.
        txn.reset();
        fence();

        assertNull(test.load(null, key1));
        fastAssertArrayEquals(big2, test.load(null, key2));

        test = mReplica.openIndex("test");
        assertNull(test.load(null, key1));
        fastAssertArrayEquals(big2, test.load(null, key2));
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
        public void redo(Database db, Transaction txn, byte[] message) {
            mMessage = message;
        }

        @Override
        public void redo(Database db, Transaction txn, byte[] message, long indexId, byte[] key) {
            mMessage = message;
            mIndexId = indexId;
            mKey = key;
        }

        @Override
        public void undo(Database db, byte[] message) {
        }
    }
}
