/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.File;
import java.io.IOException;

import java.net.ServerSocket;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import java.util.function.Supplier;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.TestUtils;
import static org.cojen.tupl.TestUtils.fastAssertArrayEquals;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DatabaseReplicatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DatabaseReplicatorTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        if (mDatabases != null) {
            for (Database db : mDatabases) {
                if (db != null) {
                    db.close();
                }
            }
        }

        if (mSockets != null) {
            for (ServerSocket ss : mSockets) {
                Utils.closeQuietly(ss);
            }
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private ServerSocket[] mSockets;
    private File[] mReplBaseFiles;
    private ReplicatorConfig[] mReplConfigs;
    private StreamReplicator[] mReplicators;
    private DatabaseConfig[] mDbConfigs;
    private Database[] mDatabases;

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members) throws Exception {
        return startGroup(members, Role.NORMAL, null, false);
    }

    /**
     * @return first is the leader
     */
    private Database[] startGroup(int members, Role replicaRole,
                                  Supplier<PrepareHandler> handlerSupplier,
                                  boolean debug)
        throws Exception
    {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        mSockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            mSockets[i] = TestUtils.newServerSocket();
        }

        mReplBaseFiles = new File[members];
        mReplConfigs = new ReplicatorConfig[members];
        mReplicators = new StreamReplicator[members];
        mDbConfigs = new DatabaseConfig[members];
        mDatabases = new Database[members];

        for (int i=0; i<members; i++) {
            mReplBaseFiles[i] = TestUtils.newTempBaseFile(getClass()); 

            EventListener listener = debug ? EventListener.printTo(System.out) : null;

            mReplConfigs[i] = new ReplicatorConfig()
                .groupToken(1)
                .localSocket(mSockets[i])
                .baseFile(mReplBaseFiles[i])
                .eventListener(listener)
                .failoverLagTimeoutMillis(-1);

            if (i > 0) {
                mReplConfigs[i].addSeed(mSockets[0].getLocalSocketAddress());
                mReplConfigs[i].localRole(replicaRole);
            }

            mReplicators[i] = StreamReplicator.open(mReplConfigs[i]);

            ((Controller) mReplicators[i]).keepServerSocket();

            mDbConfigs[i] = new DatabaseConfig()
                .baseFile(mReplBaseFiles[i])
                .replicate(mReplicators[i])
                .eventListener(listener)
                .lockTimeout(5, TimeUnit.SECONDS);

            if (handlerSupplier != null) {
                mDbConfigs[i].prepareHandlers(Map.of("TestHandler", handlerSupplier.get()));
            }

            Database db = Database.open(mDbConfigs[i]);
            mDatabases[i] = db;

            readyCheck: {
                for (int trial=0; trial<100; trial++) {
                    TestUtils.sleep(100);

                    if (i == 0) {
                        try {
                            db.openIndex("control");
                            // Ensure that replicas obtain the index in the snapshot.
                            db.checkpoint();
                            break readyCheck;
                        } catch (UnmodifiableReplicaException e) {
                            // Not leader yet.
                        }
                    } else {
                        assertNotNull(db.openIndex("control"));
                        break readyCheck;
                    }
                }

                throw new AssertionError(i == 0 ? "No leader" : "Not joined");
            }
        }

        return mDatabases;
    }

    @Test
    public void basicTestOneMember() throws Exception {
        basicTest(1);
    }

    @Test
    public void basicTestThreeMembers() throws Exception {
        for (int i=3; --i>=0; ) {
            try {
                basicTest(3);
                break;
            } catch (UnmodifiableReplicaException e) {
                // Test is load sensitive and leadership is sometimes lost.
                // https://github.com/cojen/TuplDB/issues/70
                if (i <= 0) {
                    throw e;
                }
                teardown();
            }
        }
    }

    private void basicTest(int memberCount) throws Exception {
        Database[] dbs = startGroup(memberCount);

        Index ix0 = dbs[0].openIndex("test");

        for (int t=0; t<10; t++) {

            byte[] key = ("hello-" + t).getBytes();
            byte[] value = ("world-" + t).getBytes();
            ix0.store(null, key, value);

            fastAssertArrayEquals(value, ix0.load(null, key));

            for (int i=0; i<dbs.length; i++) {
                for (int q=100; --q>=0; ) {
                    byte[] actual = null;

                    try {
                        Index ix = dbs[i].openIndex("test");
                        actual = ix.load(null, key);
                        fastAssertArrayEquals(value, actual);
                        break;
                    } catch (UnmodifiableReplicaException e) {
                        // Index doesn't exist yet due to replication delay.
                        if (q == 0) {
                            throw e;
                        }
                    } catch (AssertionError e) {
                        // Value doesn't exist yet due to replication delay.
                        if (q == 0 || actual != null) {
                            throw e;
                        }
                    }

                    TestUtils.sleep(100);
                }
            }
        }
    }

    @Test
    public void txnPrepare() throws Exception {
        for (int i=3; --i>=0; ) {
            try {
                doTxnPrepare();
                break;
            } catch (UnmodifiableReplicaException e) {
                // Test is load sensitive and leadership is sometimes lost.
                // https://github.com/cojen/Tupl/issues/70
                if (i <= 0) {
                    throw e;
                }
                teardown();
            }
        }
    }

    private void doTxnPrepare() throws Exception {
        // Test that unfinished prepared transactions are passed to the new leader.

        TransferQueue<Database> recovered = new LinkedTransferQueue<>();

        Supplier<PrepareHandler> supplier = () -> new PrepareHandler() {
            private Database mDb;

            @Override
            public void init(Database db) {
                mDb = db;
            }

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                try {
                    recovered.transfer(mDb);

                    // Wait for the signal...
                    recovered.take();

                    // Modify the value before committing.
                    Index ix = mDb.openIndex("test");
                    Cursor c = ix.newCursor(txn);
                    c.find("hello".getBytes());
                    byte[] newValue = Arrays.copyOfRange(c.value(), 0, c.value().length + 1);
                    newValue[newValue.length - 1] = '!';
                    c.store(newValue);
                    c.reset();

                    txn.commit();
                } catch (Exception e) {
                    throw Utils.rethrow(e);
                }
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepare(txn, message);
            }
        };

        final int memberCount = 3;
        Database[] dbs = startGroup(memberCount, Role.NORMAL, supplier, false);

        Index ix0 = dbs[0].openIndex("test");

        // Wait for all members to be electable.
        allElectable: {
            int count = 0;
            for (int i=0; i<100; i++) {
                count = 0;
                for (StreamReplicator repl : mReplicators) {
                    if (repl.localRole() == Role.NORMAL) {
                        count++;
                    }
                }
                if (count >= mReplicators.length) {
                    break allElectable;
                }
                TestUtils.sleep(100);
            }

            fail("Not all members are electable: " + count);
        }

        Transaction txn = dbs[0].newTransaction();
        PrepareHandler handler = dbs[0].prepareWriter("TestHandler");
        byte[] key = "hello".getBytes();
        ix0.store(txn, key, "world".getBytes());
        handler.prepare(txn, null);

        // Close the leader and verify handoff.
        dbs[0].close();

        Database db = recovered.take();
        assertNotEquals(dbs[0], db);

        // Still locked.
        Index ix = db.openIndex("test");
        txn = db.newTransaction();
        assertEquals(LockResult.TIMED_OUT_LOCK, ix.tryLockShared(txn, key, 0));
        txn.reset();

        // Signal that the handler can finish the transaction.
        recovered.add(db);

        assertArrayEquals("world!".getBytes(), ix.load(null, key));

        // Verify replication.
        Database remaining = dbs[1];
        if (remaining == db) {
            remaining = dbs[2];
        }
        ix = remaining.openIndex("test");
        for (int i=10; --i>=0; ) {
            try {
                assertArrayEquals("world!".getBytes(), ix.load(null, key));
                break;
            } catch (Throwable e) {
                if (i <= 0) {
                    throw e;
                }
                TestUtils.sleep(100);
            }
        }
    }

    @Test
    public void largeWrite() throws Exception {
        Database[] dbs = startGroup(1);
        Database db = dbs[0];
        Index ix = db.openIndex("test");

        var value = new byte[100_000];
        Arrays.fill(value, 0, value.length, (byte) 0x7f); // illegal redo op

        byte[] key = "hello".getBytes();

        Transaction txn = db.newTransaction();
        Cursor c = ix.newCursor(txn);
        c.find(key);
        // This used to hang due to a bug. The commit index was too high, and so it wouldn't be
        // confirmed.
        c.commit(value);

        db.checkpoint();

        db = closeAndReopen(0);

        ix = db.openIndex("test");
        fastAssertArrayEquals(value, ix.load(null, key));

        db.close();
    }

    @Test
    public void valueWriteRecover() throws Exception {
        // Verifies that a checkpoint in the middle of a value write on the replica can still
        // properly recover the registered cursor.

        Database[] dbs = startGroup(2, Role.OBSERVER, null, false);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        var rnd = new Random();

        var part1 = new byte[1000];
        rnd.nextBytes(part1);

        var part2 = new byte[1000];
        rnd.nextBytes(part2);

        Transaction txn = leaderDb.newTransaction();
        byte[] key = "key1".getBytes();
        Cursor c = leaderIx.newAccessor(txn, key);
        c.valueWrite(0, part1, 0, part1.length);
        txn.flush();

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);

        Index replicaIx = replicaDb.openIndex("test");
        assertTrue(replicaIx.exists(Transaction.BOGUS, key));
        replicaDb.checkpoint();

        replicaDb = closeAndReopen(1);

        replicaIx = replicaDb.openIndex("test");
        assertTrue(replicaIx.exists(Transaction.BOGUS, key));

        // Finish writing and wait for replica to catch up.
        c.valueWrite(part1.length, part2, 0, part2.length);
        c.close();
        txn.commit();
        fence(leaderDb, replicaDb);

        var expect = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expect, 0, part1.length);
        System.arraycopy(part2, 0, expect, part1.length, part2.length);

        fastAssertArrayEquals(expect, leaderIx.load(null, key));
        fastAssertArrayEquals(expect, replicaIx.load(null, key));

        replicaDb.close();
    }

    @Test
    public void standbyLeader() throws Exception {
        // Test that a standby member can become an interim leader and prevent data loss.

        Database[] dbs = startGroup(2, Role.STANDBY, null, false);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);
        
        Index replicaIx = replicaDb.openIndex("test");

        leaderDb.suspendCheckpoints();
        replicaDb.suspendCheckpoints();
        leaderDb.checkpoint();
        replicaDb.checkpoint();

        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        leaderIx.store(null, key, value);

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);

        fastAssertArrayEquals(value, replicaIx.load(null, key));

        // Close and re-open leader, which doesn't have the key in the log. Must explicitly
        // close the replicator, to ensure that closing the database doesn't flush the log.
        mReplicators[0].close(); 
        leaderDb = closeAndReopen(0);
        leaderIx = leaderDb.openIndex("test");

        byte[] found = null;
        for (int i=0; i<100; i++) {
            found = leaderIx.load(null, key);
            if (found != null) {
                break;
            }
            TestUtils.sleep(1000);
        }

        fastAssertArrayEquals(value, found);

        value = "value!".getBytes();

        for (int i=0; i<10; i++) {
            try {
                leaderIx.store(null, key, value);
                break;
            } catch (UnmodifiableReplicaException e) {
                TestUtils.sleep(1000);
            }
        }

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);

        fastAssertArrayEquals(value, replicaIx.load(null, key));

        leaderDb.close();
    }

    @Test
    public void emergencyRecovery() throws Exception {
        // Test that database can be opened after the replication files are deleted. Anything
        // in them is lost.

        Database db = startGroup(1)[0];
        Index ix = db.openIndex("test");
        ix.store(null, "k1".getBytes(), "v1".getBytes());
        db.suspendCheckpoints();
        db.checkpoint();
        ix.store(null, "k2".getBytes(), "v2".getBytes());
        db.close();

        File baseFile = mReplBaseFiles[0];
        String prefix = baseFile.getName();
        baseFile.getParentFile().listFiles(file -> {
            String name = file.getName();
            if (name.startsWith(prefix) && !name.endsWith(".db")) {
                try {
                    Utils.delete(file);
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }
            return false;
        });

        db = closeAndReopen(0);
        ix = db.openIndex("test");

        fastAssertArrayEquals("v1".getBytes(), ix.load(null, "k1".getBytes()));
        assertNull(ix.load(null, "k2".getBytes()));

        db.close();
    }

    @Test
    public void explicitFailover() throws Exception {
        Database[] dbs = startGroup(2);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);
        
        Index replicaIx = replicaDb.openIndex("test");
        
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();

        leaderDb.failover();

        try {
            leaderIx.store(null, key, value);
            fail();
        } catch (UnmodifiableReplicaException e) {
        }

        boolean success = false;
        for (int i=0; i<10; i++) {
            try {
                replicaIx.store(null, key, value);
                success = true;
                break;
            } catch (UnmodifiableReplicaException e) {
            }
            TestUtils.sleep(1000);
        }

        assertTrue(success);

        // Wait for old leader to catch up.
        fence(replicaDb, leaderDb);

        fastAssertArrayEquals(value, leaderIx.load(null, key));
    }

    @Test
    public void prepareTransfer() throws Exception {
        prepareTransfer(false);
    }

    @Test
    public void prepareCommitTransfer() throws Exception {
        prepareTransfer(true);
    }

    private void prepareTransfer(boolean prepareCommit) throws Exception {
        // Prepared transaction should be transferred to replica and finish.

        var dbQueue = new LinkedBlockingQueue<Database>();
        var txnQueue = new LinkedBlockingQueue<Transaction>();

        Supplier<PrepareHandler> supplier = () -> new PrepareHandler() {
            private Database mDb;

            @Override
            public void init(Database db) {
                mDb = db;
            }

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                dbQueue.add(mDb);
                txnQueue.add(txn);
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepare(txn, message);
            }
        };

        Database[] dbs = startGroup(2, Role.NORMAL, supplier, false);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);
        
        Index replicaIx = replicaDb.openIndex("test");

        Transaction txn1 = leaderDb.newTransaction();
        PrepareHandler handler = leaderDb.prepareWriter("TestHandler");
        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        leaderIx.store(txn1, k1, v1);

        if (prepareCommit) {
            handler.prepareCommit(txn1, null);
            fastAssertArrayEquals(v1, leaderIx.load(null, k1));
        } else {
            handler.prepare(txn1, null);
            try {
                leaderIx.load(null, k1);
                fail();
            } catch (LockTimeoutException e) {
            }
        }

        leaderDb.failover();

        // Replica is now the leader and should have the transaction.

        assertEquals(replicaDb, dbQueue.take());
        Transaction txn2 = txnQueue.take();

        assertNotEquals(txn1, txn2);
        assertEquals(txn1.id(), txn2.id());

        fastAssertArrayEquals(v1, replicaIx.load(txn2, k1));

        byte[] k2 = "k2".getBytes();
        byte[] v2 = "v2".getBytes();
        replicaIx.store(txn2, k2, v2);

        if (prepareCommit) {
            fastAssertArrayEquals(v1, replicaIx.load(null, k1));
        } else {
            try {
                replicaIx.load(null, k1);
                fail();
            } catch (LockTimeoutException e) {
            }
        }

        txn2.commit();

        // Wait for old leader to catch up. This will fail at first because the old leader
        // transaction is stuck.
        boolean pass = true;
        try {
            fence(replicaDb, leaderDb, true);
            pass = false;
        } catch (AssertionError e) {
        }

        assertTrue(pass);

        try {
            txn1.commit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // This will unstick the transaction.
        }

        fence(replicaDb, leaderDb);

        // Verify that the old leader observes the committed changes.
        fastAssertArrayEquals(v1, leaderIx.load(null, k1));
        fastAssertArrayEquals(v2, leaderIx.load(null, k2));
    }

    @Test
    public void prepareTransferPingPong() throws Exception {
        // Prepared transaction should be transferred to replica, back to old leader, and then
        // finish.

        var dbQueue = new LinkedBlockingQueue<Database>();
        var txnQueue = new LinkedBlockingQueue<Transaction>();
        var msgQueue = new LinkedBlockingQueue<byte[]>();

        Supplier<PrepareHandler> supplier = () -> new PrepareHandler() {
            private Database mDb;

            @Override
            public void init(Database db) {
                mDb = db;
            }

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                dbQueue.add(mDb);
                txnQueue.add(txn);
                msgQueue.add(message);
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepare(txn, message);
            }
        };

        Database[] dbs = startGroup(2, Role.NORMAL, supplier, false);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);
        
        Index replicaIx = replicaDb.openIndex("test");

        Transaction txn1 = leaderDb.newTransaction();
        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        leaderIx.store(txn1, k1, v1);
        PrepareHandler handler = leaderDb.prepareWriter("TestHandler");
        handler.prepare(txn1, "message".getBytes());

        leaderDb.failover();

        // Must capture the id before it gets replaced.
        long txnId = txn1.id();

        try {
            txn1.commit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // This will unstick the transaction.
        }

        // Replica is now the leader and should have the transaction.

        assertEquals(replicaDb, dbQueue.take());
        Transaction txn2 = txnQueue.take();

        assertNotEquals(txn1, txn2);
        assertEquals(txnId, txn2.id());

        fastAssertArrayEquals("message".getBytes(), msgQueue.take());

        fastAssertArrayEquals(v1, replicaIx.load(txn2, k1));

        byte[] k2 = "k2".getBytes();
        byte[] v2 = "v2".getBytes();
        replicaIx.store(txn2, k2, v2);

        handler = replicaDb.prepareWriter("TestHandler");
        try {
            handler.prepare(txn2, null);
            fail();
        } catch (IllegalStateException e) {
            // Already prepared.
        }

        replicaDb.failover();

        try {
            txn2.commit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // This will unstick the transaction.
        }

        // Now the old leader is the leader again.

        assertEquals(leaderDb, dbQueue.take());
        Transaction txn3 = txnQueue.take();

        assertNotEquals(txn1, txn3);
        assertNotEquals(txn2, txn3);
        assertEquals(txnId, txn3.id());

        fastAssertArrayEquals("message".getBytes(), msgQueue.take());

        fastAssertArrayEquals(v1, leaderIx.load(txn3, k1));
        assertNull(leaderIx.load(txn3, k2));

        byte[] k3 = "k3".getBytes();
        byte[] v3 = "v3".getBytes();
        leaderIx.store(txn3, k3, v3);

        txn3.commit();

        fence(leaderDb, replicaDb);

        // Verify that leader and replica observe the committed changes. Note that v2 was
        // rolled back, because when the replica was acting as leader, it was unable to commit
        // v2. It tried to prepare the transaction again, but that's currently illegal.

        fastAssertArrayEquals(v1, leaderIx.load(null, k1));
        assertNull(leaderIx.load(null, k2));
        fastAssertArrayEquals(v3, leaderIx.load(null, k3));

        fastAssertArrayEquals(v1, replicaIx.load(null, k1));
        assertNull(replicaIx.load(null, k2));
        fastAssertArrayEquals(v3, replicaIx.load(null, k3));
    }

    @Test
    public void prepareBlank() throws Exception {
        prepareBlank(false);
    }

    @Test
    public void prepareCommitBlank() throws Exception {
        prepareBlank(true);
    }

    private void prepareBlank(boolean prepareCommit) throws Exception {
        // Test against a prepared transaction that has no changes. It should still ensure that
        // the transaction is created properly on the replica.

        var dbQueue = new LinkedBlockingQueue<Database>();
        var txnQueue = new LinkedBlockingQueue<Transaction>();
        var msgQueue = new LinkedBlockingQueue<byte[]>();
        var pcQueue = new LinkedBlockingQueue<Boolean>();

        Supplier<PrepareHandler> supplier = () -> new PrepareHandler() {
            private Database mDb;

            @Override
            public void init(Database db) {
                mDb = db;
            }

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                doPrepare(txn, message, false);
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                doPrepare(txn, message, true);
            }

            private void doPrepare(Transaction txn, byte[] message, boolean commit)
                throws IOException
            {
                dbQueue.add(mDb);
                txnQueue.add(txn);
                msgQueue.add(message);
                pcQueue.add(commit);
            }
        };

        Database[] dbs = startGroup(2, Role.NORMAL, supplier, false);
        Database leaderDb = dbs[0];
        Database replicaDb = dbs[1];

        Index leaderIx = leaderDb.openIndex("test");

        // Wait for replica to catch up.
        fence(leaderDb, replicaDb);
        
        Index replicaIx = replicaDb.openIndex("test");

        Transaction txn1 = leaderDb.newTransaction();
        PrepareHandler handler = leaderDb.prepareWriter("TestHandler");

        if (prepareCommit) {
            handler.prepareCommit(txn1, "message".getBytes());
        } else {
            handler.prepare(txn1, "message".getBytes());
        }

        leaderDb.failover();

        // Must capture the id before it gets replaced.
        long txnId = txn1.id();

        try {
            txn1.commit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // This will unstick the transaction.
        }

        // Replica is now the leader and should have the transaction.

        assertEquals(replicaDb, dbQueue.take());
        Transaction txn2 = txnQueue.take();

        assertNotEquals(txn1, txn2);
        assertEquals(txnId, txn2.id());

        fastAssertArrayEquals("message".getBytes(), msgQueue.take());

        assertEquals(prepareCommit, pcQueue.take());

        byte[] k1 = "k1".getBytes();
        byte[] v1 = "v1".getBytes();
        replicaIx.store(txn2, k1, v1);

        txn2.commit();

        fence(replicaDb, leaderDb);

        // Verify that the old leader observes the committed changes.
        fastAssertArrayEquals(v1, leaderIx.load(null, k1));
    }

    /**
     * Writes a message to the "control" index, and block the replica until it's received.
     */
    private void fence(Database leaderDb, Database replicaDb) throws Exception {
        fence(leaderDb, replicaDb, false);
    }

    private void fence(Database leaderDb, Database replicaDb, boolean failFast) throws Exception {
        Index leaderIx = leaderDb.openIndex("control");
        Index replicaIx = replicaDb.openIndex("control");

        byte[] key = (System.nanoTime() + ":" + ThreadLocalRandom.current().nextLong()).getBytes();

        leaderIx.store(null, key, key);

        int limit = failFast ? 100 : 10_000;

        for (int i=0; i<limit; i++) {
            byte[] value = replicaIx.load(null, key);
            if (value != null) {
                if (Arrays.equals(key, value)) {
                    return;
                }
                fail("Mismatched fence");
            }
            TestUtils.sleep(10);
        }

        fail("No fence received");
    }

    private Database closeAndReopen(int member) throws Exception {
        mDatabases[member].close();

        mReplicators[member] = StreamReplicator.open(mReplConfigs[member]);
        mDbConfigs[member].replicate(mReplicators[member]);

        return Database.open(mDbConfigs[member]);
    }
}
