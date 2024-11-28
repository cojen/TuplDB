/*
 *  Copyright (C) 2019 Cojen.org
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

import java.util.Random;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CheckpointFailureTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CheckpointFailureTest.class.getName());
    }

    private Database mDb;

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
        }
        deleteTempDatabases(getClass());
    }

    @Test
    public void checkpointResume() throws Exception {
        var config0 = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .pageSize(4096);

        Database db0 = newTempDatabase(getClass(), config0);

        var pa = new DatabasePageArray(4096, db0);
        DatabaseConfig config = config0.clone();
        config.dataPageArray(pa).baseFile(newTempBaseFile(getClass()));
        mDb = Database.open(config);

        final int count = 100_000;

        // Fill up with stuff.
        final long seed = 29083745;
        {
            Index ix = mDb.openIndex("test");

            Transaction txn = mDb.newTransaction();
            ix.store(txn, "hello".getBytes(), "world".getBytes());

            var rnd = new Random(seed);
            for (int i=0; i<count; i++) {
                byte[] key = randomStr(rnd, 10, 20);
                byte[] value = randomStr(rnd, 10, 100);
                ix.store(Transaction.BOGUS, key, value);
            }

            // Checkpoint and fail.
            pa.enableWriteFailures(() -> rnd.nextDouble() < 0.1);
            try {
                mDb.checkpoint();
                fail();
            } catch (WriteFailureException e) {
            }
        }

        // Resume checkpoint without failures.
        pa.enableWriteFailures(null);
        mDb.checkpoint();

        // Reopen and verify.
        mDb.close();
        db0 = reopenTempDatabase(getClass(), db0, config0);
        pa = new DatabasePageArray(4096, db0);
        config.dataPageArray(pa);
        mDb = Database.open(config);

        assertTrue(mDb.verify(null, 1));

        Index ix = mDb.openIndex("test");

        var rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 20);
            byte[] value = randomStr(rnd, 10, 100);
            fastAssertArrayEquals(value, ix.load(null, key));
        }

        assertEquals(count, ix.count(null, null));

        // Rolled back.
        assertNull(ix.load(null, "hello".getBytes()));
    }

    @Test
    public void undoCommitRollback() throws Exception {
        // Test that a transaction which optimistically committed its undo log rolls back when
        // the checkpoint fails.

        var config = new DatabaseConfig()
            .checkpointRate(-1, null);

        var repl = new NonReplicator();
        config.replicate(repl);

        mDb = newTempDatabase(getClass(), config);

        // open index
        repl.asLeader();
        Thread.yield();
        Index ix = null;
        for (int i=0; i<100; i++) {
            try {
                ix = mDb.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                sleep(100);
            }
        }
        assertNotNull(ix);

        mDb.checkpoint();

        var keys = new byte[4][];
        for (int i=0; i<keys.length; i++) {
            keys[i] = ("key" + i).getBytes();
        }

        Transaction txn1 = mDb.newTransaction();
        ix.store(txn1, keys[0], keys[0]);
        txn1.commit();

        Transaction txn2 = mDb.newTransaction();
        ix.store(txn2, keys[1], keys[1]);
        txn2.flush();

        // This one isn't explicitly committed or rolled back.
        Transaction txn3 = mDb.newTransaction();
        ix.store(txn3, keys[3], keys[3]);
        txn3.flush();

        final long pos = repl.position();

        ix.store(txn2, keys[2], keys[2]);

        var exRef = new AtomicReference<Throwable>();

        var committer = new Thread(() -> {
            // Commit replication at a lower position.
            repl.suspendCommit(Thread.currentThread(), pos);

            try {
                txn2.commit();
            } catch (Throwable e) {
                exRef.set(e);
            }
        });

        repl.startAndWaitUntilSuspended(committer);

        var exRef2 = new AtomicReference<Throwable>();

        var checkpointer = new Thread(() -> {
            try {
                mDb.checkpoint();
            } catch (Throwable e) {
                exRef2.set(e);
            }
        });

        startAndWaitUntilBlocked(checkpointer);

        repl.asReplica();
        repl.setPosition(pos);

        committer.join();
        checkpointer.join();

        Throwable ex = exRef.get();
        assertTrue("" + ex, ex instanceof UnmodifiableReplicaException);

        ex = exRef2.get();
        assertNull(ex);

        // Transaction 1 committed and txn 2 rolled back.
        fastAssertArrayEquals(keys[0], ix.load(null, keys[0]));
        assertNull(ix.load(null, keys[1]));
        assertNull(ix.load(null, keys[2]));

        // Close and re-open to verify that txn 1 committed and txn 2 rolled back.

        var repl2 = new NonReplicator();
        config.replicate(repl2);
        mDb = reopenTempDatabase(getClass(), mDb, config);

        ix = mDb.openIndex("test");

        fastAssertArrayEquals(keys[0], ix.load(null, keys[0]));

        for (int i=1; i<keys.length; i++) {
            try {
                ix.load(null, keys[i]);
                fail();
            } catch (LockTimeoutException e) {
                // Only the new leader can roll it back.
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void undoCommitRollback2() throws Exception {
        // Same as undoCommitRollback, but with more transactions.

        var config = new DatabaseConfig()
            .checkpointRate(-1, null);

        var repl = new NonReplicator();
        config.replicate(repl);

        mDb = newTempDatabase(getClass(), config);

        // open index
        repl.asLeader();
        Thread.yield();
        Index ix = null;
        for (int i=0; i<1000; i++) {
            try {
                ix = mDb.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                sleep(100);
            }
        }
        assertNotNull(ix);

        mDb.checkpoint();

        final long seed = 29083745;
        var rnd = new Random(seed);

        var txns = new Transaction[100];
        int mid = txns.length / 2;

        long pos = Long.MIN_VALUE;

        for (int i=0; i<txns.length; i++) {
            Transaction txn = mDb.newTransaction();
            txns[i] = txn;

            int amt = rnd.nextInt(203) + 1;
            for (int j=0; j<amt; j++) {
                byte[] key = ("txn-" + i + "-key-" + j).getBytes();
                ix.store(txn, key, key);
            }

            if (i < mid) {
                txn.commit();
                txn.flush();
            } else if (i == mid) {
                txn.flush();
                pos = repl.position();
            }
        }

        final long fpos = pos;

        class Committer extends Thread {
            final Transaction txn;
            volatile Throwable ex;

            Committer(Transaction txn) {
                this.txn = txn;
            }

            @Override
            public void run() {
                // Commit replication at a lower position.
                repl.suspendCommit(this, fpos);

                try {
                    txn.commit();
                } catch (Throwable e) {
                    ex = e;
                }
            }
        }

        var committers = new Committer[txns.length - mid];
        for (int i=0; i<committers.length; i++) {
            committers[i] = new Committer(txns[mid + i]);
        }

        for (Thread c : committers) {
            repl.startAndWaitUntilSuspended(c);
        }

        var exRef = new AtomicReference<Throwable>();

        var checkpointer = new Thread(() -> {
            try {
                mDb.checkpoint();
            } catch (Throwable e) {
                exRef.set(e);
            }
        });

        startAndWaitUntilBlocked(checkpointer);

        repl.asReplica();
        repl.setPosition(pos);

        for (Thread c : committers) {
            c.join();
        }

        checkpointer.join();

        for (Committer c : committers) {
            Throwable ex = c.ex;
            assertTrue("" + ex, ex instanceof UnmodifiableReplicaException);
        }

        assertNull(exRef.get());

        // Verify that transactions lower than mid committed and those higher rolled back.

        rnd = new Random(seed);

        for (int i=0; i<txns.length; i++) {
            int amt = rnd.nextInt(203) + 1;
            for (int j=0; j<amt; j++) {
                byte[] key = ("txn-" + i + "-key-" + j).getBytes();
                byte[] value = ix.load(null, key);
                if (i < mid) {
                    fastAssertArrayEquals(key, value);
                } else {
                    assertNull(value);
                }
            }
        }

        // Close and re-open to verify the same commits and rollbacks.

        var repl2 = new NonReplicator();
        config.replicate(repl2);
        config.lockTimeout(1, TimeUnit.MILLISECONDS);
        mDb = reopenTempDatabase(getClass(), mDb, config);

        ix = mDb.openIndex("test");

        rnd = new Random(seed);

        for (int i=0; i<txns.length; i++) {
            int amt = rnd.nextInt(203) + 1;
            for (int j=0; j<amt; j++) {
                byte[] key = ("txn-" + i + "-key-" + j).getBytes();
                if (i < mid) {
                    byte[] value = ix.load(null, key);
                    fastAssertArrayEquals(key, value);
                } else {
                    try {
                        ix.load(null, key);
                        fail();
                    } catch (LockTimeoutException e) {
                        // Only the new leader can roll it back.
                    }
                }
            }
        }
    }
}
