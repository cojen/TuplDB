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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.core.TestUtils.*;

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
        DatabaseConfig config0 = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .pageSize(4096);

        Database db0 = newTempDatabase(getClass(), config0);

        DatabasePageArray pa = new DatabasePageArray(4096, db0);
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

            Random rnd = new Random(seed);
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

        assertTrue(mDb.verify(null));

        Index ix = mDb.openIndex("test");

        Random rnd = new Random(seed);
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
        // Test that a transaction which optimisically committed its undo log rolls back when
        // the checkpoint fails.

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null);

        NonReplicationManager replMan = new NonReplicationManager();
        config.replicate(replMan);

        mDb = newTempDatabase(getClass(), config);

        // open index
        replMan.asLeader();
        Thread.yield();
        Index ix = null;
        for (int i=0; i<10; i++) {
            try {
                ix = mDb.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                Thread.sleep(100);
            }
        }
        assertTrue(ix != null);

        mDb.checkpoint();

        byte[][] keys = new byte[4][];
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

        final long pos = replMan.writer().position();

        ix.store(txn2, keys[2], keys[2]);

        var exRef = new AtomicReference<Throwable>();

        Thread committer = new Thread(() -> {
            // Confirm at a lower position.
            replMan.suspendConfirmation(Thread.currentThread(), pos);

            try {
                txn2.commit();
            } catch (Throwable e) {
                exRef.set(e);
            }
        });

        startAndWaitUntilBlocked(committer);

        var exRef2 = new AtomicReference<Throwable>();

        Thread checkpointer = new Thread(() -> {
            try {
                mDb.checkpoint();
            } catch (Throwable e) {
                exRef2.set(e);
            }
        });

        startAndWaitUntilBlocked(checkpointer);

        replMan.toReplica(pos);

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

        NonReplicationManager replMan2 = new NonReplicationManager();
        config.replicate(replMan2);
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
}