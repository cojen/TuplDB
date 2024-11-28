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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class UnreplicatedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(UnreplicatedTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mRepl = new NonReplicator();
        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.SYNC) // need to wait for commit confirmation
            .replicate(mRepl);
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected NonReplicator mRepl;
    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void basicFailover() throws Exception {
        doBasicFailover();
    }

    /**
     * @return test index, as a replica
     */
    private Index doBasicFailover() throws Exception {
        try {
            mDb.openIndex("test");
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        mRepl.asLeader();
        Thread.yield();

        Index ix = null;
        for (int i=0; i<100; i++) {
            try {
                ix = mDb.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                Thread.sleep(100);
            }
        }

        assertNotNull(ix);

        ix.store(null, "hello".getBytes(), "world".getBytes());

        mRepl.asReplica();

        try {
            ix.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        return ix;
    }

    @Test
    public void durabilityDowngrade() throws Exception {
        // This will prepare the index as a side effect.
        Index ix = doBasicFailover();

        Transaction txn = mDb.newTransaction();
        try {
            Cursor c = ix.newCursor(txn);
            try {
                c.find("key".getBytes());
                assertNull(c.value());

                try {
                    c.commit("value".getBytes());
                    fail();
                } catch (UnmodifiableReplicaException e) {
                    // Expected.
                }

                // Try again without replication, but it should still fail.
                txn.durabilityMode(DurabilityMode.NO_REDO);

                try {
                    c.commit("value".getBytes());
                    fail();
                } catch (InvalidTransactionException e) {
                    // Caused by earlier commit failure.
                }
            } finally {
                c.reset();
            }
        } finally {
            txn.reset();
        }

        // Verify the value doesn't exist.
        byte[] value = ix.load(null, "key".getBytes());
        assertNull(value);
    }

    @Test
    public void redoSideEffects() throws Exception {
        // When a write fails do to redo failure, the local transaction still observes the
        // change until the transaction rolls back.

        mRepl.asLeader();
        Thread.yield();

        Index ix = null;
        for (int i=0; i<100; i++) {
            try {
                ix = mDb.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                // Wait for replication thread to finish the switch.
                Thread.sleep(100);
            }
        }

        assertNotNull(ix);

        ix.store(null, "key1".getBytes(), "value1".getBytes());

        mRepl.asReplica();

        Transaction txn = mDb.newTransaction();

        try {
            // Large value to force a flush.
            ix.store(txn, "key1".getBytes(), new byte[100000]);
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        fastAssertArrayEquals(new byte[100000], ix.load(txn, "key1".getBytes()));

        try {
            txn.commit();
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        // Transaction should rollback when commit fails.
        fastAssertArrayEquals("value1".getBytes(), ix.load(txn, "key1".getBytes()));
    }

    @Test
    public void noLockReplicate() throws Exception {
        Transaction txn = mDb.newTransaction();

        byte[] key1 = "key-1".getBytes();
        assertEquals(LockResult.ACQUIRED, txn.lockShared(123, key1));
        assertEquals(LockResult.OWNED_SHARED, txn.lockCheck(123, key1));

        byte[] key2 = "key-2".getBytes();
        assertEquals(LockResult.ACQUIRED, txn.lockUpgradable(123, key2));
        assertEquals(LockResult.OWNED_UPGRADABLE, txn.lockCheck(123, key2));

        byte[] key3 = "key-3".getBytes();
        assertEquals(LockResult.ACQUIRED, txn.lockExclusive(123, key3));
        assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(123, key3));

        assertEquals(LockResult.UPGRADED, txn.lockExclusive(123, key2));
        assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(123, key2));
    }
}
