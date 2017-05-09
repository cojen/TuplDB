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

import org.junit.*;
import static org.junit.Assert.*;

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
        mReplManager = new NonReplicationManager();
        mConfig = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.SYNC) // need to wait for commit confirmation
            .replicate(mReplManager);
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected NonReplicationManager mReplManager;
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

        mReplManager.asLeader();
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

        ix.store(null, "hello".getBytes(), "world".getBytes());

        mReplManager.asReplica();

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

        mReplManager.asLeader();
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

        ix.store(null, "key1".getBytes(), "value1".getBytes());

        mReplManager.asReplica();

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
}
