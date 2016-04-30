/*
 *  Copyright 2016 Cojen.org
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
        mDb = newTempDatabase(mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
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
}
