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
public class TempIndexTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TempIndexTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null);
        mDb = newTempDatabase(mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void openClose() throws Exception {
        Index temp = mDb.newTemporaryIndex();
        assertNull(temp.getName());
        long id = temp.getId();
        assertTrue(temp == mDb.indexById(id));
        temp.close();
        assertNull(mDb.indexById(id));
        temp = mDb.newTemporaryIndex();
        assertNotEquals(id, temp.getId());
    }

    @Test
    public void noRedo() throws Exception {
        noRedo(false, false);
    }

    @Test
    public void noRedoStableId() throws Exception {
        noRedo(true, false);
    }

    @Test
    public void noRedoCheckpoint() throws Exception {
        noRedo(false, true);
    }

    private void noRedo(boolean stableId, boolean checkpoint) throws Exception {
        if (stableId) {
            // Open a regular index to force the random identifier mask to be persisted.
            mDb.openIndex("test");
        }

        Index temp = mDb.newTemporaryIndex();

        temp.store(null, "k1".getBytes(), "v1".getBytes());
        temp.store(Transaction.BOGUS, "k2".getBytes(), "v2".getBytes());
        Transaction txn = mDb.newTransaction(DurabilityMode.SYNC);
        temp.store(txn, "k3".getBytes(), "v3".getBytes());
        txn.commit();
        txn = mDb.newTransaction(DurabilityMode.NO_REDO);
        temp.store(txn, "k4".getBytes(), "v4".getBytes());
        txn.commit();
        txn = mDb.newTransaction(DurabilityMode.NO_FLUSH);
        temp.store(txn, "k5".getBytes(), "v5".getBytes());
        txn.reset();

        for (int i=1; i<=5; i++) {
            byte[] value = ("v" + i).getBytes();
            byte[] found = temp.load(null, ("k" + i).getBytes());
            if (i == 5) {
                assertNull(found);
            } else {
                fastAssertArrayEquals(value, found);
            }
        }

        if (checkpoint) {
            mDb.checkpoint();
        }

        mDb = reopenTempDatabase(mDb, mConfig);

        assertNull(mDb.indexById(temp.getId()));

        Index temp2 = mDb.newTemporaryIndex();

        if (stableId) {
            assertEquals(temp.getId(), temp2.getId());
        } else {
            assertNotEquals(temp.getId(), temp2.getId());
        }

        assertEquals(0, temp.count(null, null));
        assertEquals(0, temp2.count(null, null));
    }

    @Test
    public void explicitDelete() throws Exception {
        explicitDelete(false);
    }

    @Test
    public void explicitDeleteCheckpoint() throws Exception {
        explicitDelete(true);
    }

    public void explicitDelete(boolean checkpoint) throws Exception {
        // Open a regular index to force the random identifier mask to be persisted.
        mDb.openIndex("test");

        Index temp = mDb.newTemporaryIndex();
        for (int i=0; i<10000; i++) {
            temp.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        if (checkpoint) {
            mDb.checkpoint();
        }

        mDb.deleteIndex(temp).run();

        mDb = reopenTempDatabase(mDb, mConfig);

        Index temp2 = mDb.newTemporaryIndex();

        if (checkpoint) {
            assertNotEquals(temp.getId(), temp2.getId());
        } else {
            assertEquals(temp.getId(), temp2.getId());
        }

        assertEquals(0, temp.count(null, null));
        assertEquals(0, temp2.count(null, null));
    }

    @Test
    public void forReplica() throws Exception {
        mConfig.replicate(new NonReplicationManager());
        mDb = reopenTempDatabase(mDb, mConfig);

        try {
            mDb.openIndex("test");
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        Index temp = mDb.newTemporaryIndex();

        for (int i=0; i<10000; i++) {
            Transaction txn;
            switch (i % 3) {
            default:
                txn = null;
                break;
            case 1:
                txn = Transaction.BOGUS;
                break;
            case 2:
                txn = mDb.newTransaction(DurabilityMode.SYNC);
                break;
            }

            temp.insert(txn, ("key-" + i).getBytes(), ("value-" + i).getBytes());

            if (txn != null) {
                txn.commit();
            }
        }

        for (int i=0; i<10000; i++) {
            byte[] value = temp.load(null, ("key-" + i).getBytes());
            fastAssertArrayEquals(("value-" + i).getBytes(), value);
        }

        mDb.checkpoint();

        mConfig.replicate(new NonReplicationManager());
        mDb = reopenTempDatabase(mDb, mConfig);
    }
}
