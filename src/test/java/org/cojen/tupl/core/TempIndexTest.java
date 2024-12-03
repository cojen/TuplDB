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
public class TempIndexTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TempIndexTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null);
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void openClose() throws Exception {
        Index temp = mDb.newTemporaryIndex();
        assertNull(temp.name());
        long id = temp.id();
        assertSame(temp, mDb.indexById(id));
        temp.close();
        assertNull(mDb.indexById(id));
        temp = mDb.newTemporaryIndex();
        assertNotEquals(id, temp.id());
    }

    @Test
    public void noRedo() throws Exception {
        noRedo(false);
    }

    @Test
    public void noRedoCheckpoint() throws Exception {
        noRedo(true);
    }

    private void noRedo(boolean checkpoint) throws Exception {
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

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertNull(mDb.indexById(temp.id()));

        Index temp2 = mDb.newTemporaryIndex();

        if (checkpoint) {
            assertNotEquals(temp.id(), temp2.id());
        } else {
            assertEquals(temp.id(), temp2.id());
        }

        try {
            assertEquals(0, temp.count(null, null));
            fail();
        } catch (ClosedIndexException e) {
        }

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

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        Index temp2 = mDb.newTemporaryIndex();

        if (checkpoint) {
            assertNotEquals(temp.id(), temp2.id());
        } else {
            assertEquals(temp.id(), temp2.id());
        }

        try {
            assertEquals(0, temp.count(null, null));
            fail();
        } catch (DeletedIndexException e) {
        }

        assertEquals(0, temp2.count(null, null));
    }

    @Test
    public void forReplica() throws Exception {
        mDb.shutdown();
        mConfig.replicate(new NonReplicator());
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        try {
            mDb.openIndex("test");
            fail();
        } catch (UnmodifiableReplicaException e) {
            // Expected.
        }

        Index temp = mDb.newTemporaryIndex();

        for (int i=0; i<10000; i++) {
            Transaction txn = switch (i % 3) {
                default -> null;
                case 1 -> Transaction.BOGUS;
                case 2 -> mDb.newTransaction(DurabilityMode.SYNC);
            };

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

        mConfig.replicate(new NonReplicator());
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
    }

    @Test
    public void txnLock() throws Exception {
        Index temp = mDb.newTemporaryIndex();

        Transaction txn = mDb.newTransaction();
        final byte[] key = "k1".getBytes();
        final byte[] v1 = "v1".getBytes();
        final byte[] v2 = "v2".getBytes();

        Cursor c = temp.newCursor(txn);
        c.find(key);
        c.store(v1);

        Cursor c2 = temp.newCursor(null);
        try {
            c2.find(key);
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        try {
            temp.store(null, key, v2);
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals(v1, temp.load(Transaction.BOGUS, key));
        txn.exit();
        assertNull(temp.load(Transaction.BOGUS, key));

        temp.store(txn, key, v1);

        try {
            temp.store(null, key, v2);
            fail();
        } catch (LockTimeoutException e) {
            // Expected.
        }

        fastAssertArrayEquals(v1, temp.load(Transaction.BOGUS, key));
        txn.exit();
        assertNull(temp.load(Transaction.BOGUS, key));
    }

    @Test
    public void cursorValue() throws Exception {
        // Verifies that cursor value is updated after a successful store.

        Index temp = mDb.newTemporaryIndex();

        final byte[] key = "k1".getBytes();
        final byte[] v1 = "v1".getBytes();

        {
            Cursor c = temp.newCursor(null);
            c.find(key);
            c.store(v1);
            assertEquals(v1, c.value());
            c.store(null);
            assertNull(c.value());
            c.reset();
        }

        {
            Transaction txn = mDb.newTransaction();
            Cursor c = temp.newCursor(txn);
            c.find(key);
            c.store(v1);
            assertEquals(v1, c.value());
            c.store(null);
            assertNull(c.value());
            c.reset();
            txn.reset();
        }
    }

    @Test
    public void cursorCommitBogusTxn() throws Exception {
        Index ix = mDb.newTemporaryIndex();
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.find("hello".getBytes());
        c.commit("world".getBytes());
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
    }

    @Test
    public void valueWrite() throws Exception {
        Index ix = mDb.newTemporaryIndex();
        ValueAccessor accessor = ix.newAccessor(null, "hello".getBytes());
        byte[] buf = "world".getBytes();
        accessor.valueWrite(0, buf, 0, buf.length);
        accessor.close();
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
    }
}
