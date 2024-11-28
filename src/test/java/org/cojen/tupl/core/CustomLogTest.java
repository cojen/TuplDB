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

import java.io.*;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.ext.CustomHandler;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CustomLogTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CustomLogTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mWriteHandler = null;
        mWriteHandler2 = null;
    }

    protected Database newTempDatabase() throws Exception {
        mRecoveryHandler = new Handler();
        mRecoveryHandler2 = new Handler();

        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null)
            .customHandlers(Map.of("TestHandler", mRecoveryHandler,
                                   "TestHandler2", mRecoveryHandler2));

        return open();
    }

    protected Database open() throws Exception {
        mDb = TestUtils.newTempDatabase(getClass(), mConfig);
        mWriteHandler = mDb.customWriter("TestHandler");
        mWriteHandler2 = mDb.customWriter("TestHandler2");
        return mDb;
    }

    protected Database reopen() throws Exception {
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        mWriteHandler = mDb.customWriter("TestHandler");
        mWriteHandler2 = mDb.customWriter("TestHandler2");
        return mDb;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;
    protected Handler mRecoveryHandler, mRecoveryHandler2;
    protected CustomHandler mWriteHandler, mWriteHandler2;

    @Test
    public void exceptions() throws Exception {
        Transaction txn = mDb.newTransaction();

        try {
            mWriteHandler.redo(txn, null, 0, null);
            fail();
        } catch (NullPointerException e) {
            // Expected when message is null.
        }

        try {
            mWriteHandler.redo(txn, null, 123, null);
            fail();
        } catch (NullPointerException e) {
            // Expected when key is null.
        }
 
        var key = new byte[1];
        txn.lockExclusive(123, key);

        try {
            mWriteHandler.redo(txn, null, 123, key);
            fail();
        } catch (NullPointerException e) {
            // Expected when message is null.
        }

        try {
            mWriteHandler.redo(txn, null, 0, key);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected when index is zero and key isn't null.
        }

        try {
            mWriteHandler.redo(null, key);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            mWriteHandler.redo(Transaction.BOGUS, key);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            mWriteHandler.undo(null, key);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            mWriteHandler.undo(Transaction.BOGUS, key);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        txn.reset();

        Database db = Database.open(new DatabaseConfig());
        txn = db.newTransaction();

        try {
            mWriteHandler.redo(txn, null, 0, null);
            fail();
        } catch (IllegalStateException e) {
            // Wrong database instance.
        }

        try {
            mWriteHandler.undo(txn, null);
            fail();
        } catch (IllegalStateException e) {
            // Wrong database instance.
        }

        try {
            mDb.customWriter("foo");
            fail();
        } catch (IllegalStateException e) {
            // Not installed.
            assertTrue(e.getMessage().indexOf("foo") > 0);
        }

        db.close();
    }

    @Test
    public void rollback() throws Exception {
        rollback(mDb);
    }

    @Test
    public void rollbackNoRedo() throws Exception {
        teardown();
        mConfig.baseFile(null);
        rollback(open());
    }

    private void rollback(Database db) throws IOException {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();
        byte[] message3 = "hello!".getBytes();

        Transaction txn = db.newTransaction();
        mWriteHandler.undo(txn, message1);
        mWriteHandler.redo(txn, message1, 0, null);
        mWriteHandler.undo(txn, message2);
        mWriteHandler.redo(txn, message2, 0, null);
        mWriteHandler2.undo(txn, message3);

        assertTrue(mRecoveryHandler.mRedoMessages.isEmpty());
        assertTrue(mRecoveryHandler.mUndoMessages.isEmpty());
        assertTrue(mRecoveryHandler2.mRedoMessages.isEmpty());
        assertTrue(mRecoveryHandler2.mUndoMessages.isEmpty());

        txn.exit();

        assertTrue(mRecoveryHandler.mRedoMessages.isEmpty());
        assertEquals(2, mRecoveryHandler.mUndoMessages.size());
        assertTrue(mRecoveryHandler2.mRedoMessages.isEmpty());
        assertEquals(1, mRecoveryHandler2.mUndoMessages.size());

        assertArrayEquals(message2, mRecoveryHandler.mUndoMessages.get(0));
        assertArrayEquals(message1, mRecoveryHandler.mUndoMessages.get(1));
        assertArrayEquals(message3, mRecoveryHandler2.mUndoMessages.get(0));
    }

    @Test
    public void commit() throws Exception {
        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();
        mWriteHandler.undo(txn, message);
        mWriteHandler.redo(txn, message, 0, null);

        assertTrue(mRecoveryHandler.mRedoMessages.isEmpty());
        assertTrue(mRecoveryHandler.mUndoMessages.isEmpty());

        txn.commit();

        assertTrue(mRecoveryHandler.mRedoMessages.isEmpty());
        assertTrue(mRecoveryHandler.mUndoMessages.isEmpty());
    }

    @Test
    public void recover() throws Exception {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();
        byte[] message3 = "hello!".getBytes();
        byte[] key = "key".getBytes();

        Transaction txn = mDb.newTransaction();
        mWriteHandler.undo(txn, message1);
        mWriteHandler.redo(txn, message1, 0, null);
        mWriteHandler.undo(txn, message2);
        txn.lockExclusive(1234, key);
        mWriteHandler.redo(txn, message2, 1234, key);
        mWriteHandler2.redo(txn, message3);
        txn.commit();

        reopen();

        assertEquals(2, mRecoveryHandler.mRedoMessages.size());
        assertTrue(mRecoveryHandler.mUndoMessages.isEmpty());
        assertEquals(1, mRecoveryHandler2.mRedoMessages.size());
        assertTrue(mRecoveryHandler2.mUndoMessages.isEmpty());

        assertArrayEquals(message1, mRecoveryHandler.mRedoMessages.get(0));
        assertArrayEquals(message2, mRecoveryHandler.mRedoMessages.get(1));
        assertArrayEquals(message3, mRecoveryHandler2.mRedoMessages.get(0));

        assertEquals(1234, mRecoveryHandler.mRedoIndexId);
        assertArrayEquals(key, mRecoveryHandler.mRedoKey);
    }

    @Test
    public void recoverNoHandlers() throws Exception {
        Transaction txn = mDb.newTransaction();
        mWriteHandler.redo(txn, "hello".getBytes());
        txn.commit();

        mConfig.customHandlers(null);
        try {
            reopen();
        } catch (IllegalStateException e) {
            // Not installed.
            assertTrue(e.getMessage().indexOf("TestHandler") > 0);
        }
    }

    @Test
    public void recover2() throws Exception {
        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();

        Index ix = mDb.openIndex("test");
        ix.store(txn, "key".getBytes(), "value".getBytes());

        mWriteHandler.undo(txn, message);
        mWriteHandler.redo(txn, message, 0, null);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertEquals(1, mRecoveryHandler.mRedoMessages.size());

        assertArrayEquals(message, mRecoveryHandler.mRedoMessages.get(0));

        ix = mDb.openIndex("test");
        assertArrayEquals("value".getBytes(), ix.load(null, "key".getBytes()));
    }

    @Test
    public void recover3() throws Exception {
        // Like recover2, except the transaction rolls back.

        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();

        Index ix = mDb.openIndex("test");
        ix.store(txn, "key".getBytes(), "value".getBytes());

        mWriteHandler.undo(txn, message);
        mWriteHandler.redo(txn, message, 0, null);

        mDb.checkpoint();
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertEquals(0, mRecoveryHandler.mRedoMessages.size());

        ix = mDb.openIndex("test");
        assertNull(ix.load(null, "key".getBytes()));
    }

    @Test
    public void scopeRedo() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.enter();
        byte[] message = "hello".getBytes();
        mWriteHandler.redo(txn, message, 0, null);
        txn.commitAll();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertEquals(1, mRecoveryHandler.mRedoMessages.size());
        assertArrayEquals(message, mRecoveryHandler.mRedoMessages.get(0));
    }

    static class Handler implements CustomHandler {
        List<byte[]> mRedoMessages = new ArrayList<>();
        long mRedoIndexId;
        byte[] mRedoKey;

        List<byte[]> mUndoMessages = new ArrayList<>();

        @Override
        public void redo(Transaction txn, byte[] message) throws IOException {
            mRedoMessages.add(message);
        }

        @Override
        public void redo(Transaction txn, byte[] message, long indexId, byte[] key)
            throws IOException
        {
            mRedoMessages.add(message);
            mRedoIndexId = indexId;
            mRedoKey = key;
        }

        @Override
        public void undo(Transaction txn, byte[] message) throws IOException {
            mUndoMessages.add(message);
        }
    }
}
