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

import java.io.*;
import java.util.*;

import java.util.concurrent.locks.Lock;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.ext.TransactionHandler;

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
    }

    protected Database newTempDatabase() throws Exception {
        mConfig = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .customTransactionHandler(mHandler = new Handler());
        return TestUtils.newTempDatabase(getClass(), mConfig);
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;
    protected Handler mHandler;

    @Test
    public void exceptions() throws Exception {
        Transaction txn = mDb.newTransaction();

        try {
            txn.customRedo(null, 0, null);
            fail();
        } catch (NullPointerException e) {
            // Expected when message is null.
        }

        try {
            txn.customRedo(null, 123, null);
            fail();
        } catch (NullPointerException e) {
            // Expected when key is null.
        }
 
        byte[] key = new byte[1];
        txn.lockExclusive(123, key);

        try {
            txn.customRedo(null, 123, key);
            fail();
        } catch (NullPointerException e) {
            // Expected when message is null.
        }

        try {
            txn.customRedo(null, 0, key);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected when index is zero and key isn't null.
        }

        txn.reset();

        Database db = Database.open(new DatabaseConfig());
        txn = db.newTransaction();

        try {
            txn.customRedo(null, 0, null);
            fail();
        } catch (IllegalStateException e) {
            // Expected when no handler is installed.
        }

        try {
            txn.customUndo(null);
            fail();
        } catch (IllegalStateException e) {
            // Expected when no handler is installed.
        }
    }

    @Test
    public void rollback() throws Exception {
        rollback(mDb);
    }

    @Test
    public void rollbackNoRedo() throws Exception {
        teardown();
        mConfig.baseFile(null);
        rollback(Database.open(mConfig));
    }

    private void rollback(Database db) throws IOException {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();

        Transaction txn = db.newTransaction();
        txn.customUndo(message1);
        txn.customRedo(message1, 0, null);
        txn.customUndo(message2);
        txn.customRedo(message2, 0, null);

        assertTrue(mHandler.mRedoMessages.isEmpty());
        assertTrue(mHandler.mUndoMessages.isEmpty());

        txn.exit();

        assertTrue(mHandler.mRedoMessages.isEmpty());
        assertEquals(2, mHandler.mUndoMessages.size());

        assertArrayEquals(message2, mHandler.mUndoMessages.get(0));
        assertArrayEquals(message1, mHandler.mUndoMessages.get(1));
    }

    @Test
    public void commit() throws Exception {
        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();
        txn.customUndo(message);
        txn.customRedo(message, 0, null);

        assertTrue(mHandler.mRedoMessages.isEmpty());
        assertTrue(mHandler.mUndoMessages.isEmpty());

        txn.commit();

        assertTrue(mHandler.mRedoMessages.isEmpty());
        assertTrue(mHandler.mUndoMessages.isEmpty());
    }

    @Test
    public void recover() throws Exception {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();
        byte[] key = "key".getBytes();

        Transaction txn = mDb.newTransaction();
        txn.customUndo(message1);
        txn.customRedo(message1, 0, null);
        txn.customUndo(message2);
        txn.lockExclusive(1234, key);
        txn.customRedo(message2, 1234, key);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertEquals(2, mHandler.mRedoMessages.size());
        assertTrue(mHandler.mUndoMessages.isEmpty());

        assertArrayEquals(message1, mHandler.mRedoMessages.get(0));
        assertArrayEquals(message2, mHandler.mRedoMessages.get(1));

        assertEquals(1234, mHandler.mRedoIndexId);
        assertArrayEquals(key, mHandler.mRedoKey);
    }

    @Test
    public void recover2() throws Exception {
        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();

        Index ix = mDb.openIndex("test");
        ix.store(txn, "key".getBytes(), "value".getBytes());

        txn.customUndo(message);
        txn.customRedo(message, 0, null);
        txn.commit();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertEquals(1, mHandler.mRedoMessages.size());

        assertArrayEquals(message, mHandler.mRedoMessages.get(0));

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

        txn.customUndo(message);
        txn.customRedo(message, 0, null);

        mDb.checkpoint();
        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertEquals(0, mHandler.mRedoMessages.size());

        ix = mDb.openIndex("test");
        assertNull(ix.load(null, "key".getBytes()));
    }

    @Test
    public void scopeRedo() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.enter();
        byte[] message = "hello".getBytes();
        txn.customRedo(message, 0, null);
        txn.commitAll();

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);
        assertEquals(1, mHandler.mRedoMessages.size());
        assertArrayEquals(message, mHandler.mRedoMessages.get(0));
    }

    class Handler implements TransactionHandler {
        List<byte[]> mRedoMessages = new ArrayList<>();
        long mRedoIndexId;
        byte[] mRedoKey;

        List<byte[]> mUndoMessages = new ArrayList<>();

        @Override
        public void init(Database db) {
        }

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
        public void undo(byte[] message) throws IOException {
            mUndoMessages.add(message);
        }
    }
}
