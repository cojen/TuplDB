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
    public void rollback() throws Exception {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();

        Transaction txn = mDb.newTransaction();
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

    class Handler implements TransactionHandler {
        List<byte[]> mRedoMessages = new ArrayList<>();
        long mRedoIndexId;
        byte[] mRedoKey;

        List<byte[]> mUndoMessages = new ArrayList<>();

        @Override
        public void redo(Database db, Transaction txn, byte[] message) throws IOException {
            mRedoMessages.add(message);
        }

        @Override
        public void redo(Database db, Transaction txn, byte[] message, long indexId, byte[] key)
            throws IOException
        {
            mRedoMessages.add(message);
            mRedoIndexId = indexId;
            mRedoKey = key;
        }

        @Override
        public void undo(Database db, byte[] message) throws IOException {
            mUndoMessages.add(message);
        }

        @Override
        public void setCheckpointLock(Database db, Lock lock) {
        }

        @Override
        public Object checkpointStart(Database db) throws IOException {
            return null;
        }

        @Override
        public void checkpointFinish(Database db, Object obj) throws IOException {
        }
    }
}
