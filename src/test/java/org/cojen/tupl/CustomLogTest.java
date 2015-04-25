/*
 *  Copyright 2015 Brian S O'Neill
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

import java.io.*;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.ext.*;

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
        deleteTempDatabases();
        mDb = null;
    }

    protected Database newTempDatabase() throws Exception {
        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null)
            .customRedoHandler(mRedo = new Redo())
            .customUndoHandler(mUndo = new Undo());
        return TestUtils.newTempDatabase(mConfig);
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;
    protected Redo mRedo;
    protected Undo mUndo;

    @Test
    public void rollback() throws Exception {
        byte[] message1 = "hello".getBytes();
        byte[] message2 = "world".getBytes();

        Transaction txn = mDb.newTransaction();
        txn.customUndo(message1);
        txn.customRedo(message1, 0, null);
        txn.customUndo(message2);
        txn.customRedo(message2, 0, null);

        assertTrue(mRedo.mMessages.isEmpty());
        assertTrue(mUndo.mMessages.isEmpty());

        txn.exit();

        assertTrue(mRedo.mMessages.isEmpty());
        assertEquals(2, mUndo.mMessages.size());

        assertArrayEquals(message2, mUndo.mMessages.get(0));
        assertArrayEquals(message1, mUndo.mMessages.get(1));
    }

    @Test
    public void commit() throws Exception {
        byte[] message = "hello".getBytes();

        Transaction txn = mDb.newTransaction();
        txn.customUndo(message);
        txn.customRedo(message, 0, null);

        assertTrue(mRedo.mMessages.isEmpty());
        assertTrue(mUndo.mMessages.isEmpty());

        txn.commit();

        assertTrue(mRedo.mMessages.isEmpty());
        assertTrue(mUndo.mMessages.isEmpty());
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
        txn.customRedo(message2, 1234, key);
        txn.commit();

        mDb = reopenTempDatabase(mDb, mConfig);

        assertEquals(2, mRedo.mMessages.size());
        assertTrue(mUndo.mMessages.isEmpty());

        assertArrayEquals(message1, mRedo.mMessages.get(0));
        assertArrayEquals(message2, mRedo.mMessages.get(1));

        assertEquals(1234, mRedo.mIndexId);
        assertArrayEquals(key, mRedo.mKey);
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

        mDb = reopenTempDatabase(mDb, mConfig);

        assertEquals(1, mRedo.mMessages.size());

        assertArrayEquals(message, mRedo.mMessages.get(0));

        ix = mDb.openIndex("test");
        assertArrayEquals("value".getBytes(), ix.load(null, "key".getBytes()));
    }

    class Redo implements RedoHandler {
        List<byte[]> mMessages = new ArrayList<>();
        long mIndexId;
        byte[] mKey;

        @Override
        public void redo(Database db, Transaction txn, byte[] message) throws IOException {
            mMessages.add(message);
        }

        @Override
        public void redo(Database db, Transaction txn, byte[] message, long indexId, byte[] key)
            throws IOException
        {
            mMessages.add(message);
            mIndexId = indexId;
            mKey = key;
        }
    }

    class Undo implements UndoHandler {
        List<byte[]> mMessages = new ArrayList<>();

        @Override
        public void undo(Database db, byte[] message) throws IOException {
            mMessages.add(message);
        }
    }
}
