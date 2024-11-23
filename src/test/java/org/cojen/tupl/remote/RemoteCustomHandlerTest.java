/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import java.util.Map;

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
public class RemoteCustomHandlerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteCustomHandlerTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerConfig = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .checkpointRate(-1, null);

        mServerHandler = new Handler();

        mServerConfig.customHandlers(Map.of("test", mServerHandler));

        mServerDb = newTempDatabase(getClass(), mServerConfig);

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mClientDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            mClientDb.close();
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());
    }

    private DatabaseConfig mServerConfig;
    private Handler mServerHandler;
    private Database mServerDb;
    private Database mClientDb;

    private static class Handler implements CustomHandler {
        private String mRedoMessage1;
        private String mRedoMessage2;
        private String mUndoMessage;

        @Override
        public synchronized void redo(Transaction txn, byte[] message) {
            mRedoMessage1 = new String(message);
            notify();
        }

        @Override
        public synchronized void redo(Transaction txn, byte[] message, long indexId, byte[] key) {
            mRedoMessage2 = new String(message) + "," + indexId + "," + new String(key);
            notify();
        }

        @Override
        public synchronized void undo(Transaction txn, byte[] message) {
            mUndoMessage = new String(message);
            notify();
        }

        synchronized String waitForRedo1() throws InterruptedException {
            while (mRedoMessage1 == null) {
                wait();
            }
            return mRedoMessage1;
        }

        synchronized String waitForRedo2() throws InterruptedException {
            while (mRedoMessage2 == null) {
                wait();
            }
            return mRedoMessage2;
        }

        synchronized String waitForUndo() throws InterruptedException {
            while (mUndoMessage == null) {
                wait();
            }
            return mUndoMessage;
        }
    }

    @Test
    public void basic() throws Exception {
        CustomHandler handler = mClientDb.customWriter("test");
        Index ix = mClientDb.openIndex("test");
        long ixId = ix.id();

        Transaction txn = mClientDb.newTransaction();

        handler.undo(txn, "message-3".getBytes());
        txn.exit();

        assertEquals("message-3", mServerHandler.waitForUndo());

        handler.redo(txn, "message-1".getBytes());

        try {
            handler.redo(txn, "message-2".getBytes(), ixId, "key-2".getBytes());
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't owned"));
        }

        assertEquals(LockResult.ACQUIRED, txn.lockExclusive(ixId, "key-2".getBytes()));

        handler.redo(txn, "message-2".getBytes(), ixId, "key-2".getBytes());

        txn.commit();

        mServerDb = reopenTempDatabase(getClass(), mServerDb, mServerConfig);

        assertEquals("message-1", mServerHandler.waitForRedo1());
        assertEquals("message-2," + ixId + ",key-2", mServerHandler.waitForRedo2());
    }
}
