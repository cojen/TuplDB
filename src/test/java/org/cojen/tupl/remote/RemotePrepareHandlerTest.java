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

import org.cojen.tupl.ext.PrepareHandler;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemotePrepareHandlerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemotePrepareHandlerTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerConfig = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_SYNC)
            .checkpointRate(-1, null);

        mServerHandler = new Handler();

        mServerConfig.prepareHandlers(Map.of("test", mServerHandler));

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

    private static class Handler implements PrepareHandler {
        private String mWhich;
        private byte[] mMessage;

        @Override
        public synchronized void prepare(Transaction txn, byte[] message) {
            mWhich = "prepare";
            mMessage = message;
            notify();
        }

        @Override
        public synchronized void prepareCommit(Transaction txn, byte[] message) {
            mWhich = "prepareCommit";
            mMessage = message;
            notify();
        }

        synchronized String waitForMessage() throws InterruptedException {
            while (mMessage == null) {
                wait();
            }
            return mWhich + ":" + new String(mMessage);
        }
    }

    @Test
    public void basic() throws Exception {
        basic(false);
    }

    @Test
    public void basicCommit() throws Exception {
        basic(true);
    }

    private void basic(boolean commit) throws Exception {
        PrepareHandler handler = mClientDb.prepareWriter("test");

        Transaction txn = mClientDb.newTransaction();

        if (commit) {
            handler.prepareCommit(txn, "message1".getBytes());
        } else {
            handler.prepare(txn, "message2".getBytes());

            try {
                handler.prepareCommit(txn, "message3".getBytes());
                fail();
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("already prepared"));
            }
        }

        // Force recovery on a copy of the database. Closing and reopen the database would
        // close the server, which in turn disposes all remote transaction objects and causes
        // them to explicitly roll back.
        Database copyDb = copyTempDatabase(getClass(), mServerDb, mServerConfig);

        String result = mServerHandler.waitForMessage();

        if (commit) {
            assertEquals(result, "prepareCommit:message1");
        } else {
            assertEquals(result, "prepare:message2");
        }
    }
}
