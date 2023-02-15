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

import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.dirmi.Connector;
import org.cojen.dirmi.Environment;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.Session;

import org.cojen.tupl.*;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RestorableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RestorableTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mEnv = RemoteUtils.createEnvironment();
        mConnector = new SuspendableConnector(123456);
        mEnv.connector(mConnector);

        SocketAddress addr = ss.getLocalSocketAddress();
        var remote = mEnv.connect(RemoteDatabase.class, Database.class.getName(), addr).root();

        mClientDb = ClientDatabase.from(remote);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            Utils.closeQuietly(mClientDb);
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());

        if (mEnv != null) {
            mEnv.close();
            mEnv = null;
            mConnector = null;
        }
    }

    private Environment mEnv;
    private SuspendableConnector mConnector;

    private Database mServerDb;
    private Database mClientDb;

    @Test
    public void indexAccess() throws Exception {
        var ix = mClientDb.openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        mConnector.suspend();

        try {
            ix.load(null, key);
            fail();
        } catch (RemoteException e) {
        }

        mConnector.resume();

        await: {
            for (int i=0; i<30; i++) {
                try {
                    assertArrayEquals(value, ix.load(null, key));
                    break await;
                } catch (RemoteException e) {
                    Thread.sleep(100);
                }
            }

            fail("Not restored");
        }
    }

    @Test
    public void leaderNotification() throws Exception {
        assertTrue(mClientDb.isLeader());

        mConnector.suspend();

        try {
            mClientDb.isLeader();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RemoteException);
        }

        var callback = new Runnable() {
            private boolean mReady;

            @Override
            public synchronized void run() {
                mReady = true;
                notify();
            }

            synchronized boolean waitUntilReady(long timeoutMillis) throws InterruptedException {
                long end = System.currentTimeMillis() + timeoutMillis;
                while (!mReady) {
                    wait(timeoutMillis);
                    if (System.currentTimeMillis() >= end) {
                        break;
                    }
                }
                return mReady;
            }
        };

        mClientDb.uponLeader(callback, null);

        mConnector.resume();

        assertTrue(callback.waitUntilReady(30_000));
    }

    private static class SuspendableConnector implements Connector {
        private final long mToken;
        private final List<Socket> mSockets;

        private boolean mSuspended;

        SuspendableConnector(long token) {
            mToken = token;
            mSockets = new ArrayList<>();
        }

        @Override
        public void connect(Session<?> session) throws IOException {
            Socket s;
            synchronized (this) {
                if (mSuspended) {
                    throw new IOException("suspended");
                }

                s = new Socket();
                s.connect(session.remoteAddress());

                try {
                    ClientDatabase.initConnection(s.getInputStream(), s.getOutputStream(), mToken);
                } catch (IOException e) {
                    Utils.closeQuietly(s);
                    throw e;
                }

                mSockets.add(s);
            }

            session.connected(s);
        }

        synchronized void suspend() {
            mSuspended = true;

            for (Socket s : mSockets) {
                try {
                    s.close();
                } catch (IOException e) {
                }
            }

            mSockets.clear();
        }

        synchronized void resume() {
            mSuspended = false;
        }
    }
}
