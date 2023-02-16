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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.repl.ReplicatorConfig;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LeaderNotifyTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LeaderNotifyTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        var ss1 = new ServerSocket(0);

        ReplicatorConfig replConfig1 = new ReplicatorConfig()
            .groupToken(123456)
            .localSocket(ss1);

        DatabaseConfig config1 = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(replConfig1);

        mServerDb1 = newTempDatabase(getClass(), config1);

        var ss2 = new ServerSocket(0);

        ReplicatorConfig replConfig2 = new ReplicatorConfig()
            .groupToken(123456)
            .localSocket(ss2)
            .addSeed(ss1.getLocalSocketAddress());

        DatabaseConfig config2 = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(replConfig2);

        mServerDb2 = newTempDatabase(getClass(), config2);

        mClientDb1 = Database.connect(ss1.getLocalSocketAddress(), null, 123456);
        mClientDb2 = Database.connect(ss2.getLocalSocketAddress(), null, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb1 != null) {
            mClientDb1.close();
            mClientDb1 = null;
        }

        if (mClientDb2 != null) {
            mClientDb2.close();
            mClientDb2 = null;
        }

        mServerDb1 = null;
        mServerDb2 = null;

        closeTempDatabases(getClass());
        deleteTempFiles(getClass());
    }

    private Database mServerDb1, mServerDb2;
    private Database mClientDb1, mClientDb2;

    @Test
    public void failover() throws Exception {
        class Listener implements Runnable {
            private boolean mReady;

            @Override
            public synchronized void run() {
                mReady = true;
                notify();
            }

            synchronized boolean isReady() {
                return mReady;
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

            synchronized void reset() {
                mReady = false;
            }
        }

        mClientDb1.openIndex("test");

        long end = System.currentTimeMillis() + 30_000;
        while (mClientDb2.findIndex("test") == null) {
            if (System.currentTimeMillis() >= end) {
                fail("Replica not caught up");
            }
            sleep(100);
        }

        var acquire1 = new Listener();
        var lost1 = new Listener();
        var acquire2 = new Listener();
        var lost2 = new Listener();

        mClientDb1.uponLeader(acquire1, lost1);
        mClientDb2.uponLeader(acquire2, lost2);

        acquire1.waitUntilReady(30_000);
        assertFalse(lost1.isReady());
        assertFalse(acquire2.isReady());
        assertFalse(lost2.isReady());

        acquire1.reset();

        assertTrue(mClientDb1.failover());

        lost1.waitUntilReady(30_000);
        assertFalse(acquire1.isReady());
        acquire2.waitUntilReady(30_000);
        assertFalse(lost2.isReady());
    }
}
