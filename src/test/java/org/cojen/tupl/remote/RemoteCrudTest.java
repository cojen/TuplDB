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

import java.util.Arrays;

import java.util.concurrent.TimeUnit;

import org.junit.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.CrudTest;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteCrudTest extends CrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteCrudTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), 111, 123456);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        deleteTempDatabases(getClass());
        mServerDb = null;
    }

    @Override
    protected <T extends Thread> T startAndWaitUntilBlocked(T t) throws InterruptedException {
        t.start();

        StackTraceElement[] lastTrace = null;

        while (true) {
            Thread.State state = t.getState();
            if (state != Thread.State.NEW && state != Thread.State.RUNNABLE) {
                return t;
            }

            // A thread which is blocked reading from a Socket reports a RUNNABLE state. Need
            // to inspect the stack trace instead.

            StackTraceElement[] trace = t.getStackTrace();
            if (Arrays.equals(trace, lastTrace) && trace.length != 0) {
                StackTraceElement top = trace[0];
                if (top.isNativeMethod()
                    && top.getClassName().contains("Socket")
                    && top.getMethodName().contains("read"))
                {
                    return t;
                }
            }

            lastTrace = trace;

            Thread.sleep(100);
        }
    }

    private Database mServerDb;

    @Test
    @Override
    public void testFill() throws Exception {
        View ix = openIndex("test");
        testFill(ix, 10);
    }
}
