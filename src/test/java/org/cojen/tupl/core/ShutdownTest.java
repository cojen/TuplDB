/*
 *  Copyright 2019 Cojen.org
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ShutdownTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ShutdownTest.class.getName());
    }

    protected void decorate(DatabaseConfig config) throws Exception {
    }

    @Before
    public void createTempDb() throws Exception {
        createTempDb(DurabilityMode.NO_FLUSH);
    }

    private void createTempDb(DurabilityMode mode) throws Exception {
        mConfig = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(mode);
        decorate(mConfig);
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
    public void shutdown() throws Exception {
        // Verifies that no inserts are lost when database is concurrently shutdown.

        var lastNum = new AtomicLong();
        var exception = new AtomicReference<Throwable>();

        var t1 = new Thread(() -> {
            try {
                Index ix = mDb.openIndex("test");
                long num = 1;
                while (true) {
                    ix.insert(null, ("key-" + num).getBytes(), ("value-" + num).getBytes());
                    lastNum.set(num);
                    num++;
                }
            } catch (DatabaseException e) {
                // Expected when closed.
            } catch (Throwable e) {
                // Not expected.
                exception.set(e);
            }
        });

        t1.start();

        do {
            Thread.sleep(1000);
        } while (lastNum.get() == 0);

        mDb.shutdown();

        t1.join();
        assertNull(exception.get());

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        Index ix = mDb.openIndex("test");
        assertTrue(ix.count(null, null) >= lastNum.get());
    }
}
