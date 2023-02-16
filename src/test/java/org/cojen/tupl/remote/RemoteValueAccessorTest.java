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

import org.cojen.tupl.core.ValueAccessorTest;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteValueAccessorTest extends ValueAccessorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteValueAccessorTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
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

    private Database mServerDb;

    @Override
    protected void readFragmented(boolean useWrite, boolean setLength) throws Exception {
        // Speed up the test by not bothering with extremely large values.
        readFragmented(useWrite, setLength, 15);
    }

    @Test
    @Override
    public void readLargeFragmented() throws Exception {
        // Speed up the test by not bothering with extremely large values.
        readLargeFragmented(10);
    }

    @Test
    @Override
    public void extendExisting() throws Exception {
        // Speed up the test by not bothering with extremely large values.

        int[] from = {
            0, 100, 200, 1000, 2000, 10000, 100_000
        };

        long[] to = {
            0, 101, 201, 1001, 5000, 10001, 100_001
        };

        for (int fromLen : from) {
            for (long toLen : to) {
                if (toLen > fromLen) {
                    try {
                        extendExisting(fromLen, toLen, true);
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        throw e;
                    }
                }
            }
        }
    }

    @Test
    @Override
    public void fill() throws Exception {
        // A reasonably sized buffer is required. Also, cannot verify value length because
        // writes are being applied asynchronously.
        fill(5000, false);
    }

    @Override
    public void undoClearNonFragmented() throws Exception {
        // Override and do nothing. No need to test database rollback during recovery.
    }

    @Override
    public void fullyTruncateSparseValue() throws Exception {
        // Override and do nothing. It doesn't add value for remote testing, and it doesn't
        // work anyhow because the original test depends on implementation specifics.
    }
}
