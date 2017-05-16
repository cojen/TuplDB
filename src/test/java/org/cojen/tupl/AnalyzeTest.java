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

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class AnalyzeTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AnalyzeTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.durabilityMode(DurabilityMode.NO_FLUSH);
        config.directPageAccess(false);
        config.checkpointRate(-1, null);
        mDb = newTempDatabase(getClass(), config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Index openIndex(String name) throws Exception {
        return mDb.openIndex(name);
    }

    protected Database mDb;

    @Test
    public void analyze() throws Exception {
        // Simple regression test which checks that analyze returns something reasonable.

        Index ix = openIndex("stuff");

        {
            Index.Stats stats = ix.analyze(null, null);
            assertEquals(0, stats.entryCount(), 0);
            assertEquals(0, stats.keyBytes(), 0);
            assertEquals(0, stats.valueBytes(), 0);
            assertEquals(0, stats.freeBytes(), 0);
            assertEquals(0, stats.totalBytes(), 0);
        }

        Random rnd = new Random(98765);

        final int count = 100_000;

        long keyBytes = 0;
        long valueBytes = 0;

        for (int i=0; i<count; i++) {
            byte[] key = new byte[rnd.nextInt(30) + 1];
            rnd.nextBytes(key);
            keyBytes += key.length;

            byte[] value = new byte[rnd.nextInt(100) + 1];
            rnd.nextBytes(value);
            valueBytes += value.length;

            ix.store(Transaction.BOGUS, key, value);
        }

        final int probeCount = 10000;
        Index.Stats total = null;

        for (int i=0; i<probeCount; i++) {
            Index.Stats stats = ix.analyze(null, null);
            if (total == null) {
                total = stats;
            } else {
                total = total.add(stats);
            }
        }

        Index.Stats average = total.divideAndRound(probeCount);

        assertEquals(count, average.entryCount(), count * 0.1);
        assertEquals(keyBytes, average.keyBytes(), keyBytes * 0.1);
        assertEquals(valueBytes, average.valueBytes(), valueBytes * 0.1);

        // Compare to emperical data.
        assertEquals(2200000, average.freeBytes(), 2200000 * 0.1);
        assertEquals(9080000, average.totalBytes(), 9080000 * 0.1);
    }

    @Test
    public void analyzeLargeKeyAndValue() throws Exception {
        Index ix = openIndex("stuff");

        byte[] key = new byte[10000];
        byte[] value = new byte[999999];

        ix.store(Transaction.BOGUS, key, value);

        Index.Stats stats = ix.analyze(null, null);
        assertEquals(1, stats.entryCount(), 0);
        assertEquals(key.length, stats.keyBytes(), 0);
        assertEquals(value.length, stats.valueBytes(), 0);

        // Compare to emperical data.
        assertEquals(2242, stats.freeBytes(), 0);

        // Compare to expected data.
        assertEquals(1019904, stats.totalBytes(), 0);
    }
}
