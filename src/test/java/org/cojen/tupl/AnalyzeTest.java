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
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
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

        assertEquals(average.entryCount(), count, count * 0.05);
        assertEquals(average.keyBytes(), keyBytes, keyBytes * 0.05);
        assertEquals(average.valueBytes(), valueBytes, valueBytes * 0.05);

        // Compare to emperical data.
        assertEquals(average.freeBytes(), 2200000, 2200000 * 0.05);
        assertEquals(average.totalBytes(), 9080000, 9080000 * 0.05);
    }
}
