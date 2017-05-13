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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LargeValueFuzzTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargeValueFuzzTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void insertFuzz1() throws Exception {
        // Running with these parameters found several bugs. A large number of iterations was
        // required to find one of them.

        doInsertFuzz(2,        // threads
                     500_000,  // iterationsPerThread
                     2, 7,     // count min/max
                     10, 4000, // key min/max
                     10, 4000  // value min/max
                     );
    }

    @Test
    public void insertFuzz2() throws Exception {
        // Test with small keys and large values. Use a higher count to cover more cases.

        doInsertFuzz(2,        // threads
                     100_000,  // iterationsPerThread
                     2, 20,    // count min/max
                     10, 40,   // key min/max
                     10, 4000  // value min/max
                     );
    }

    @Test
    public void insertFuzz3() throws Exception {
        // Test with large keys and small values. Use a higher count to cover more cases.

        doInsertFuzz(2,        // threads
                     100_000,  // iterationsPerThread
                     2, 20,    // count min/max
                     10, 4000, // key min/max
                     10, 40    // value min/max
                     );
    }

    private void doInsertFuzz(int threads, int iterationsPerThread,
                              int countMin, int countMax,
                              int keyMin, int keyMax,
                              int valueMin, int valueMax)
        throws Exception
    {
        class Runner extends Thread {
            long seed;
            Throwable error;

            Runner(long seed) {
                this.seed = seed;
            }

            @Override
            public void run() {
                try {
                    doInsertFuzz(seed, iterationsPerThread,
                                 countMin, countMax, keyMin, keyMax, valueMin, valueMax);
                } catch (Throwable e) {
                    error = e;
                }
            }
        };

        // Base seed. Each thread gets a different seed and stores into a separate index. The
        // threads shouldn't interfere with each other.
        long seed = new Random().nextLong();

        Runner[] runners = new Runner[threads];
        for (int i=0; i<runners.length; i++) {
            (runners[i] = new Runner(seed++)).start();
        }

        for (Runner r : runners) {
            r.join();
        }

        for (Runner r : runners) {
            if (r.error != null) {
                throw new AssertionError("failed with seed: " + r.seed, r.error);
            }
        }
    }

    private void doInsertFuzz(long seed, int iterations,
                              int countMin, int countMax,
                              int keyMin, int keyMax,
                              int valueMin, int valueMax)
        throws Exception
    {
        Random rnd = new Random(seed);

        for (int i=0; i<iterations; i++) {
            Index ix = mDb.openIndex("test-" + seed);

            int count = rnd.nextInt(countMax - countMin) + countMin;
            for (int j=0; j<count; j++) {
                byte[] key = rndBytes(rnd, keyMin, keyMax);
                byte[] value = rndBytes(rnd, valueMin, valueMax);
                ix.store(Transaction.BOGUS, key, value);
                assertTrue(ix.verify(null));
            }

            mDb.deleteIndex(ix).run();
        }
    }

    private static byte[] rndBytes(Random rnd, int min, int max) {
        byte[] bytes = new byte[min + rnd.nextInt(max - min)];
        rnd.nextBytes(bytes);
        return bytes;
    }
}
