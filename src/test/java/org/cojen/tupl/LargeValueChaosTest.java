/*
 *  Copyright 2016 Cojen.org
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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LargeValueChaosTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargeValueChaosTest.class.getName());
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

    protected Database mDb;

    @Test
    public void insertChaos1() throws Exception {
        // Running with these parameters found several bugs. A large number of iterations was
        // required to find one of them.

        doInsertChaos(2,        // threads
                      500_000,  // iterationsPerThread
                      2, 7,     // count min/max
                      10, 4000, // key min/max
                      10, 4000  // value min/max
                      );
    }

    @Test
    public void insertChaos2() throws Exception {
        // Test with small keys and large values. Use a higher count to cover more cases.

        doInsertChaos(2,        // threads
                      100_000,  // iterationsPerThread
                      2, 20,    // count min/max
                      10, 40,   // key min/max
                      10, 4000  // value min/max
                      );
    }

    @Test
    public void insertChaos3() throws Exception {
        // Test with large keys and small values. Use a higher count to cover more cases.

        doInsertChaos(2,        // threads
                      100_000,  // iterationsPerThread
                      2, 20,    // count min/max
                      10, 4000, // key min/max
                      10, 40    // value min/max
                      );
    }

    private void doInsertChaos(int threads, int iterationsPerThread,
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
                    doInsertChaos(seed, iterationsPerThread,
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

    private void doInsertChaos(long seed, int iterations,
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
