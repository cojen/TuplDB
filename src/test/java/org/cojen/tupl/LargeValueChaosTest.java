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
    public void insertChaos() throws Exception {
        class Runner extends Thread {
            long seed;
            Throwable error;

            @Override
            public void run() {
                seed = new Random().nextLong();
                try {
                    doInsertChaos(seed, 500_000);
                } catch (Throwable e) {
                    error = e;
                }
            }
        };

        Runner[] runners = new Runner[2];
        for (int i=0; i<runners.length; i++) {
            (runners[i] = new Runner()).start();
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

    private void doInsertChaos(long seed, int iterations) throws Exception {
        Random rnd = new Random(seed);

        for (int i=0; i<iterations; i++) {
            Index ix = mDb.openIndex("test-" + seed);

            int count = rnd.nextInt(5) + 2;
            for (int j=0; j<count; j++) {
                byte[] key = rndBytes(rnd, 10, 4000);
                byte[] value = rndBytes(rnd, 10, 4000);
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
