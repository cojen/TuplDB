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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@RunWith(Parameterized.class)
public class CountTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CountTest.class.getName());
    }

    @Parameters(name="{index}: fullCount[{0}], increment[{1}], mode[{2}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { 10, 1, Mode.NO_CHECKPOINTS },
            { 10, 1, Mode.EXPLICIT_CHECKPOINTS },
            { 10, 1, Mode.AUTO_CHECKPOINTS },

            { 100, 11, Mode.NO_CHECKPOINTS },
            { 100, 12, Mode.EXPLICIT_CHECKPOINTS },
            { 100, 13, Mode.AUTO_CHECKPOINTS },

            { 1000, 110, Mode.NO_CHECKPOINTS },
            { 1000, 120, Mode.EXPLICIT_CHECKPOINTS },
            { 1000, 131, Mode.AUTO_CHECKPOINTS },

            { 10_000, 1103, Mode.NO_CHECKPOINTS },
            { 10_000, 1202, Mode.EXPLICIT_CHECKPOINTS },
            { 10_000, 1301, Mode.AUTO_CHECKPOINTS },

            { 100_000, 5035, Mode.NO_CHECKPOINTS },
            { 100_000, 4025, Mode.EXPLICIT_CHECKPOINTS },
            { 100_000, 3015, Mode.AUTO_CHECKPOINTS },

            { 1_000_000, 25035, Mode.NO_CHECKPOINTS },
            { 1_000_000, 24025, Mode.EXPLICIT_CHECKPOINTS },
            { 1_000_000, 23015, Mode.AUTO_CHECKPOINTS },
        });
    }

    static enum Mode {
        NO_CHECKPOINTS, EXPLICIT_CHECKPOINTS, AUTO_CHECKPOINTS
    }

    public CountTest(int fullCount, int increment, Mode mode) {
        mFullCount = fullCount;
        mIncrement = increment;
        mMode = mode;
    }

    private final int mFullCount;
    private final int mIncrement;
    private final Mode mMode;

    private Database mDb;

    @Before
    public void setup() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .directPageAccess(false);

        if (mMode != Mode.AUTO_CHECKPOINTS) {
            config.checkpointRate(-1, null);
        }

        mDb = newTempDatabase(config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    @Test
    public void countStuff() throws Exception {
        Index ix = mDb.openIndex("test");

        assertEquals(0, ix.count(null, null));

        for (int i=0; i<mFullCount; i++) {
            ix.store(null, key(i), value(i));
        }

        verifyCount(ix);

        if (mMode == Mode.EXPLICIT_CHECKPOINTS) {
            mDb.checkpoint();
            verifyCount(ix);
            verifyCount(ix);
            mDb.checkpoint();
            verifyCount(ix);
        }
    }

    private void verifyCount(Index ix) throws Exception {
        for (int low=-1; low <= mFullCount; low += mIncrement) {
            byte[] lowKey;
            if (low < 0) {
                lowKey = null;
                low = -mIncrement; // will be zero next time
            } else {
                lowKey = key(low);
            }

            for (int high=0; high <= mFullCount + 1; high += mIncrement) {
                byte[] highKey;
                if (high > mFullCount) {
                    highKey = null;
                } else {
                    highKey = key(high);
                }

                int expect;
                if (lowKey == null) {
                    if (highKey == null) {
                        expect = mFullCount;
                    } else {
                        expect = high;
                    }
                } else if (highKey == null) {
                    expect = mFullCount - low;
                } else {
                    expect = Math.max(0, high - low);
                }

                long count = ix.count(lowKey, highKey);
                assertEquals(expect, count);
            }
        }
    }

    private static byte[] key(int i) {
        byte[] key = new byte[4];
        Utils.encodeIntBE(key, 0, i);
        return key;
    }

    private static byte[] value(int i) {
        return ("value-" + i).getBytes();
    }
}
