/*
 *  Copyright 2015-2015 Cojen.org
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

import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.deleteTempDatabases;
import static org.cojen.tupl.TestUtils.newTempDatabase;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EvictionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(EvictionTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(new DatabaseConfig().pageSize(2048)
                                            .minCacheSize(1_000_000)
                                            .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
                                            .durabilityMode(DurabilityMode.NO_FLUSH));
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void testEvictBasic() throws IOException {
        Index ix = mDb.openIndex("test");
        int initialRecordCount = 100_000;
        for (int i=0; i<initialRecordCount; i++) { // tree of depth 4
            String key = textOfLength(i, 'k', 1024);
            String val = textOfLength(i, 'v', 1024);
            ix.store(null, key.getBytes(), val.getBytes());
        }
        byte[][] keyRef = new byte[1][];
        byte[][] valueRef = new byte[1][];
        int recordCount = initialRecordCount;
        {   // basic eviction
            assertEquals(2048, ix.evict(null, null, null, keyRef, valueRef, 1));
            assertEquals(--recordCount, ix.count(null, null));
            assertNull(ix.load(null, keyRef[0]));
        }
        {   // basic eviction, do not return key/value
            assertEquals(2048, ix.evict(null, null, null, null, null, 1));
            assertEquals(--recordCount, ix.count(null, null));
        }
        {   // basic eviction, return key only
            assertEquals(2048, ix.evict(null, null, null, keyRef, null, 1));
            assertEquals(--recordCount, ix.count(null, null));
            assertNull(ix.load(null, keyRef[0]));
        }
        {   // empty range
            assertEquals(0, ix.evict(null, "a".getBytes(), "b".getBytes(), keyRef, valueRef, 1));
            assertEquals(recordCount, ix.count(null, null));
        }
        {   // evict on nodes which are in cache
            ix.newCursor(null).find("a".getBytes());    // loads rightmost nodes at all levels into cache
            int highKey = initialRecordCount-1;
            int lowKey = initialRecordCount-2;
            assertEquals(2048, ix.evict(null, String.valueOf(lowKey).getBytes(), String.valueOf(highKey).getBytes(), keyRef, valueRef, 1));
            assertTrue(new String(keyRef[0]).startsWith("9999"));
            assertTrue(new String(valueRef[0]).startsWith("9999"));
            assertEquals(--recordCount, ix.count(null, null));
        }
        {   // ghost records
            Transaction txn = mDb.newTransaction();
            int lowKey = initialRecordCount-11;
            int highKey = initialRecordCount-20;
            for (int i=lowKey; i<=highKey; i++) {
                String key = textOfLength(i, 'k', 1019);
                ix.delete(txn, key.getBytes());
            }
            assertEquals(0, ix.evict(null, String.valueOf(lowKey).getBytes(), String.valueOf(highKey).getBytes(), keyRef, valueRef, 1));
            txn.reset();
        }
        VerificationObserver observer = new VerificationObserver();
        ix.verify(observer);
        assertFalse(observer.failed);
    }

    private String textOfLength(int prefix, char c, int len) {
        len-=5;
        char[] chars = new char[len];
        Arrays.fill(chars, c);
        return String.format("%05d%s", prefix, new String(chars));
    }
}
