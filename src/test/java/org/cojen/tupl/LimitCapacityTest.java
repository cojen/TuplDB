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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LimitCapacityTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LimitCapacityTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig().checkpointSizeThreshold(0);
        mDb = (LocalDatabase) newTempDatabase(config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected LocalDatabase mDb;

    @Test
    public void basic() throws Exception {
        mDb.capacityLimit(100_000_000L);
        Index ix = mDb.openIndex("test");

        byte[] key = new byte[6];
        byte[] value = new byte[0];

        Cursor fill = ix.newCursor(Transaction.BOGUS);

        for (int i=0; i<50_000_000; i++) {
            Utils.encodeInt48BE(key, 0, i);
            fill.findNearby(key);

            try {
                fill.store(value);
            } catch (DatabaseFullException e) {
                trim(mDb, ix);
                continue;
            }
        }

        mDb.compactFile(null, 0.95);

        Database.Stats stats = mDb.stats();
        long size = stats.totalPages() * stats.pageSize();

        assertTrue(size < mDb.capacityLimit());
    }

    private static void trim(LocalDatabase db, Index ix) throws Exception {
        db.capacityLimitOverride(-1);
        try {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            try {
                c.autoload(false);
                long count = 0;
                for (c.first(); c.key() != null; c.next()) {
                    c.store(null);
                    count++;
                    if (count >= 10_000_000) {
                        break;
                    }
                }
            } finally {
                c.reset();
            }
        } finally {
            db.capacityLimitOverride(0);
        }
    }
}
