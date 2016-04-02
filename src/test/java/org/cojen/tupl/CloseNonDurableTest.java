/*
 *  Copyright 2013-2015 Cojen.org
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CloseNonDurableTest extends CloseTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CloseNonDurableTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = Database.open(new DatabaseConfig().directPageAccess(false));
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    @Test @Ignore
    public void cacheChurn() {
        // Non-durable database cannot exhaust the cache.
    }

    @Test
    public void dropChurn() throws Exception {
        // Verify that dropping indexes allows space to be reclaimed.

        for (int i=0; i<10000; i++) {
            Index ix = mDb.openIndex("ix-" + i);
            byte[] key = "hello".getBytes();
            ix.store(null, key, ("world-" + i).getBytes());
            ix.store(null, key, null);
            ix.drop();
        }
    }
}
