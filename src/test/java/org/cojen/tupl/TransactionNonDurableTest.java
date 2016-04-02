/*
 *  Copyright 2012-2015 Cojen.org
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
public class TransactionNonDurableTest extends TransactionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TransactionNonDurableTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    @Override
    protected Database newTempDatabase() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.directPageAccess(false);
        config.maxCacheSize(200000000);
        return Database.open(config);
    }
}
