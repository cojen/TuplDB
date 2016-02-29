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

import org.junit.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ExtraLargeValueDirectTest extends LargeValueTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ExtraLargeValueDirectTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.durabilityMode(DurabilityMode.NO_FLUSH);
        // Use smaller page size so that more inode levels are required without
        // requiring super large arrays.
        config.pageSize(512);
        config.directPageAccess(true);
        mDb = TestUtils.newTempDatabase(config);
    }

}
