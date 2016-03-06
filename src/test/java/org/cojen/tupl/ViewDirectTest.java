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
public class ViewDirectTest extends ViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ViewDirectTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mDb = TestUtils.newTempDatabase(TestUtils.OpenMode.DIRECT);
    }
}
