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

import org.junit.Test;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SnapshotMappedTest extends SnapshotTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotMappedTest.class.getName());
    }

    @Override
    public void decorate(DatabaseConfig config) throws Exception {
        config.mapDataFiles(true);
    }

    @Test
    @Override
    public void snapshot() throws Exception {
        if (TestUtils.is64bit()) {
            super.snapshot();
        }
    }
}
