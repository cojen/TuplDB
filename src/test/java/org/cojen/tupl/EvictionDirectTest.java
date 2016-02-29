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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class EvictionDirectTest extends EvictionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(EvictionDirectTest.class.getName());
    }

    @Parameters(name="{index}: size[{0}], autoLoad[{1}]")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { 1024, false }, // single item fits on a page, autoLoad is false
            { 1024, true }, // single item fits on a page, autoLoad is true
            { 256, false }, // at least two items on a page, autoLoad is false
            { 256, true }, // at least two items on a page, autoLoad is true
            { 128, false }, // more than two items on a page, autoLoad is false
            { 128, true },  // more than two items on a page, autoLoad is true
        });
    }
    

    public EvictionDirectTest(int size, boolean autoLoad) {
        super(size, autoLoad);
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mDb = TestUtils.newTempDatabase(new DatabaseConfig().pageSize(2048)
                                        .minCacheSize(1_000_000)
                                        .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
                                        .durabilityMode(DurabilityMode.NO_FLUSH)
                                        .directPageAccess(true));
    }
}
