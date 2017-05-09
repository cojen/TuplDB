/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@RunWith(Parameterized.class)
@net.jcip.annotations.NotThreadSafe
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
        mDb = TestUtils.newTempDatabase(getClass(),
                                        new DatabaseConfig().pageSize(2048)
                                        .minCacheSize(1_000_000)
                                        .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
                                        .durabilityMode(DurabilityMode.NO_FLUSH)
                                        .directPageAccess(true));
    }
}
