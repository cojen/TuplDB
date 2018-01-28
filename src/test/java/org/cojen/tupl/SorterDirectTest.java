/*
 *  Copyright (C) 2018 Cojen.org
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

import org.junit.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SorterDirectTest extends SorterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SorterDirectTest.class.getName());
    }

    @Before
    @Override
    public void setup() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(true)
            .checkpointSizeThreshold(0)
            .minCacheSize(10_000_000)
            .maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        mDatabase = TestUtils.newTempDatabase(getClass(), config);
    }
}
