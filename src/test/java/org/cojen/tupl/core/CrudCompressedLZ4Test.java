/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;

import org.cojen.tupl.io.PageCompressor;

import org.junit.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class CrudCompressedLZ4Test extends CrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudCompressedLZ4Test.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        var config = new DatabaseConfig()
            .cacheSize(10_000_000L) // small cache forces more decompression activity
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .compressPages(65536, 1_000_000L, PageCompressor.lz4());

        mDb = newTempDatabase(getClass(), config);
    }
}

