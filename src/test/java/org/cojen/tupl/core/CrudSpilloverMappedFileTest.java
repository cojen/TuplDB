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

import java.io.File;

import java.util.EnumSet;

import java.util.function.Supplier;

import org.junit.*;

import org.cojen.tupl.DatabaseConfig;

import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.SpilloverPageArray;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class CrudSpilloverMappedFileTest extends CrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudSpilloverMappedFileTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        int pageSize = 4096;
        int joinIndex = 10;
        var options = EnumSet.of(OpenOption.CREATE, OpenOption.RANDOM_ACCESS);

        var firstFile = new File(newTempBaseFile(getClass()).getPath() + ".db");
        var first = MappedPageArray.factory(pageSize, joinIndex, firstFile, options);

        var secondFile = new File(newTempBaseFile(getClass()).getPath() + ".db");
        var second = MappedPageArray.factory(pageSize, 100000, secondFile, options);

        Supplier<PageArray> factory = SpilloverPageArray.factory(first, joinIndex, second);
        var config = new DatabaseConfig().dataPageArray(factory);
        mDb = newTempDatabase(getClass(), config);
    }
}
