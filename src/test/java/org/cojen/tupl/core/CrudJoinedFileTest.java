/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.core;

import java.io.File;

import java.util.EnumSet;

import java.util.function.Supplier;

import org.junit.*;

import org.cojen.tupl.DatabaseConfig;

import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.JoinedPageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CrudJoinedFileTest extends CrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudJoinedFileTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        Supplier<PageArray> factory = JoinedPageArray
            .factory(newPageArrayFactory(), 10, newPageArrayFactory());
        var config = new DatabaseConfig().dataPageArray(factory);
        decorate(config);
        mDb = newTempDatabase(getClass(), config);
    }

    void decorate(DatabaseConfig config) {
    }

    private Supplier<PageArray> newPageArrayFactory() throws Exception {
        var file = new File(newTempBaseFile(getClass()).getPath() + ".db");
        return FilePageArray.factory(4096, file, EnumSet.of(OpenOption.CREATE));
    }
}
