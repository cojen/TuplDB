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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.DatabaseConfig;

import org.cojen.tupl.io.FilePageArray;
import org.cojen.tupl.io.JoinedPageArray;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.core.TestUtils.*;

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
        PageArray pa = JoinedPageArray.join(newFilePageArray(), 10, newFilePageArray());
        var config = new DatabaseConfig().dataPageArray(pa).directPageAccess(false);
        decorate(config);
        mDb = newTempDatabase(getClass(), config);
    }

    void decorate(DatabaseConfig config) {
    }

    FilePageArray newFilePageArray() throws Exception {
        var file = new File(newTempBaseFile(getClass()).getPath() + ".db");
        return new FilePageArray(4096, file, EnumSet.of(OpenOption.CREATE));
    }
}
