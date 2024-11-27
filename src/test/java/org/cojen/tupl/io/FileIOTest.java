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

package org.cojen.tupl.io;

import static org.junit.Assert.*;
import static org.cojen.tupl.TestUtils.*;

import java.io.File;
import java.util.EnumSet;

import org.junit.*;

public class FileIOTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FileIOTest.class.getName());
    }

    private File file;

    @Before
    public void setup() throws Exception {
        file = newTempBaseFile(getClass());
    }

    @After
    public void teardown() throws Exception {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

    @Test
    public void preallocateGrowShrink() throws Exception {
        try (FileIO fio = FileIO.open(file, EnumSet.of(OpenOption.CREATE))) {
            for (long len = 0; len <= 50_000L; len += 5000L) {
                fio.expandLength(len, LengthOption.PREALLOCATE_ALWAYS);
                assertEquals(len, fio.length());
            }
            for (long len = 50_000L; len >= 0; len -= 5000L) {
                fio.truncateLength(len);
                assertEquals(len, fio.length());
            }
        }
    }
}
