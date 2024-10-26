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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class IdSetTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IdSetTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        var tfm = new TempFileManager(newTempBaseFile(getClass()));
        var set = new IdSet(tfm);

        for (int i=0; i<2_000_000; i++) {
            assertTrue(set.add(i));
        }

        for (int i=0; i<2_000_000; i++) {
            assertFalse(set.add(i));
        }

        set.close();
    }
}
