/*
 *  Copyright (C) 2019 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Supplementary tests for LHashTable. Most cases are covered indirectly by other tests.
 *
 * @author Brian S O'Neill
 */
public class LHashTableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LHashTableTest.class.getName());
    }

    @Test
    public void clear() {
        var ht = new LHashTable.Int(10);

        ht.put(1).value = 2;
        ht.put(3).value = 4;
        assertEquals(2, ht.get(1).value);
        assertEquals(4, ht.get(3).value);
        assertEquals(2, ht.size());

        ht.clear(10);
        assertNull(ht.get(1));
        assertNull(ht.get(3));
        assertEquals(0, ht.size());
    }

    @Test
    public void replace() {
        var ht = new LHashTable.Int(10);

        ht.replace(0x100).value = 2;
        assertEquals(2, ht.get(0x100).value);
        ht.replace(0x100).value = 3;
        assertEquals(3, ht.get(0x100).value);

        // Again some collisions.
        ht.put(0x200).value = 1;
        ht.replace(0x100).value = 4;
        assertEquals(4, ht.get(0x100).value);
        assertEquals(2, ht.size());

        // Traverse and remove first in the chain, to improve code coverage.
        ht.traverse(entry -> {
            return entry.value == 1; // remove just one
        });

        assertEquals(1, ht.size());
        assertNull(ht.get(0x200));
        assertEquals(4, ht.get(0x100).value);
    }

    @Test
    public void remove() {
        var ht = new LHashTable.Int(10);

        assertNull(ht.remove(1));
        ht.put(1).value = 2;
        assertNull(ht.remove(2));
        assertEquals(2, ht.remove(1).value);
        assertNull(ht.remove(1));
    }
}
