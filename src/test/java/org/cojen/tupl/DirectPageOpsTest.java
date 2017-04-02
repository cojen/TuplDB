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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DirectPageOpsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DirectPageOpsTest.class.getName());
    }

    @Test
    public void arenaFill() throws Exception {
        final int count = 10;
        Object arena = DirectPageOps.p_arenaAlloc(4096, count);

        try {
            long ptr = DirectPageOps.p_calloc(arena, 100);
            fail();
        } catch (IllegalArgumentException e) {
            // Wrong size.
        }

        long last = 0;
        for (int i=0; i<count; i++) {
            long ptr = DirectPageOps.p_calloc(arena, 4096);
            assertTrue(DirectPageOps.inArena(ptr));
            if (i != 0) {
                assertEquals(4096, ptr - last);
            }
            last = ptr;
        }

        // Arena is depleted, and so the next allocation is outside the arena.
        long ptr = DirectPageOps.p_calloc(arena, 4096);
        assertFalse(DirectPageOps.inArena(ptr));

        DirectPageOps.p_delete(ptr);
        DirectPageOps.p_arenaDelete(arena);

        assertFalse(DirectPageOps.inArena(last));
    }

    @Test
    public void arenaSearch() throws Exception {
        long[] p1 = new long[10];
        Object a1 = DirectPageOps.p_arenaAlloc(4096, p1.length);
        allocAll(a1, p1, 4096);

        long[] p2 = new long[20];
        Object a2 = DirectPageOps.p_arenaAlloc(4096, p2.length);
        allocAll(a2, p2, 4096);

        long[] p3 = new long[30];
        Object a3 = DirectPageOps.p_arenaAlloc(4096, p3.length);
        allocAll(a3, p3, 4096);

        for (long p : p1) assertTrue(DirectPageOps.inArena(p));
        for (long p : p2) assertTrue(DirectPageOps.inArena(p));
        for (long p : p3) assertTrue(DirectPageOps.inArena(p));

        DirectPageOps.p_arenaDelete(a2);

        for (long p : p1) assertTrue(DirectPageOps.inArena(p));
        for (long p : p2) assertFalse(DirectPageOps.inArena(p));
        for (long p : p3) assertTrue(DirectPageOps.inArena(p));

        DirectPageOps.p_arenaDelete(a1);

        for (long p : p1) assertFalse(DirectPageOps.inArena(p));
        for (long p : p2) assertFalse(DirectPageOps.inArena(p));
        for (long p : p3) assertTrue(DirectPageOps.inArena(p));

        DirectPageOps.p_arenaDelete(a3);

        for (long p : p1) assertFalse(DirectPageOps.inArena(p));
        for (long p : p2) assertFalse(DirectPageOps.inArena(p));
        for (long p : p3) assertFalse(DirectPageOps.inArena(p));

        // Harmless double delete.
        DirectPageOps.p_arenaDelete(a3);

        // Ignored.
        DirectPageOps.p_arenaDelete(null);

        // Error.
        try {
            DirectPageOps.p_arenaDelete("hello");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    private void allocAll(Object arena, long[] pages, int size) {
        for (int i=0; i<pages.length; i++) {
            pages[i] = DirectPageOps.p_calloc(arena, size);
        }
    }
}
