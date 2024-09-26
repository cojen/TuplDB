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

package org.cojen.tupl.core;

import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.cojen.tupl.io.MappedPageArray;

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
        assumeTrue(MappedPageArray.isSupported());

        final int count = 10;
        Object arena = DirectPageOps.p_arenaAlloc(4096, count, null);

        try {
            long ptr = DirectPageOps.p_callocPage(arena, 100);
            fail();
        } catch (IllegalArgumentException e) {
            // Wrong size.
        }

        long last = 0;
        for (int i=0; i<count; i++) {
            long ptr = DirectPageOps.p_callocPage(arena, 4096);
            assertTrue(DirectPageOps.inArena(ptr));
            if (i != 0) {
                assertEquals(4096, ptr - last);
            }
            last = ptr;
        }

        // Arena is depleted, and so the next allocation is outside the arena.
        long ptr = DirectPageOps.p_callocPage(arena, 4096);
        assertFalse(DirectPageOps.inArena(ptr));

        DirectPageOps.p_delete(ptr);
        DirectPageOps.p_arenaDelete(arena);

        assertFalse(DirectPageOps.inArena(last));
    }

    @Test
    public void arenaSearch() throws Exception {
        assumeTrue(MappedPageArray.isSupported());

        var p1 = new long[10];
        Object a1 = DirectPageOps.p_arenaAlloc(4096, p1.length, null);
        allocAll(a1, p1, 4096);

        var p2 = new long[20];
        Object a2 = DirectPageOps.p_arenaAlloc(4096, p2.length, null);
        allocAll(a2, p2, 4096);

        var p3 = new long[30];
        Object a3 = DirectPageOps.p_arenaAlloc(4096, p3.length, null);
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
            pages[i] = DirectPageOps.p_callocPage(arena, size);
        }
    }

    @Test
    public void varInt() throws Exception {
        long page = DirectPageOps.p_alloc(100);

        // Pairs of value to encode and expected length.
        int[] values = {
            Integer.MIN_VALUE, 5,
            -10, 5,
            0, 1,
            1, 1,
            200, 2,
            20000, 3,
            12345678, 4,
            1234567890, 5,
            Integer.MAX_VALUE, 5,
        };

        for (int i=0; i<values.length; i+=2) {
            int val = values[i];
            int len = values[i + 1];
            assertEquals(len, DirectPageOps.p_uintPutVar(page, 1, val) - 1);
            long decoded = DirectPageOps.p_uintGetVar(page, 1);
            assertEquals(val, (int) decoded);
            assertEquals(1 + len, (int) (decoded >> 32));
        }

        DirectPageOps.p_delete(page);
    }

    @Test
    public void varLong() throws Exception {
        long page = DirectPageOps.p_alloc(100);

        // Pairs of value to encode and expected length.
        long[] values = {
            Long.MIN_VALUE, 9,
            -10, 9,
            0, 1,
            1, 1,
            200, 2,
            20000, 3,
            12345678, 4,
            1234567890, 5,
            1234567890123L, 6,
            123456789012345L, 7,
            12345678901234567L, 8,
            1234567890123456780L, 9,
            Long.MAX_VALUE, 9,
        };

        var ref = new IntegerRef.Value();

        for (int i=0; i<values.length; i+=2) {
            long val = values[i];
            int len = (int) values[i + 1];
            assertEquals(len, DirectPageOps.p_ulongPutVar(page, 1, val) - 1);
            ref.set(1);
            assertEquals(val, DirectPageOps.p_ulongGetVar(page, ref));
            assertEquals(len, ref.get() - 1);
        }

        DirectPageOps.p_delete(page);
    }
}
