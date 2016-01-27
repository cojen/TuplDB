/*
 *  Copyright 2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
