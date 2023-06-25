/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.util;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CanonicalizerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CanonicalizerTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        record Rec1(int a) {}
        record Rec2(String b) {}

        var cache = new Canonicalizer();

        Rec1 r1 = cache.apply(new Rec1(10));
        Rec2 r2 = cache.apply(new Rec2("hello"));

        assertSame(r1, cache.apply(new Rec1(10)));
        assertSame(r2, cache.apply(new Rec2("hello")));

        assertEquals(2, cache.size());
    }

    @Test
    public void flood() throws Exception {
        record Rec(int a) {}

        var cache = new Canonicalizer();

        int lastSize = 0;

        var rnd = new java.util.Random();

        while (true) {
            var rec = new Rec(rnd.nextInt());
            assertEquals(rec, cache.apply(rec));
            int size = cache.size();
            if (size < lastSize) {
                break;
            }
            lastSize = size;
        }

        assertTrue(lastSize > 0);
    }
}
