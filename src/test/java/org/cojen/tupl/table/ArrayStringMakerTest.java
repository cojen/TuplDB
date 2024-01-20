/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandle;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ArrayStringMakerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ArrayStringMakerTest.class.getName());
    }

    @Test
    public void basic() throws Throwable {
        {
            MethodHandle mh = ArrayStringMaker.make(int[].class, false);

            StringBuilder bob = null;
            bob = (StringBuilder) mh.invokeExact(bob, new int[] {10, -10, 100}, 2);
            assertEquals("[10, -10, ...]", bob.toString());
            bob.setLength(0);
            bob = (StringBuilder) mh.invokeExact(bob, new int[] {10, -10, 100}, 1000);
            assertEquals("[10, -10, 100]", bob.toString());

            MethodHandle mh2 = ArrayStringMaker.make(int[].class, false);
            assertSame(mh, mh2);
        }

        {
            MethodHandle mh = ArrayStringMaker.make(long[].class, true);
            StringBuilder bob = null;
            bob = (StringBuilder) mh.invokeExact(bob, new long[] {10, -10, 100}, 2);
            assertEquals("[10, 18446744073709551606, ...]", bob.toString());
        }
 
        {
            MethodHandle mh = ArrayStringMaker.make(long[].class, false);
            StringBuilder bob = null;
            bob = (StringBuilder) mh.invokeExact(bob, new long[] {10, -10, 100}, 2);
            assertEquals("[10, -10, ...]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(Long[].class, true);
            StringBuilder bob = null;
            bob = (StringBuilder) mh.invokeExact(bob, new Long[] {10L, -10L, 100L}, 2);
            assertEquals("[10, 18446744073709551606, ...]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(Long[].class, false);
            StringBuilder bob = null;
            bob = (StringBuilder) mh.invoke(bob, new Long[] {10L, -10L, 100L}, 2);
            assertEquals("[10, -10, ...]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(String[][].class, true);
            StringBuilder bob = null;

            String[][] array = {
                {"a", "b", "c"},
                {"d", "e", "f"},
                {"g", "h", "i"}
            };

            bob = (StringBuilder) mh.invoke(bob, array, 2);
            assertEquals("[[a, b, ...], [d, e, ...], ...]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(Integer[].class, true);
            StringBuilder bob = null;
            Integer[] array = {10, 20, 30, null, 40, 50};
            bob = (StringBuilder) mh.invoke(bob, array, 20);
            assertEquals("[10, 20, 30, null, 40, 50]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(Boolean[].class, true);
            StringBuilder bob = null;
            Boolean[] array = null;
            bob = (StringBuilder) mh.invoke(bob, array, 20);
            assertEquals("null", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(char[].class, true);
            StringBuilder bob = null;
            bob = (StringBuilder) mh.invoke(bob, new char[] {'a', 'b', 'c'}, 20);
            assertEquals("[a, b, c]", bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(byte[].class, true);
            StringBuilder bob = null;

            byte[] array = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12,
            };

            bob = (StringBuilder) mh.invoke(bob, array, Integer.MAX_VALUE);
            assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, " +
                         "255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244]",
                         bob.toString());
        }

        {
            MethodHandle mh = ArrayStringMaker.make(short[].class, true);
            StringBuilder bob = null;

            short[] array = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11,
            };

            bob = (StringBuilder) mh.invoke(bob, array, array.length - 1);
            assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 65535, 65534, 65533, " +
                         "65532, 65531, 65530, 65529, 65528, 65527, 65526, ...]",
                         bob.toString());

            bob.setLength(0);
            bob = (StringBuilder) mh.invoke(bob, array, 0);
            assertEquals("[...]", bob.toString());

            bob.setLength(0);
            bob = (StringBuilder) mh.invoke(bob, new short[0], 0);
            assertEquals("[]", bob.toString());
        }
    }
}
