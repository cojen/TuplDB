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

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class TupleKeyTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TupleKeyTest.class.getName());
    }

    @Test
    public void test1() throws Exception {
        TupleKey t1 = TupleKey.make.with("hello".getBytes());
        TupleKey t2 = TupleKey.make.with("hello".getBytes());

        assertEquals(1, t1.size());
        assertEquals(byte[].class, t1.type(0));
        assertNotSame(t1.get(0), t2.get(0));

        try {
            t1.type(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            t1.get(1);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            t1.getString(0);
            fail();
        } catch (ClassCastException e) {
        }
 
        try {
            t1.get_int(0);
            fail();
        } catch (ClassCastException e) {
        }

        try {
            t1.get_long(0);
            fail();
        } catch (ClassCastException e) {
        }

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("([104, 101, 108, 108, 111])", t1.toString());

        TupleKey t3 = TupleKey.make.with("world".getBytes());
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());
    }

    @Test
    public void test2() throws Exception {
        TupleKey t1 = TupleKey.make.with(10, "hello".getBytes());
        TupleKey t2 = TupleKey.make.with(10, "hello".getBytes());

        assertEquals(2, t1.size());
        assertEquals(int.class, t1.type(0));
        assertEquals(byte[].class, t1.type(1));
        assertEquals(10, t1.get(0));
        assertEquals(10, t2.get_int(0));
        assertEquals(10L, t2.get_long(0));
        assertArrayEquals("hello".getBytes(), (byte[]) t2.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("(10, [104, 101, 108, 108, 111])", t1.toString());

        TupleKey t3 = TupleKey.make.with(2, "hello".getBytes());
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());

        TupleKey t4 = TupleKey.make.with(10, "world".getBytes());
        assertNotEquals(t1, t4);
        assertNotEquals(t1.toString(), t4.toString());
    }

    @Test
    public void test3() throws Exception {
        TupleKey t1 = TupleKey.make.with(10, (Object) "hello");
        TupleKey t2 = TupleKey.make.with(10, (Object) new String("hello"));

        assertEquals(2, t1.size());
        assertEquals(int.class, t1.type(0));
        assertEquals(Object.class, t1.type(1));
        assertEquals(10, t1.get(0));
        assertEquals(10, t2.get_int(0));
        assertEquals(10L, t2.get_long(0));
        assertEquals("hello", t2.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("(10, hello)", t1.toString());

        TupleKey t3 = TupleKey.make.with(2, (Object) "hello");
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());

        TupleKey t4 = TupleKey.make.with(10, (Object) "world");
        assertNotEquals(t1, t4);
        assertNotEquals(t1.toString(), t4.toString());
    }

    @Test
    public void test4() throws Exception {
        TupleKey t1 = TupleKey.make.with((Object) new int[] {1, 2}, true);
        TupleKey t2 = TupleKey.make.with((Object) new int[] {1, 2}, true);

        assertEquals(2, t1.size());
        assertEquals(Object.class, t1.type(0));
        assertEquals(boolean.class, t1.type(1));
        assertArrayEquals(new int[] {1, 2}, (int[]) t1.get(0));
        assertEquals(true, t2.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("([1, 2], true)", t1.toString());

        TupleKey t3 = TupleKey.make.with((Object) new int[] {1, 2}, false);
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());

        TupleKey t4 = TupleKey.make.with((Object) new int[] {2, 3}, true);
        assertNotEquals(t1, t4);
        assertNotEquals(t1.toString(), t4.toString());
    }

    @Test
    public void test5() throws Exception {
        TupleKey t1 = TupleKey.make.with((Object) new Object[] {"a", 2}, true);
        TupleKey t2 = TupleKey.make.with((Object) new Object[] {"a", 2}, true);

        assertEquals(2, t1.size());
        assertEquals(Object.class, t1.type(0));
        assertEquals(boolean.class, t1.type(1));
        assertArrayEquals(new Object[] {"a", 2}, (Object[]) t1.get(0));
        assertEquals(true, t2.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("([a, 2], true)", t1.toString());

        TupleKey t3 = TupleKey.make.with((Object) new Object[] {"a", 2}, false);
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());

        TupleKey t4 = TupleKey.make.with((Object) new Object[] {"b", 2}, true);
        assertNotEquals(t1, t4);
        assertNotEquals(t1.toString(), t4.toString());

        TupleKey t5 = TupleKey.make.with((Object) false, true);
        assertNotEquals(t1, t5);
        assertNotEquals(t1.toString(), t5.toString());
    }

    @Test
    public void test6() throws Exception {
        TupleKey t1 = TupleKey.make.with((Object) "hello", (Object) new String[] {"a", "b"});
        TupleKey t2 = TupleKey.make.with((Object) new String("hello"), (Object) new String[] {"a", "b"});

        assertEquals(2, t1.size());
        assertEquals(Object.class, t1.type(0));
        assertEquals(Object.class, t1.type(1));
        assertEquals("hello", t2.get(0));
        assertArrayEquals(new String[] {"a", "b"}, (Object[]) t1.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("(hello, [a, b])", t1.toString());

        TupleKey t3 = TupleKey.make.with((Object) "hello", (Object) new String[] {"a", "c"});
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());

        TupleKey t4 = TupleKey.make.with((Object) "world", (Object) new String[] {"a", "b"});
        assertNotEquals(t1, t4);
        assertNotEquals(t1.toString(), t4.toString());
    }

    @Test
    public void test7() throws Exception {
        TupleKey t1 = TupleKey.make.with((Object) "a", "b", (String) "c");
        TupleKey t2 = TupleKey.make.with((Object) new String("a"),
                                   new String("b"), (String) new String("c"));

        assertEquals(3, t1.size());
        assertEquals(Object.class, t1.type(0));
        assertEquals(String.class, t1.type(2));
        assertEquals("a", t2.get(0));
        assertEquals("a", t2.getString(0));
        assertEquals("b", t2.get(1));
        assertEquals("b", t2.getString(1));
        assertEquals("c", t2.get(2));
        assertEquals("c", t2.getString(2));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("(a, b, c)", t1.toString());

        TupleKey t3 = TupleKey.make.with((Object) "a", "x", (String) "c");
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());
    }

    @Test
    public void test8() throws Exception {
        TupleKey t1 = TupleKey.make.with(new Object[] {1, "b"}, (Object) new String("x"));
        TupleKey t2 = TupleKey.make.with(new Object[] {1, new String("b")}, (Object) "x");

        assertEquals(2, t1.size());
        assertEquals(Object[].class, t1.type(0));
        assertEquals(Object.class, t1.type(1));
        assertArrayEquals(new Object[] {1, new String("b")}, (Object[]) t2.get(0));
        assertEquals("x", t1.get(1));

        assertEquals(t1.hashCode(), t2.hashCode());
        assertEquals(t1, t2);
        assertEquals(t1.toString(), t2.toString());

        assertEquals("([1, b], x)", t1.toString());

        TupleKey t3 = TupleKey.make.with(new Object[] {2, "b"}, (Object) "x");
        assertNotEquals(t1, t3);
        assertNotEquals(t1.toString(), t3.toString());
    }
}
