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

package org.cojen.tupl.rows;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ScanControllerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ScanControllerTest.class.getName());
    }

    @Test
    public void comparisons() throws Exception {
        byte[] a = "aaaa".getBytes();
        byte[] b = "bbbb".getBytes();
        byte[] c = "cccc".getBytes();
        byte[] d = "dddd".getBytes();
        byte[] e = "eeee".getBytes();

        var c0 = new TestController(b, false, d, false);

        assertTrue(c0.isTooLow(a));
        assertTrue(c0.isTooLow(b));
        assertFalse(c0.isTooLow(c));
        assertFalse(c0.isTooLow(d));
        assertFalse(c0.isTooLow(e));

        assertFalse(c0.isTooHigh(a));
        assertFalse(c0.isTooHigh(b));
        assertFalse(c0.isTooHigh(c));
        assertTrue(c0.isTooHigh(d));
        assertTrue(c0.isTooHigh(e));

        var c1 = new TestController(b, false, d, true);

        assertTrue(c1.isTooLow(a));
        assertTrue(c1.isTooLow(b));
        assertFalse(c1.isTooLow(c));
        assertFalse(c1.isTooLow(d));
        assertFalse(c1.isTooLow(e));

        assertFalse(c1.isTooHigh(a));
        assertFalse(c1.isTooHigh(b));
        assertFalse(c1.isTooHigh(c));
        assertFalse(c1.isTooHigh(d));
        assertTrue(c1.isTooHigh(e));

        var c2 = new TestController(b, true, d, false);

        assertTrue(c2.isTooLow(a));
        assertFalse(c2.isTooLow(b));
        assertFalse(c2.isTooLow(c));
        assertFalse(c2.isTooLow(d));
        assertFalse(c2.isTooLow(e));

        assertFalse(c2.isTooHigh(a));
        assertFalse(c2.isTooHigh(b));
        assertFalse(c2.isTooHigh(c));
        assertTrue(c2.isTooHigh(d));
        assertTrue(c2.isTooHigh(e));

        var c3 = new TestController(b, true, d, true);

        assertTrue(c3.isTooLow(a));
        assertFalse(c3.isTooLow(b));
        assertFalse(c3.isTooLow(c));
        assertFalse(c3.isTooLow(d));
        assertFalse(c3.isTooLow(e));

        assertFalse(c3.isTooHigh(a));
        assertFalse(c3.isTooHigh(b));
        assertFalse(c3.isTooHigh(c));
        assertFalse(c3.isTooHigh(d));
        assertTrue(c3.isTooHigh(e));

        assertEquals(0, c0.compareLow(c1));
        assertEquals(0, c1.compareLow(c0));
        assertEquals(0, c2.compareLow(c3));
        assertEquals(0, c3.compareLow(c2));

        assertEquals(0, c0.compareHigh(c2));
        assertEquals(0, c2.compareHigh(c0));
        assertEquals(0, c1.compareHigh(c3));
        assertEquals(0, c3.compareHigh(c1));

        assertEquals(1, c0.compareLow(c2));
        assertEquals(-1, c2.compareLow(c0));
        assertEquals(-1, c0.compareHigh(c1));
        assertEquals(1, c1.compareHigh(c0));

        var c4 = new TestController(a, false, e, false);

        assertEquals(-1, c4.compareLow(c0));
        assertEquals(1, c0.compareLow(c4));
        assertEquals(1, c4.compareHigh(c0));
        assertEquals(-1, c0.compareHigh(c4));

        var c5 = new TestController(null, false, null, false);

        assertEquals(-1, c5.compareLow(c0));
        assertEquals(1, c0.compareLow(c5));
        assertEquals(1, c5.compareHigh(c0));
        assertEquals(-1, c0.compareHigh(c5));

        assertEquals(0, c5.compareLow(c5));
        assertEquals(0, c5.compareHigh(c5));
    }

    static class TestController extends SingleScanController<Object> {
        TestController(byte[] lowBound, boolean lowInclusive,
                       byte[] highBound, boolean highInclusive)
        {
            super(lowBound, lowInclusive, highBound, highInclusive);
        }

        @Override
        public Object decodeRow(byte[] key, byte[] value, Object row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] encodeKey(Object row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] encodeValue(Object row) {
            throw new UnsupportedOperationException();
        }
    }
}
