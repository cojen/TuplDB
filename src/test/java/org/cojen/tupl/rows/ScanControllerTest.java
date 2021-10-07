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

import java.util.HashSet;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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

    @Test
    public void mergedScans() throws Exception {
        Database db = Database.open(new DatabaseConfig());
        Table<MyRow> table = db.openTable(MyRow.class);
        Table<MyRow> altKey = table.alternateKeyTable("a", "b");

        int id = 0;
        for (int a=0; a<10; a++) {
            for (int b=0; b<10; b++) {
                var row = table.newRow();
                row.id(id++);
                row.a(a);
                row.b(b);
                table.insert(null, row);
            }
        }

        var rnd = new Random(0xcafebabe);

        var bob = new StringBuilder(64);
        for (int i=0; i<64; i++) {
            bob.append("(a ").append(op(">=", "> ", i, 0))
                .append(" ? && a ").append(op("< ", "<=", i, 1))
                .append(" ? && b ").append(op("==", "!=", i, 2))
                .append(" ?) || (a ").append(op(">=", "> ", i, 3))
                .append(" ? && a ").append(op("< ", "<=", i, 4))
                .append(" ? && b ").append(op("==", "!=", i, 5))
                .append(" ?)");

            String filter = bob.toString();
            bob.setLength(0);

            int total = 0;
            for (int j=0; j<10; j++) {
                total += mergedScans(rnd, table, altKey, filter);
            }
            assertTrue(total > 0);
        }
    }

    private int mergedScans(Random rnd, Table<MyRow> table, Table<MyRow> altKey, String filter)
        throws Exception
    {
        var args = new Object[6];
        for (int i=0; i<args.length; i++) {
            args[i] = rnd.nextInt(10);
        }

        var expect = new HashSet<MyRow>();

        {
            var scanner = table.newRowScanner(null, filter, args);
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertTrue(expect.add(row));
            }
        }

        var scanner = (BasicRowScanner<MyRow>) altKey.newRowScanner(null, filter, args);
        var contoller = (MultiScanController<MyRow>) scanner.mController;

        var actual = new HashSet<MyRow>();

        for (var row = scanner.row(); row != null; row = scanner.step()) {
            assertTrue(actual.add(row));
        }

        assertEquals(expect, actual);

        return actual.size();
    }

    private static String op(String a, String b, int i, int shift) {
        return ((i >> shift) & 1) == 0 ? a : b;
    }

    @PrimaryKey("id")
    @AlternateKey({"a", "b"})
    public interface MyRow {
        int id();
        void id(int id);
        int a();
        void a(int a);
        int b();
        void b(int b);
    }
}
