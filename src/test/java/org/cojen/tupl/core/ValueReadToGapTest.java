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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ValueReadToGapTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ValueReadToGapTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    private Database mDb;

    @Test
    public void nonFragmented() throws Exception {
        Index ix = mDb.openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] buf = new byte[10];

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            assertEquals(-1, c.valueReadToGap(0, buf, 0, buf.length));
            assertEquals(-1, c.valueSkipGap(0));
        }

        byte[] value = "world".getBytes();
        ix.store(Transaction.BOGUS, key, value);

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(5, amt);
            assertTrue(Arrays.equals(value, 0, 5, buf, 0, 5));
            assertEquals(0, c.valueSkipGap(0));
            assertEquals(0, c.valueSkipGap(100));
        }
    }

    @Test
    public void directPointers() throws Exception {
        Index ix = mDb.openIndex("test");

        byte[] key = "key".getBytes();
        byte[] buf = new byte[50000];

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.valueLength(40960);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(0, amt);
            amt = c.valueReadToGap(20000, buf, 0, buf.length);
            assertEquals(0, amt);

            assertEquals(40960, c.valueSkipGap(0));
            assertEquals(40960 - 1000, c.valueSkipGap(1000));
            assertEquals(0, c.valueSkipGap(40960));
            assertEquals(0, c.valueSkipGap(50000));
        }

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.store("hello".getBytes());
            c.valueLength(50000);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(50000 % 4096, amt); // some content is inlined
            assertTrue(Arrays.equals("hello".getBytes(), 0, 5, buf, 0, 5));
            amt = c.valueReadToGap(20000, buf, 0, buf.length);
            assertEquals(0, amt);

            assertEquals(0, c.valueSkipGap(0));
            assertEquals(0, c.valueSkipGap(3));
            assertEquals(50000 - (50000 % 4096), c.valueSkipGap(50000 % 4096));
            assertEquals(50000 - 1000, c.valueSkipGap(1000));
            assertEquals(50000 - 20000, c.valueSkipGap(20000));
            assertEquals(0, c.valueSkipGap(50000));
        }

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.delete();
            c.valueLength(50000);
            c.valueWrite(25000, "hello".getBytes(), 0, 5);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(50000 % 4096, amt); // some content is inlined
            amt = c.valueReadToGap(24999, buf, 0, buf.length);
            assertEquals(25000 % 4096 + 1, amt);
            assertTrue(Arrays.equals("hello".getBytes(), 0, 5, buf, 1, 6));

            assertEquals(0, c.valueSkipGap(0));
            assertEquals(20328, c.valueSkipGap(1000));
            assertEquals(20328 - 4000, c.valueSkipGap(5000));
            assertEquals(0, c.valueSkipGap(25000));
            assertEquals(20000, c.valueSkipGap(30000));
        }
    }

    @Test
    public void indirectPointers() throws Exception {
        Index ix = mDb.openIndex("test");

        byte[] key = "key".getBytes();
        byte[] buf = new byte[5_000_000];

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.valueLength(100_000_000);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(0, amt);
            amt = c.valueReadToGap(20000, buf, 0, buf.length);
            assertEquals(0, amt);

            assertEquals(100_000_000, c.valueSkipGap(0));
            assertEquals(100_000_000 - 1000, c.valueSkipGap(1000));
            assertEquals(0, c.valueSkipGap(100_000_000));
            assertEquals(0, c.valueSkipGap(100_000_000 + 1000));
        }

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.store("hello".getBytes());
            c.valueLength(100_000_000);
            int amt = c.valueReadToGap(0, buf, 0, 50_000);
            assertEquals(4096, amt);
            amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(4096, amt);
            assertTrue(Arrays.equals("hello".getBytes(), 0, 5, buf, 0, 5));
            amt = c.valueReadToGap(20000, buf, 0, buf.length);
            assertEquals(0, amt);

            assertEquals(0, c.valueSkipGap(0));
            assertEquals(0, c.valueSkipGap(3));
            assertEquals(0, c.valueSkipGap(5));
            assertEquals(0, c.valueSkipGap(100));
            assertEquals(100_000_000 - 4096, c.valueSkipGap(4096));
            assertEquals(50_000_000, c.valueSkipGap(50_000_000));
            assertEquals(0, c.valueSkipGap(100_000_000));
        }

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.delete();
            c.valueLength(100_000_000);
            c.valueWrite(50_000_000, "hello".getBytes(), 0, 5);
            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(0, amt);
            amt = c.valueReadToGap(50_000_000, buf, 0, buf.length);
            assertEquals(4096 - (50_000_000 % 4096), amt);
            assertTrue(Arrays.equals("hello".getBytes(), 0, 5, buf, 0, 5));

            assertEquals((50_000_000 / 4096) * 4096, c.valueSkipGap(0));
            assertEquals(((50_000_000 / 4096) * 4096) - 1000, c.valueSkipGap(1000));
            assertEquals(128, c.valueSkipGap(50_000_000 - 256));
            assertEquals(0, c.valueSkipGap(50_000_000 - 10));
            assertEquals(0, c.valueSkipGap(50_000_000));
            assertEquals(0, c.valueSkipGap(50_000_000 + 5));
            assertEquals(5000, c.valueSkipGap(100_000_000 - 5000));
            assertEquals(4096, c.valueSkipGap(100_000_000 - 4096));
            assertEquals(0, c.valueSkipGap(100_000_000));
            assertEquals(0, c.valueSkipGap(100_000_000 + 100));
        }

        try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.delete();
            c.valueLength(100_000_000);

            var value = new byte[3_000_000];
            for (int i=0; i<value.length; i++) {
                value[i] = (byte) i;
            }

            c.valueWrite(50_000_000, value, 0, value.length);

            int amt = c.valueReadToGap(0, buf, 0, buf.length);
            assertEquals(0, amt);

            amt = c.valueReadToGap(50_000_000, buf, 0, buf.length);
            assertTrue(Arrays.equals(value, 0, value.length, buf, 0, value.length));
            var expect = (4096 * (((value.length + 4095)) / 4096)) - (50_000_000 % 4096);
            assertEquals(expect, amt);

            assertEquals((50_000_000 / 4096) * 4096, c.valueSkipGap(0));
            assertEquals(((50_000_000 / 4096) * 4096) - 1000, c.valueSkipGap(1000));
            assertEquals(0, c.valueSkipGap(51_000_000));
            assertEquals(0, c.valueSkipGap(50_000_000 + value.length));
            assertEquals(40_000_000, c.valueSkipGap(60_000_000));
            assertEquals(5000, c.valueSkipGap(100_000_000 - 5000));
            assertEquals(4096, c.valueSkipGap(100_000_000 - 4096));
            assertEquals(0, c.valueSkipGap(100_000_000));
            assertEquals(0, c.valueSkipGap(100_000_000 + 100));
        }
    }

    @Test
    public void fuzz1() throws Exception {
        fuzz(2_500_000, 1000);
    }

    @Test
    public void fuzz2() throws Exception {
        fuzz(100_000_000, 1000);
    }

    private void fuzz(long range, int num) throws Exception {
        var rnd = new Random(8675309);

        Index ix = mDb.openIndex("test");

        byte[] key = "key".getBytes();

        var buf = new byte[10_000];

        for (int i=0; i<num; i++) {
            try (var c = (BTreeCursor) ix.newCursor(Transaction.BOGUS)) {
                c.find(key);
                c.delete();

                long length = rnd.nextLong(range);
                c.valueLength(length);

                var values = new HashMap<Long, Byte>();

                for (int j=0; j<10; j++) {
                    long pos;
                    byte value;
                    do {
                        pos = rnd.nextLong(length);
                        while ((value = (byte) rnd.nextInt()) == 0);
                    } while (values.containsKey(pos));

                    values.put(pos, value);

                    buf[0] = value;
                    c.valueWrite(pos, buf, 0, 1);
                }

                long pos = 0;

                do {
                    int amt = c.valueReadToGap(pos, buf, 0, buf.length);

                    for (int j=0; j<amt; j++) {
                        byte value = buf[j];
                        if (value != 0) {
                            byte expect = values.remove(pos + j);
                            assertEquals(expect, value);
                        }
                    }

                    pos += amt;

                    long skipped = c.valueSkipGap(pos);
                    pos += skipped;
                } while (pos < length);

                assertTrue(values.isEmpty());
                assertEquals(length, pos);
            }
        }
    }
}
