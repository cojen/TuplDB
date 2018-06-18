/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SorterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SorterTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointSizeThreshold(0)
            .minCacheSize(10_000_000)
            .maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        mDatabase = TestUtils.newTempDatabase(getClass(), config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    protected Database mDatabase;

    @Test
    public void sortNothing() throws Exception {
        Sorter s = mDatabase.newSorter(null);

        Index ix1 = s.finish();
        assertEquals(0, ix1.count(null, null));

        Index ix2 = s.finish();
        assertEquals(0, ix2.count(null, null));
        assertNotSame(ix1, ix2);

        s.reset();
        Index ix3 = s.finish();
        assertEquals(0, ix3.count(null, null));
        assertNotSame(ix1, ix3);
        assertNotSame(ix2, ix3);

        s.reset();
        s.reset();
        Index ix4 = s.finish();
        assertEquals(0, ix4.count(null, null));
    }

    @Test
    public void sortOne() throws Exception {
        Sorter s = mDatabase.newSorter(null);

        s.add("hello".getBytes(), "world".getBytes());
        s.reset();

        Index ix1 = s.finish();
        assertEquals(0, ix1.count(null, null));

        s.add("hello".getBytes(), "world".getBytes());

        Index ix2 = s.finish();
        assertEquals(1, ix2.count(null, null));
        assertNotSame(ix1, ix2);

        fastAssertArrayEquals("world".getBytes(), ix2.load(null, "hello".getBytes()));

        mDatabase.deleteIndex(ix2);
        assertEquals(0, ix2.count(null, null));

        // dups
        s.add("hello".getBytes(), "world".getBytes());
        s.add("hello".getBytes(), "world!!!".getBytes());
        Index ix3 = s.finish();
        assertEquals(1, ix3.count(null, null));
        assertNotSame(ix2, ix3);
        fastAssertArrayEquals("world!!!".getBytes(), ix3.load(null, "hello".getBytes()));
    }

    @Test
    public void sortMany() throws Exception {
        // count = 1_000_000, range = 2_000_000
        sortMany(1_000_000, 2_000_000, null);
    }

    @Test
    public void sortManyMore() throws Exception {
        // count = 10_000_000, range = 2_000_000_000 (fewer duplicates)
        sortMany(10_000_000, 2_000_000_000, null);
    }

    @Test
    public void sortRecycle() throws Exception {
        // count = 2000, range = 10_000
        Sorter s = sortMany(2_000, 10_000, null);
        // count = 10_000, range = 100_000
        s = sortMany(10_000, 100_000, s);
        // count = 1_000_000, range = 2_000_000
        sortMany(1_000_000, 2_000_000, s);
    }

    /**
     * @param s non-null to use recycled instance
     */
    private Sorter sortMany(int count, int range, Sorter s) throws Exception {
        final long seed = 123 + count + range;
        Random rnd = new Random(seed);

        if (s == null) {
            s = mDatabase.newSorter(null);
        }

        for (int i=0; i<count; i++) {
            byte[] key = String.valueOf(rnd.nextInt(range)).getBytes();
            byte[] value = ("value-" + i).getBytes();
            s.add(key, value);
        }

        Index ix = s.finish();

        long actualCount = ix.count(null, null);
        assertTrue(actualCount <= count);

        // Verify entries.

        rnd = new Random(seed);
        TreeMap<byte[], byte[]> expected = new TreeMap<>(KeyComparator.THE);

        for (int i=0; i<count; i++) {
            byte[] key = String.valueOf(rnd.nextInt(range)).getBytes();
            byte[] value = ("value-" + i).getBytes();
            expected.put(key, value);
        }

        assertEquals(expected.size(), actualCount);

        Iterator<Map.Entry<byte[], byte[]>> it = expected.entrySet().iterator();
        try (Cursor c = ix.newCursor(null)) {
            c.first();
            while (it.hasNext()) {
                Map.Entry<byte[], byte[]> e = it.next();
                fastAssertArrayEquals(e.getKey(), c.key());
                fastAssertArrayEquals(e.getValue(), c.value());
                c.next();
            }
            assertNull(c.key());
        }

        return s;
    }

    @Test
    public void largeKeysAndValues() throws Exception {
        final int count = 5000;
        final long seed = 394508;
        Random rnd = new Random(seed);

        Sorter s = mDatabase.newSorter(null);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 8000);
            byte[] value = randomStr(rnd, 100, 100_000);
            s.add(key, value);
        }

        Index ix = s.finish();

        assertEquals(count, ix.count(null, null));

        rnd = new Random(seed);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 8000);
            byte[] value = randomStr(rnd, 100, 100_000);
            fastAssertArrayEquals(value, ix.load(null, key));
        }
    }

    @Test
    public void scanFew() throws Exception {
        scan(10, false, false);
        scan(10, true, false); // reverse
    }

    @Test
    public void scanFewCloseEarly() throws Exception {
        scan(10, false, true);
        scan(10, true, true); // reverse
    }

    @Test
    public void scanMany() throws Exception {
        scan(1_000_000, false, false);
        scan(1_000_000, true, false); // reverse
    }

    @Test
    public void scanManyCloseEarly() throws Exception {
        scan(1_000_000, false, true);
        scan(1_000_000, true, true); // reverse
    }

    private void scan(int count, boolean reverse, boolean close) throws Exception {
        final long seed = 123 + count;
        Random rnd = new Random(seed);

        Sorter s = mDatabase.newSorter(null);

        TreeMap<byte[], byte[]> expected = new TreeMap<>(KeyComparator.THE);

        for (int i=0; i<count; i++) {
            byte[] key = String.valueOf(rnd.nextLong()).getBytes();
            byte[] value = ("value-" + i).getBytes();
            s.add(key, value);
            expected.put(key, value);
        }

        Scanner scanner = reverse ? s.finishScanReverse() : s.finishScan();

        if (close) {
            scanner.close();
        }

        byte[][] prevRef = new byte[1][];

        scanner.scanAll((k, v) -> {
            fastAssertArrayEquals(v, expected.get(k));
            expected.remove(k);

            byte[] prev = prevRef[0];
            if (prev != null) {
                assertTrue(scanner.getComparator().compare(prev, k) < 0);

                int cmp = KeyComparator.THE.compare(prev, k);
                assertTrue(reverse ? cmp > 0 : cmp < 0);
            }

            prevRef[0] = k;
        });

        if (!close) {
            assertTrue(expected.isEmpty());
        }
    }
}
