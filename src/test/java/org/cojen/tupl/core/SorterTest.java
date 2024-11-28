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

package org.cojen.tupl.core;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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
        var config = new DatabaseConfig()
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
        Sorter s = mDatabase.newSorter();

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
        Sorter s = mDatabase.newSorter();

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

        try {
            ix2.count(null, null);
            fail();
        } catch (ClosedIndexException e) {
        }

        // dups
        s.add("hello".getBytes(), "world".getBytes());
        s.add("hello".getBytes(), "world!!!".getBytes());
        Index ix3 = s.finish();
        assertEquals(1, ix3.count(null, null));
        assertNotSame(ix2, ix3);
        fastAssertArrayEquals("world!!!".getBytes(), ix3.load(null, "hello".getBytes()));
    }

    @Test
    public void noSortTrees() throws Exception {
        // Tests special cases where no sort trees need to be merged. Counts were determined
        // experimentally.
        sortMany(12232, 10_000_000, null, false); // numLevelTrees == 1
        sortMany(23955, 10_000_000, null, false); // numLevelTrees == 2
    }

    @Test
    public void sortMany() throws Exception {
        // count = 1_000_000, range = 2_000_000
        sortMany(1_000_000, 2_000_000, null, false);
    }

    @Test
    public void sortManyMore() throws Exception {
        // count = 10_000_000, range = 2_000_000_000 (fewer duplicates)
        sortMany(10_000_000, 2_000_000_000, null, false);
    }

    @Test
    public void sortRecycle() throws Exception {
        // count = 2000, range = 10_000
        Sorter s = sortMany(2_000, 10_000, null, false);
        // count = 10_000, range = 100_000
        s = sortMany(10_000, 100_000, s, false);
        // count = 1_000_000, range = 2_000_000
        sortMany(1_000_000, 2_000_000, s, false);
    }

    @Test
    public void sortBatch() throws Exception {
        // count = 1_000_000, range = 2_000_000
        sortMany(1_000_000, 2_000_000, null, true);
    }

    /**
     * @param s non-null to use recycled instance
     */
    private Sorter sortMany(int count, int range, Sorter s, boolean batch) throws Exception {
        final long seed = 123 + count + range;
        var rnd = new Random(seed);

        if (s == null) {
            s = mDatabase.newSorter();
        }

        byte[][] kvPairs = null;
        int kvOffset = 0;
        if (batch) {
            kvPairs = new byte[1000][];
        }

        for (int i=0; i<count; i++) {
            byte[] key = String.valueOf(rnd.nextInt(range)).getBytes();
            byte[] value = ("value-" + i).getBytes();
            if (kvPairs == null) {
                s.add(key, value);
            } else {
                kvPairs[kvOffset++] = key;
                kvPairs[kvOffset++] = value;
                if (kvOffset >= kvPairs.length) {
                    s.addBatch(kvPairs, 0, kvPairs.length / 2);
                    kvOffset = 0;
                }
            }
        }

        if (kvPairs != null) {
            s.addBatch(kvPairs, 0, kvOffset / 2);
        }

        Index ix = s.finish();

        long actualCount = ix.count(null, null);
        assertTrue(actualCount <= count);

        // Verify entries.

        rnd = new Random(seed);
        var expected = new TreeMap<byte[], byte[]>(Utils.KEY_COMPARATOR);

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
        var rnd = new Random(seed);

        Sorter s = mDatabase.newSorter();

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
        var rnd = new Random(seed);

        Sorter s = mDatabase.newSorter();

        var expected = new TreeMap<byte[], byte[]>(Utils.KEY_COMPARATOR);

        for (int i=0; i<count; i++) {
            byte[] key = String.valueOf(rnd.nextLong()).getBytes();
            byte[] value = ("value-" + i).getBytes();
            s.add(key, value);
            expected.put(key, value);
        }

        Scanner<Entry> scanner = reverse ? s.finishScanReverse() : s.finishScan();

        if (close) {
            scanner.close();
        }

        Entry prev = null;

        for (Entry e = scanner.row(); e != null; e = scanner.step()) {
            fastAssertArrayEquals(e.value(), expected.get(e.key()));
            expected.remove(e.key());

            if (prev != null) {
                assertTrue(scanner.getComparator().compare(prev, e) < 0);

                int cmp = Utils.KEY_COMPARATOR.compare(prev.key(), e.key());
                assertTrue(reverse ? cmp > 0 : cmp < 0);
            }

            prev = e;
        }

        if (!close) {
            assertTrue(expected.isEmpty());
        }

        scanner.close();
    }

    @Test
    public void sortScanner() throws Exception {
        // Fill an index with random entries, scan it in value order, and then sort it using a
        // background-sorting scanner.

        var rnd = new Random(928451);
        Index ix = mDatabase.openIndex("test");

        for (int i=0; i<100_000; i++) {
            byte[] key = randomStr(rnd, 10);
            byte[] value = randomStr(rnd, 10);
            ix.store(null, key, value);
        }

        Scanner<Entry> byValue = ix.asTable(Entry.class).newScanner(null, "{+value, key}");

        Scanner<Entry> result = mDatabase.newSorter().finishScan(byValue);

        checkResults(ix.asTable(Entry.class).newScanner(null), result);
    }

    private void checkResults(Scanner<Entry> expect, Scanner<Entry> result) throws Exception {
        while (true) {
            Entry e1 = expect.row();
            Entry e2 = result.row();
            fastAssertArrayEquals(e1.key(), e2.key());
            fastAssertArrayEquals(e1.value(), e2.value());
            if (expect.step() == null) {
                assertNull(result.step());
                break;
            } else {
                assertNotNull(result.step());
            }
        }
    }
}
