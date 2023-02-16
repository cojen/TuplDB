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

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteSorterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteSorterTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mClientDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            mClientDb.close();
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());
    }

    private Database mServerDb;
    private Database mClientDb;

    @Test
    public void basic() throws Exception {
        Sorter sorter = mClientDb.newSorter();

        sorter.add("keyx".getBytes(), "valuex".getBytes());
        sorter.reset();

        byte[] k1 = "hello1".getBytes();
        byte[] v1 = "world1".getBytes();

        sorter.add(k1, v1);

        assertEquals(0, sorter.progress());

        byte[][] kvPairs = {
            "hello0".getBytes(), "world0".getBytes(),
            "hello2".getBytes(), "world2".getBytes(),
        };

        sorter.addBatch(kvPairs, 0, 2);

        Index ix = sorter.finish();

        assertEquals(3, ix.count(null, null));

        try (Cursor c = ix.newCursor(null)) {
            int i = 0;
            for (c.first(); c.key() != null; i++, c.next()) {
                assertEquals("hello" + i, new String(c.key()));
                assertEquals("world" + i, new String(c.value()));
            }
        }

        mClientDb.deleteIndex(ix);

        sorter.add("key".getBytes(), "value".getBytes());
        ix = sorter.finish();
        assertEquals(1, ix.count(null, null));
        assertArrayEquals("value".getBytes(), ix.load(null, "key".getBytes()));
    }

    @Test
    public void addAll() throws Exception {
        Index ix = mClientDb.openIndex("test");

        var rnd = new Random(9035768);

        for (int i=0; i<1000; i++) {
            byte[] key = new byte[10];
            rnd.nextBytes(key);
            byte[] value = new byte[10];
            rnd.nextBytes(value);
            ix.store(null, key, value);
        }

        Sorter sorter = mClientDb.newSorter();

        Table<Entry> table = ix.asTable(Entry.class);
        sorter.addAll(table.newScanner(null, "{+value, *}"));

        Index sorted = sorter.finish();

        assertEquals(1000, ix.count(null, null));

        try (Cursor c = ix.newCursor(null)) {
            byte[] last = null;
            for (c.first(); c.key() != null; c.next()) {
                assertFalse(Arrays.equals(c.key(), c.value()));
                if (last != null) {
                    assertTrue(Arrays.compareUnsigned(last, c.key()) < 0);
                }
                last = c.key();
            }
        }

        sorter.reset();
    }

    @Test
    public void finishScan() throws Exception {
        finishScan(false);
    }

    @Test
    public void finishScanReverse() throws Exception {
        finishScan(true);
    }

    private void finishScan(boolean reverse) throws Exception {
        Index ix = mClientDb.openIndex("test");

        var rnd = new Random(9035768);

        for (int i=0; i<1000; i++) {
            byte[] key = new byte[10];
            rnd.nextBytes(key);
            byte[] value = new byte[10];
            rnd.nextBytes(value);
            ix.store(null, key, value);
        }

        Sorter sorter = mClientDb.newSorter();

        Table<Entry> table = ix.asTable(Entry.class);
        sorter.addAll(table.newScanner(null, "{+value, *}"));

        try (Scanner<Entry> sorted = reverse ? sorter.finishScanReverse() : sorter.finishScan()) {
            byte[] last = null;
            for (Entry e = sorted.row(); e != null; e = sorted.step(e)) {
                assertFalse(Arrays.equals(e.key(), e.value()));
                if (last != null) {
                    int cmp = Arrays.compareUnsigned(last, e.key());
                    assertTrue(reverse ? cmp > 0 : cmp < 0);
                }
                last = e.key();
            }
        }
    }

    @Test
    public void addAllFinishScan() throws Exception {
        addAllFinishScan(false);
    }

    @Test
    public void addAllFinishScanReverse() throws Exception {
        addAllFinishScan(true);
    }

    private void addAllFinishScan(boolean reverse) throws Exception {
        Index ix = mClientDb.openIndex("test");

        var rnd = new Random(9035768);

        for (int i=0; i<1000; i++) {
            byte[] key = new byte[10];
            rnd.nextBytes(key);
            byte[] value = new byte[10];
            rnd.nextBytes(value);
            ix.store(null, key, value);
        }

        Sorter sorter = mClientDb.newSorter();

        Table<Entry> table = ix.asTable(Entry.class);
        var srcScanner = table.newScanner(null, "{+value, *}");

        Scanner<Entry> sorted;
        if (reverse) {
            sorted = sorter.finishScanReverse(srcScanner);
        } else {
            sorted = sorter.finishScan(srcScanner);
        }

        try (sorted) {
            byte[] last = null;
            for (Entry e = sorted.row(); e != null; e = sorted.step(e)) {
                assertFalse(Arrays.equals(e.key(), e.value()));
                if (last != null) {
                    int cmp = Arrays.compareUnsigned(last, e.key());
                    assertTrue(reverse ? cmp > 0 : cmp < 0);
                }
                last = e.key();
            }
        }
    }
}
