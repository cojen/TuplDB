/*
 *  Copyright (C) 2017 Cojen.org
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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ScannerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ScannerTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.directPageAccess(false);
        config.maxCacheSize(100000000);
        mDb = Database.open(config);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    protected Scanner newScanner(View view, Transaction txn) throws Exception {
        return view.newScanner(txn);
    }

    protected Database mDb;

    @Test
    public void empty() throws Exception {
        Index ix = mDb.openIndex("test");

        Scanner s = newScanner(ix, null);
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());
        s.close();
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());

        s = newScanner(ix, null);
        assertFalse(s.step(0));
        assertFalse(s.step(1));
        try {
            s.step(-1);
            fail();
        } catch (IllegalArgumentException e) {
        }

        s = newScanner(ix.viewReverse(), null);
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());
        s.close();
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());

        s = newScanner(ix, null);
        AtomicInteger count = new AtomicInteger();
        s.scanAll((k, v) -> count.getAndIncrement());
        assertEquals(0, count.get());
        assertNull(s.key());
        assertNull(s.value());
    }

    @Test
    public void oneEntry() throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        Scanner s = newScanner(ix, null);
        fastAssertArrayEquals(key, s.key());
        fastAssertArrayEquals(value, s.value());
        assertFalse(s.step());
        s.close();
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());

        s = newScanner(ix, null);
        assertTrue(s.step(0));
        fastAssertArrayEquals(key, s.key());
        fastAssertArrayEquals(value, s.value());
        assertTrue(s.step(0));
        fastAssertArrayEquals(key, s.key());
        fastAssertArrayEquals(value, s.value());
        assertFalse(s.step(1));
        assertNull(s.key());
        assertNull(s.value());
        try {
            s.step(-1);
            fail();
        } catch (IllegalArgumentException e) {
        }

        s = newScanner(ix.viewReverse(), null);
        fastAssertArrayEquals(key, s.key());
        fastAssertArrayEquals(value, s.value());
        assertFalse(s.step());
        s.close();
        assertNull(s.key());
        assertNull(s.value());
        assertFalse(s.step());

        s = newScanner(ix, null);
        class Observed {
            volatile int count;
            volatile byte[] key;
            volatile byte[] value;
        }
        Observed obs = new Observed();
        AtomicInteger count = new AtomicInteger();
        s.scanAll((k, v) -> {
            obs.count++;
            obs.key = k;
            obs.value = v;
        });
        assertEquals(1, obs.count);
        fastAssertArrayEquals(key, obs.key);
        fastAssertArrayEquals(value, obs.value);
        assertNull(s.key());
        assertNull(s.value());
    }

    @Test
    public void simpleScan() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        ArrayList<SimpleEntry<byte[], byte[]>> list = new ArrayList<>();
        Scanner s = newScanner(ix, null);
        s.scanAll((k, v) -> {
            list.add(new SimpleEntry<>(k, v));
        });

        assertEquals(10, list.size());
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            fastAssertArrayEquals(key, list.get(i).getKey());
            fastAssertArrayEquals(value, list.get(i).getValue());
        }

        // Again, with only the keys.
        list.clear();
        s = newScanner(ix.viewKeys(), null);
        s.scanAll((k, v) -> {
            list.add(new SimpleEntry<>(k, v));
        });

        assertEquals(10, list.size());
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            fastAssertArrayEquals(key, list.get(i).getKey());
            assertEquals(Cursor.NOT_LOADED, list.get(i).getValue());
        }
    }

    @Test
    public void transactional() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        ArrayList<SimpleEntry<byte[], byte[]>> list = new ArrayList<>();
        Transaction txn = mDb.newTransaction();
        assertEquals(LockMode.UPGRADABLE_READ, txn.lockMode());
        Scanner s = newScanner(ix, txn);
        s.scanAll((k, v) -> {
            list.add(new SimpleEntry<>(k, v));
        });

        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            // Only an upgradable lock should be held, so load should work.
            fastAssertArrayEquals(value, ix.load(null, key));
        }

        Transaction txn2 = mDb.newTransaction();
        txn2.lockTimeout(0, null);
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            try {
                ix.store(txn2, key, null);
                fail();
            } catch (LockTimeoutException e) {
                // Expected.
            }
        }

        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            // Lock should have been acquired by original transaction.
            ix.store(txn, key, null);
        }

        assertEquals(0, ix.count(null, null));

        txn.commit();
        txn2.commit();

        assertEquals(0, ix.count(null, null));
    }
}
