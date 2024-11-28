/*
 *  Copyright (C) 2017-2022 Cojen.org
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

import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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
        var config = new DatabaseConfig();
        config.maxCacheSize(100_000_000);
        mDb = Database.open(config);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    protected Scanner<Entry> newScanner(Index ix, Transaction txn) throws Exception {
        return ix.asTable(Entry.class).newScanner(txn);
    }

    protected Database mDb;

    @Test
    public void empty() throws Exception {
        Index ix = mDb.openIndex("test");

        Scanner<Entry> s = newScanner(ix, null);
        assertNull(s.row());
        assertNull(s.step());
        s.close();
        assertNull(s.row());
        assertNull(s.step());

        s = newScanner(ix, null);
        var count = new AtomicInteger();
        s.forEachRemaining(r -> count.getAndIncrement());
        assertEquals(0, count.get());
        assertNull(s.row());
    }

    @Test
    public void oneEntry() throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        Scanner<Entry> s = newScanner(ix, null);
        fastAssertArrayEquals(key, s.row().key());
        fastAssertArrayEquals(value, s.row().value());
        assertNull(s.step());
        s.close();
        assertNull(s.row());
        assertNull(s.step());

        s = newScanner(ix, null);
        fastAssertArrayEquals(key, s.row().key());
        fastAssertArrayEquals(value, s.row().value());
        assertNull(s.step());
        assertNull(s.row());

        s = newScanner(ix, null);
        var obs = new Object() {
            volatile int count;
            volatile byte[] key;
            volatile byte[] value;
        };
        var count = new AtomicInteger();
        s.forEachRemaining(r -> {
            obs.count++;
            obs.key = r.key();
            obs.value = r.value();
        });
        assertEquals(1, obs.count);
        fastAssertArrayEquals(key, obs.key);
        fastAssertArrayEquals(value, obs.value);
        assertNull(s.row());
    }

    @Test
    public void simpleScan() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        var list = new ArrayList<Entry>();
        Scanner<Entry> s = newScanner(ix, null);
        s.forEachRemaining(list::add);

        assertEquals(10, list.size());
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            fastAssertArrayEquals(key, list.get(i).key());
            fastAssertArrayEquals(value, list.get(i).value());
        }

        // Again, with only the keys.
        list.clear();
        s = ix.asTable(Entry.class).newScanner(null, "{key}");
        s.forEachRemaining(list::add);

        assertEquals(10, list.size());
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            fastAssertArrayEquals(key, list.get(i).key());
            try {
                list.get(i).value();
                fail();
            } catch (UnsetColumnException e) {
            }
        }
    }

    @Test
    public void transactional() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        var list = new ArrayList<Entry>();
        Transaction txn = mDb.newTransaction();
        assertEquals(LockMode.UPGRADABLE_READ, txn.lockMode());
        Scanner<Entry> s = newScanner(ix, txn);
        s.forEachRemaining(list::add);

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
