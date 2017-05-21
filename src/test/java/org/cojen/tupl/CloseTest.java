/*
 *  Copyright (C) 2011-2017 Cojen.org
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

import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CloseTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CloseTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void basic() throws Exception {
        Index ix = mDb.openIndex("basic");
        ix.close();
        ix.close(); // harmless double close

        Index ix2 = mDb.openIndex("basic");
        assertTrue(ix != ix2);

        try {
            ix.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (ClosedIndexException e) {
            // expected -- index is closed
        }

        Cursor c = ix.newCursor(null);
        c.first();
        assertNull(c.key());
        try {
            c.next();
            fail();
        } catch (UnpositionedCursorException e) {
            // expected -- cursor is unpositioned
        }

        c.find("nothing".getBytes());
        fastAssertArrayEquals("nothing".getBytes(), c.key());
        assertNull(c.value());
        c.load();
        try {
            c.store("something".getBytes());
            fail();
        } catch (ClosedIndexException e) {
            // expected -- index is closed
        }
        c.reset();

        ix2.store(null, "hello".getBytes(), "world".getBytes());

        c = ix2.newCursor(null);
        c.first();
        fastAssertArrayEquals("hello".getBytes(), c.key());
        fastAssertArrayEquals("world".getBytes(), c.value());

        ix2.close();

        fastAssertArrayEquals("hello".getBytes(), c.key());
        fastAssertArrayEquals("world".getBytes(), c.value());

        try {
            c.store("value".getBytes());
            fail();
        } catch (ClosedIndexException e) {
            // expected -- index is closed
        }

        fastAssertArrayEquals("hello".getBytes(), c.key());
        fastAssertArrayEquals("world".getBytes(), c.value());

        c.next();
        assertNull(c.key());
        assertNull(c.value());

        c.reset();
        ix2.close();

        Index ix3 = mDb.openIndex("basic");
        assertTrue(ix2 != ix3);

        fastAssertArrayEquals("world".getBytes(), ix3.load(null, "hello".getBytes()));
    }

    @Test
    public void txn() throws Exception {
        Index ix = mDb.openIndex("txn");

        ix.store(null, "hello".getBytes(), "world".getBytes());

        Transaction txn = mDb.newTransaction();
        ix.store(txn, "hello".getBytes(), "everyone".getBytes());
        ix.close();
        // Rollback should succeed even after index is closed. Index will get
        // re-opened as a side-effect.
        txn.exit();

        ix = mDb.openIndex("txn");

        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));

        txn = mDb.newTransaction();
        ix.store(txn, "hello".getBytes(), "everyone".getBytes());
        ix.close();
        // Commit should succeed even after index is closed.
        txn.commit();

        ix = mDb.openIndex("txn");

        fastAssertArrayEquals("everyone".getBytes(), ix.load(null, "hello".getBytes()));
    }

    @Test
    public void ghost() throws Exception {
        Index ix = mDb.openIndex("ghost");

        ix.store(null, "hello".getBytes(), "world".getBytes());

        Transaction txn = mDb.newTransaction();
        ix.store(txn, "hello".getBytes(), null);
        ix.close();
        // Should be able to delete the ghost record after original index has
        // been closed.
        txn.commit();
        
        ix = mDb.openIndex("ghost");
        assertNull(ix.load(null, "hello".getBytes()));
    }

    @Test
    public void cacheChurn() throws Exception {
        // Open and close a bunch of indexes, demonstrating that closed root
        // node pages get recycled into the cache.

        // Prevent garbage collection.
        ArrayList<Index> indexes = new ArrayList<Index>();

        for (int i=0; i<10000; i++) {
            Index ix = mDb.openIndex("ix-" + i);
            indexes.add(ix);
            ix.store(null, "hello".getBytes(), ("world-" + i).getBytes());
            ix.close();
        }

        try {
            for (int i=0; i<10000; i++) {
                Index ix = mDb.openIndex("ix-" + i);
                indexes.add(ix);
                fastAssertArrayEquals(("world-" + i).getBytes(),
                                      ix.load(null, "hello".getBytes()));
            }
            if (expectCacheExhausted()) {
                // Cache size is too large for the test.
                fail();
            }
        } catch (CacheExhaustedException e) {
            if (expectCacheExhausted()) {
                // expected
            }
        }

        for (int i=0; i<10000; i++) {
            Index ix = mDb.openIndex("ix-" + i);
            indexes.add(ix);
            fastAssertArrayEquals(("world-" + i).getBytes(),
                                  ix.load(null, "hello".getBytes()));
            ix.close();
        }
    }

    protected boolean expectCacheExhausted() {
        return true;
    }

    @Test
    public void drop() throws Exception {
        Index ix = mDb.openIndex("drop");
        long id = ix.getId();

        assertEquals(ix, mDb.findIndex("drop"));
        assertEquals(ix, mDb.indexById(id));

        ix.drop();

        assertNull(mDb.findIndex("drop"));
        assertNull(mDb.indexById(id));

        ix.store(null, "hello".getBytes(), null);

        try {
            ix.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (ClosedIndexException e) {
        }

        ix = mDb.openIndex("drop");
        long id2 = ix.getId();

        assertFalse(id == id2);

        ix.store(null, "hello".getBytes(), "world".getBytes());

        try {
            ix.drop();
            fail();
        } catch (IllegalStateException e) {
            // Non-empty index.
        }

        Transaction txn = mDb.newTransaction();
        ix.store(txn, "hello".getBytes(), null);

        try {
            ix.drop();
            fail();
        } catch (IllegalStateException e) {
            // Non-empty index.
        }

        txn.reset();

        try {
            ix.drop();
            fail();
        } catch (IllegalStateException e) {
            // Non-empty index.
        }

        Cursor c = ix.newCursor(null);
        c.first();
        assertArrayEquals("hello".getBytes(), c.key());

        ix.store(null, "hello".getBytes(), null);

        ix.drop();

        c.load();
        assertArrayEquals("hello".getBytes(), c.key());
        assertNull(c.value());
        c.reset();

        try {
            ix.drop();
            fail();
        } catch (ClosedIndexException e) {
        }

        assertNull(mDb.findIndex("drop"));
        assertNull(mDb.indexById(id2));
    }

    @Test
    public void sneakyDrop() throws Exception {
        Index ix = mDb.openIndex("drop");

        Transaction txn = mDb.newTransaction();
        assertTrue(ix.insert(txn, "hello".getBytes(), "world".getBytes()));

        try {
            ix.drop();
            fail();
        } catch (IllegalStateException e) {
        }

        // Delete the entry, bypassing the transaction, and then drop the index.
        assertTrue(ix.delete(Transaction.BOGUS, "hello".getBytes()));
        ix.drop();

        // Rollback will attempt to access missing index, but nothing should happen.
        txn.reset();

        assertNull(mDb.findIndex("drop"));
    }

    @Test
    public void delete() throws Exception {
        Index ix = mDb.openIndex("delete");
        long id = ix.getId();

        ix.store(null, "hello".getBytes(), "world".getBytes());

        Runnable task = mDb.deleteIndex(ix);

        assertNull(mDb.findIndex("delete"));
        Index ix2 = mDb.openIndex("delete");
        assertFalse(ix == ix2);
        assertFalse(id == ix2.getId());
        // New index should be empty, and so drop should work.
        ix2.drop();

        try {
            mDb.deleteIndex(ix2);
            fail();
        } catch (ClosedIndexException e) {
        }

        try {
            ix.store(null, "hello".getBytes(), "world!".getBytes());
            fail();
        } catch (ClosedIndexException e) {
        }

        task.run();

        // Second run should do nothing.
        task.run();
    }

    @Test
    public void deleteMany() throws Exception {
        mDb.suspendCheckpoints();
        Index ix = mDb.openIndex("delete");

        for (int i=0; i<10000; i++) {
            ix.store(null, ("hello-" + i).getBytes(), ("world-" + i).getBytes());
        }

        Runnable task = mDb.deleteIndex(ix);

        Database.Stats stats1 = mDb.stats();

        task.run();

        Database.Stats stats2 = mDb.stats();

        assertEquals(0, stats1.freePages());
        assertTrue(60 <= stats2.freePages() && stats2.freePages() <= 70);
    }

    @Test
    public void findNearby() throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=0; i<1000; i++) {
            ix.store(null, ("hello-" + i).getBytes(), ("world-" + i).getBytes());
        }

        Cursor c = ix.newCursor(null);
        c.findNearby("hello-2".getBytes());
        assertTrue(c.value() != null);

        ix.close();

        c.findNearby("hello-3".getBytes());
        assertTrue(c.value() == null);

        try {
            c.store("stuff".getBytes());
            fail();
        } catch (ClosedIndexException e) {
        }
    }
}
