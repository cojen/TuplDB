/*
 *  Copyright 2012-2013 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
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
        } catch (IllegalStateException e) {
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

        try {
            ix2.close();
            fail();
        } catch (IllegalStateException e) {
            // expected -- active cursor
        }

        fastAssertArrayEquals("hello".getBytes(), c.key());
        fastAssertArrayEquals("world".getBytes(), c.value());

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
            // Cache size is too large for the test.
            fail();
        } catch (CacheExhaustedException e) {
            // expected
        }

        for (int i=0; i<10000; i++) {
            Index ix = mDb.openIndex("ix-" + i);
            indexes.add(ix);
            fastAssertArrayEquals(("world-" + i).getBytes(),
                                  ix.load(null, "hello".getBytes()));
            ix.close();
        }
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

        try {
            ix.drop();
            fail();
        } catch (IllegalStateException e) {
            // Active cursor.
        }

        c.load();
        assertNull(c.value());
        c.reset();

        ix.drop();

        assertNull(mDb.findIndex("drop"));
        assertNull(mDb.indexById(id2));
    }
}
