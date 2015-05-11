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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests that database still functions after cache exhaustion.
 *
 * @author Brian S O'Neill
 */
public class SmallCacheTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SmallCacheTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        // Cache size is 100,000 bytes.
        mDb = newTempDatabase(100000);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected Database mDb;

    /*
       More cases to consider:

     - Node.splitInternal
     - Node.finishSplitRoot
     - UndoLog.persistReady
     - UndoLog.doPush
     - UndoLog.readUndoLogNode
     - FragmentCache.LHT.get
     - Database.fragment
     - Database.writeMultilevelFragments
     - Database.removeInode

    */

    @Test
    public void loadTreeRoot() throws Exception {
        // Tests Database.loadTreeRoot, which allocates a root node.

        List<Index> indexes = new ArrayList<Index>();

        int i = 0;
        try {
            for (; i<100; i++) {
                indexes.add(mDb.openIndex("ix_" + i));
            }
            fail();
        } catch (CacheExhaustedException e) {
        }

        for (Index ix : indexes) {
            ix.close();
        }

        // Verify that indexes can be opened and created.
        assertNotNull(mDb.findIndex("ix_0"));
        assertNotNull(mDb.openIndex("another"));

        // Verify that index which failed to be created earler does not exist.
        assertNull(mDb.findIndex("ix_" + i));
        // ...and that it can still be created.
        assertNotNull(mDb.openIndex("ix_" + i));
    }

    @Test
    public void loadChild() throws Exception { 
        // Tests Node.loadChild, which always allocates a node.

        Index ix = mDb.openIndex("test");
        for (int i=0; i<10000; i++) {
            ix.insert(Transaction.BOGUS, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        // Allocate a bunch of unevictable nodes.
        List<Index> indexes = fillCacheWithIndexes();
        Cursor c1 = ix.newCursor(Transaction.BOGUS);
        c1.first();
        Cursor c2 = ix.newCursor(Transaction.BOGUS);
        c2.last();

        // Verify cache failure and recovery with Node.subSearch.

        try {
            for (int i=0; i<10000; i++) {
                ix.load(null, ("key-" + i).getBytes());
            }
            fail();
        } catch (CacheExhaustedException e) {
        }

        // Free up space and try again.
        c1.reset();
        c2.reset();

        for (int i=0; i<10000; i++) {
            ix.load(null, ("key-" + i).getBytes());
        }

        // Verify cache failure and recovery with TreeCursor.latchChild.

        c1.first();
        c2.last();

        try {
            Cursor c3 = ix.newCursor(Transaction.BOGUS);
            for (c3.first(); c3.key() != null; c3.next()) {
            }
            fail();
        } catch (CacheExhaustedException e) {
        }

        // Free up space and try again.
        c1.reset();
        c2.reset();

        Cursor c3 = ix.newCursor(Transaction.BOGUS);
        for (c3.first(); c3.key() != null; c3.next()) {
        }

        // Verify cache failure and recovery with cursor search.

        c1.first();
        c2.last();

        try {
            c3 = ix.newCursor(Transaction.BOGUS);
            for (int i=0; i<10000; i++) {
                c3.random(null, null);
            }
            fail();
        } catch (CacheExhaustedException e) {
        }

        // Free up space and try again.
        c1.reset();
        c2.reset();

        c3 = ix.newCursor(Transaction.BOGUS);
        for (int i=0; i<10000; i++) {
            c3.random(null, null);
        }
    }

    @Test
    public void splitLeafAndCreateEntry() throws Exception { 
        // Tests Node.splitLeafAndCreateEntry, which always allocates a node.

        Index ix = mDb.openIndex("test");
        for (int i=0; i<10000; i++) {
            ix.insert(Transaction.BOGUS, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        // Allocate a bunch of unevictable nodes.
        List<Index> indexes = fillCacheWithIndexes();
        Cursor c1 = ix.newCursor(Transaction.BOGUS);
        c1.first();

        Cursor c2 = ix.newCursor(Transaction.BOGUS);
        c2.last();

        try {
            c2.store(new byte[3000]); // big value forces a split
            fail();
        } catch (CacheExhaustedException e) {
        }

        // Free up space and try again.
        c1.reset();
        for (Index i : indexes) {
            if (i != ix) {
                i.close();
            }
        }

        c2.last();
        c2.store(new byte[3000]); // big value forces a split
    }

    private List<Index> fillCacheWithIndexes() throws Exception {
        List<Index> indexes = new ArrayList<Index>();
        try {
            for (int i=0; i<100; i++) {
                indexes.add(mDb.openIndex("ix_" + i));
            }
            fail();
        } catch (CacheExhaustedException e) {
        }
        return indexes;
    }
}
