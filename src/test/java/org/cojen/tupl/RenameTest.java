/*
 *  Copyright 2013-2015 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RenameTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RenameTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .directPageAccess(false).durabilityMode(DurabilityMode.NO_FLUSH);
        mDb = newTempDatabase(mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected Database mDb;

    @Test
    public void nameConflict() throws Exception {
        Index ix1 = mDb.openIndex("a");
        Index ix2 = mDb.openIndex("b");
        try {
            mDb.renameIndex(ix1, "b");
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void closedIndex() throws Exception {
        Index ix1 = mDb.openIndex("a");
        ix1.close();
        try {
            mDb.renameIndex(ix1, "b");
            fail();
        } catch (ClosedIndexException e) {
        }
    }

    @Test
    public void noChange() throws Exception {
        Index ix1 = mDb.openIndex("a");
        mDb.renameIndex(ix1, "a");
        assertEquals("a", ix1.getNameString());
        ix1.close();
        Index ix2 = mDb.openIndex("a");
        assertEquals(ix1.getId(), ix2.getId());
    }

    @Test
    public void rename() throws Exception {
        Index ix1 = mDb.openIndex("a");
        Index ix2 = mDb.openIndex("b");
        final long id = ix1.getId();
        mDb.renameIndex(ix1, "c");
        assertEquals("c", ix1.getNameString());
        assertEquals(id, ix1.getId());
        assertNull(mDb.findIndex("a"));
        ix1.close();
        Index ix3 = mDb.openIndex("a");
        assertFalse(id == ix3.getId());
        ix1 = mDb.openIndex("c");
        assertEquals(id, ix1.getId());
        assertEquals(ix1, mDb.indexById(id));
    }

    @Test
    public void renameOpen() throws Exception {
        Index ix1 = mDb.openIndex("a");
        ix1.store(null, "hello".getBytes(), "world".getBytes());
        mDb.renameIndex(ix1, "b");

        mDb = reopenTempDatabase(mDb, mConfig);

        assertNull(mDb.findIndex("a"));

        Index ix2 = mDb.findIndex("b");
        assertEquals(ix1.getId(), ix2.getId());
        assertEquals("b", ix2.getNameString());

        fastAssertArrayEquals("world".getBytes(), ix2.load(null, "hello".getBytes()));
    }
}
