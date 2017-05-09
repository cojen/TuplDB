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
        mDb = newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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

        mDb = reopenTempDatabase(getClass(), mDb, mConfig);

        assertNull(mDb.findIndex("a"));

        Index ix2 = mDb.findIndex("b");
        assertEquals(ix1.getId(), ix2.getId());
        assertEquals("b", ix2.getNameString());

        fastAssertArrayEquals("world".getBytes(), ix2.load(null, "hello".getBytes()));
    }
}
