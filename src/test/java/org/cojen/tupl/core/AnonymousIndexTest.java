/*
 *  Copyright (C) 2021 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class AnonymousIndexTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AnonymousIndexTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mConfig = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        mDb = (CoreDatabase) newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected CoreDatabase mDb;

    @Test
    public void openClose() throws Exception {
        long id0 = mDb.createAnonymousIndex(null, id_ -> {});
        long id1 = mDb.createAnonymousIndex(null, id_ -> {});
        assertNotEquals(0, id0);
        assertNotEquals(0, id1);
        assertNotEquals(id0, id1);

        Index ix1 = mDb.indexById(id0);
        Index ix2 = mDb.indexById(id1);

        assertEquals(null, ix1.name());
        assertEquals(null, ix1.nameString());
        assertEquals(null, ix2.name());
        assertEquals(null, ix2.nameString());
        assertEquals(id0, ix1.id());
        assertEquals(id1, ix2.id());

        ix1.store(null, "k1".getBytes(), "v1".getBytes());
        ix2.store(null, "k2".getBytes(), "v2".getBytes());

        ix1.close();
        ix2.close();

        ix1 = mDb.indexById(id0);
        ix2 = mDb.indexById(id1);

        fastAssertArrayEquals("v1".getBytes(), ix1.load(null, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), ix2.load(null, "k2".getBytes()));

        try {
            mDb.renameIndex(ix1, "newName".getBytes());
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("anonymous") >= 0);
        }
    }

    @Test
    public void reopen() throws Exception {
        Index dir = mDb.openIndex("directory");
        byte[] idKey = new byte[8];

        Transaction txn = mDb.newTransaction();

        long id = mDb.createAnonymousIndex(txn, id_ -> {
            try {
                Utils.encodeLongBE(idKey, 0, id_);
                dir.store(txn, idKey, Utils.EMPTY_BYTES);
            } catch (Exception e) {
                Utils.rethrow(e);
            }
        });

        Index anon = mDb.indexById(id);
        anon.store(null, "hello".getBytes(), "world".getBytes());

        mDb = (CoreDatabase) reopenTempDatabase(getClass(), mDb, mConfig);

        Index dir2 = mDb.openIndex("directory");

        try (Cursor c = dir2.newCursor(null)) {
            c.first();
            fastAssertArrayEquals(idKey, c.key());
        }

        anon = mDb.indexById(id);
        fastAssertArrayEquals("world".getBytes(), anon.load(null, "hello".getBytes()));

        id = mDb.createAnonymousIndex(null, id_ -> {});
        assertNotEquals(anon.id(), id);

        mDb.deleteIndex(anon).run();

        assertNull(mDb.indexById(anon.id()));
    }

    @Test
    public void hidden() throws Exception {
        // Anonymous index isn't visible from the public registry.

        long id = mDb.createAnonymousIndex(null, id_ -> {});
        Index ix = mDb.indexById(id);

        assertTrue(mDb.indexRegistryByName().isEmpty());
        assertTrue(mDb.indexRegistryById().isEmpty());
    }
}
