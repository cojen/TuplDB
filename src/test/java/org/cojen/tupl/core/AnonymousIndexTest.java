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
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        mDb = (LocalDatabase) newTempDatabase(getClass(), mConfig);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mConfig = null;
    }

    protected DatabaseConfig mConfig;
    protected LocalDatabase mDb;

    @Test
    public void openClose() throws Exception {
        var ids = new long[2];
        Transaction txn = mDb.newTransaction();
        mDb.createSecondaryIndexes(txn, 0, ids, () -> {});
        assertNotEquals(0, ids[0]);
        assertNotEquals(0, ids[1]);
        assertNotEquals(ids[0], ids[1]);

        Index ix1 = mDb.indexById(ids[0]);
        Index ix2 = mDb.indexById(ids[1]);

        assertNull(ix1.name());
        assertNull(ix1.nameString());
        assertNull(ix2.name());
        assertNull(ix2.nameString());
        assertEquals(ids[0], ix1.id());
        assertEquals(ids[1], ix2.id());

        ix1.store(null, "k1".getBytes(), "v1".getBytes());
        ix2.store(null, "k2".getBytes(), "v2".getBytes());

        ix1.close();
        ix2.close();

        ix1 = mDb.indexById(ids[0]);
        ix2 = mDb.indexById(ids[1]);

        fastAssertArrayEquals("v1".getBytes(), ix1.load(null, "k1".getBytes()));
        fastAssertArrayEquals("v2".getBytes(), ix2.load(null, "k2".getBytes()));

        try {
            mDb.renameIndex(ix1, "newName".getBytes());
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("anonymous"));
        }
    }

    @Test
    public void reopen() throws Exception {
        Index dir = mDb.openIndex("directory");
        byte[] idKey = new byte[8];

        Transaction txn = mDb.newTransaction();

        var ids = new long[1];
        mDb.createSecondaryIndexes(txn, 0, ids, () -> {
            try {
                Utils.encodeLongBE(idKey, 0, ids[0]);
                dir.store(txn, idKey, Utils.EMPTY_BYTES);
            } catch (Exception e) {
                Utils.rethrow(e);
            }
        });

        Index anon = mDb.indexById(ids[0]);
        anon.store(null, "hello".getBytes(), "world".getBytes());

        mDb = (LocalDatabase) reopenTempDatabase(getClass(), mDb, mConfig);

        Index dir2 = mDb.openIndex("directory");

        try (Cursor c = dir2.newCursor(null)) {
            c.first();
            fastAssertArrayEquals(idKey, c.key());
        }

        anon = mDb.indexById(ids[0]);
        fastAssertArrayEquals("world".getBytes(), anon.load(null, "hello".getBytes()));

        Transaction txn2 = mDb.newTransaction();
        mDb.createSecondaryIndexes(txn2, 0, ids, () -> {});
        assertNotEquals(anon.id(), ids[0]);

        mDb.deleteIndex(anon).run();

        assertNull(mDb.indexById(anon.id()));
    }

    @Test
    public void hidden() throws Exception {
        // Anonymous index isn't visible from the public registry.

        Transaction txn = mDb.newTransaction();

        var ids = new long[1];
        mDb.createSecondaryIndexes(txn, 0, ids, () -> {});
        Index ix = mDb.indexById(ids[0]);

        assertTrue(mDb.indexRegistryByName().isEmpty());
        assertTrue(mDb.indexRegistryById().isEmpty());
    }
}
