/*
 *  Copyright (C) 2019 Cojen.org
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
public class RegistryTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RegistryTest.class.getName());
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
    public void registry() throws Exception {
        Index ix1 = mDb.openIndex("ix1");
        Index ix2 = mDb.openIndex("ix2");

        {
            View registry = mDb.indexRegistryByName();

            byte[] idValue = registry.load(null, "ix1".getBytes());
            assertNotNull(idValue);
            assertEquals(ix1.id(), Utils.decodeLongBE(idValue, 0));

            idValue = registry.load(null, "ix2".getBytes());
            assertNotNull(idValue);
            assertEquals(ix2.id(), Utils.decodeLongBE(idValue, 0));

            try {
                registry.store(null, "hello".getBytes(), "world".getBytes());
                fail();
            } catch (UnmodifiableViewException e) {
            }

            Cursor c = registry.newCursor(null);
            c.first();
            fastAssertArrayEquals("ix1".getBytes(), c.key());
            c.next();
            fastAssertArrayEquals("ix2".getBytes(), c.key());
            c.next();
            assertNull(c.key());
        }

        {
            View registry = mDb.indexRegistryById();

            var idKey = new byte[8];
            Utils.encodeLongBE(idKey, 0, ix1.id());
            byte[] name = registry.load(null, idKey);
            assertNotNull(name);
            assertEquals(ix1.nameString(), new String(name));

            Utils.encodeLongBE(idKey, 0, ix2.id());
            name = registry.load(null, idKey);
            assertNotNull(name);
            assertEquals(ix2.nameString(), new String(name));

            try {
                registry.store(null, "hello".getBytes(), "world".getBytes());
                fail();
            } catch (UnmodifiableViewException e) {
            }

            int compare = Long.compareUnsigned(ix1.id(), ix2.id());

            Cursor c = registry.newCursor(null);
            c.first();
            if (compare < 0) {
                Utils.encodeLongBE(idKey, 0, ix1.id());
            } else {
                Utils.encodeLongBE(idKey, 0, ix2.id());
            }
            fastAssertArrayEquals(idKey, c.key());
            c.next();
            if (compare < 0) {
                Utils.encodeLongBE(idKey, 0, ix2.id());
            } else {
                Utils.encodeLongBE(idKey, 0, ix1.id());
            }
            fastAssertArrayEquals(idKey, c.key());
            c.next();
            assertNull(c.key());
        }
    }
}
