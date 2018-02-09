/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.concurrent.atomic.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Miscellaneous tests that operate on the database instance.
 *
 * @author Brian S O'Neill
 */
public class DatabaseTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DatabaseTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void openListener() throws Exception {
        AtomicReference<Database> dbRef = new AtomicReference<>();
        AtomicReference<Index> ixRef = new AtomicReference<>();

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .indexOpenListener((db, ix) -> {
                assertTrue(db == dbRef.get());
                ixRef.set(ix);

                String name = ix.getNameString();

                switch (name) {
                case "cycle":
                    try {
                        Index self = db.openIndex(name);
                    } catch (Exception e) {
                        throw Utils.rethrow(e);
                    }
                    break;
                case "exception":
                    throw new Error("oops");
                }
            });

        Database db = newTempDatabase(getClass(), config);
        dbRef.set(db);

        Index ix = db.openIndex("test");
        assertTrue(ix == ixRef.get());
        ixRef.set(null);

        ix = db.openIndex("test");
        assertNull(ixRef.get());

        ix.close();
        ix = db.openIndex("test");
        assertTrue(ix == ixRef.get());
        ixRef.set(null);

        // Open an index which cycles back and opens itself.
        try {
            Index cycle = db.openIndex("cycle");
            fail();
        } catch (LockFailureException e) {
            // Deadlock averted.
        }

        assertNull(db.findIndex("cycle"));

        Index leaked = ixRef.get();
        assertEquals("cycle", leaked.getNameString());

        try {
            leaked.store(null, new byte[1], new byte[1]);
            fail();
        } catch (ClosedIndexException e) {
            // Leaked index was closed as a side effect of the cycle.
        }

        // Listener throws an exception.
        try {
            Index cycle = db.openIndex("exception");
            fail();
        } catch (Error e) {
        }

        assertNull(db.findIndex("exception"));

        leaked = ixRef.get();
        assertEquals("exception", leaked.getNameString());

        try {
            leaked.store(null, new byte[1], new byte[1]);
            fail();
        } catch (ClosedIndexException e) {
            // Leaked index was closed as a side effect of the exception.
        }
    }

    @Test
    public void openListenerDuringRecovery() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        final Database db1 = newTempDatabase(getClass(), config);

        Index ix1 = db1.openIndex("test");
        ix1.store(null, "hello".getBytes(), "world".getBytes());

        AtomicInteger called = new AtomicInteger();
        AtomicReference<Database> dbRef = new AtomicReference<>();
        AtomicReference<Index> ixRef = new AtomicReference<>();

        config.indexOpenListener((db2, ix2) -> {
            int count = called.getAndIncrement();
            assertTrue(db2 != db1);
            assertTrue(ix2 != ix1);
            assertEquals("test", ix2.getNameString());
            dbRef.set(db2);
            ixRef.set(ix2);

            try {
                if (count == 0) {
                    assertNull(ix2.load(null, "hello".getBytes()));
                    ix2.store(null, "key".getBytes(), "value".getBytes());
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        });

        Database db3 = reopenTempDatabase(getClass(), db1, config);
        Index ix3 = db3.openIndex("test");

        assertEquals(1, called.get());
        assertTrue(db3 == dbRef.get());
        assertTrue(ix3 == ixRef.get());

        fastAssertArrayEquals("world".getBytes(), ix3.load(null, "hello".getBytes()));
        fastAssertArrayEquals("value".getBytes(), ix3.load(null, "key".getBytes()));

        Database db4 = reopenTempDatabase(getClass(), db3, config);
        Index ix4 = db4.openIndex("test");

        assertEquals(2, called.get());
        assertTrue(db4 == dbRef.get());
        assertTrue(ix4 == ixRef.get());

        fastAssertArrayEquals("world".getBytes(), ix4.load(null, "hello".getBytes()));
        fastAssertArrayEquals("value".getBytes(), ix4.load(null, "key".getBytes()));
    }
}
