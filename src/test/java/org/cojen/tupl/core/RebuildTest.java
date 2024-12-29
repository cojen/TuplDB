/*
 *  Copyright (C) 2024 Cojen.org
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

import java.io.File;

import java.util.HashSet;
import java.util.Random;

import java.util.zip.CRC32C;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class RebuildTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RebuildTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void basic() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(4096)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        final long seed = 1234567;

        Database db = newTempDatabase(getClass(), config);

        var rnd = new Random(seed);

        Index ix = db.openIndex("test");

        var key = new byte[8];
        for (int i=0; i<10_000; i++) {
            rnd.nextBytes(key);
            var value = new byte[rnd.nextInt(500)];
            rnd.nextBytes(value);
            assertTrue(ix.insert(null, key, value));
        }

        for (int i=0; i<1000; i++) {
            rnd.nextBytes(key);
            var value = new byte[rnd.nextInt(500_000)];
            rnd.nextBytes(value);
            assertTrue(ix.insert(null, key, value));
        }

        Table<TestRow> table = db.openTable(TestRow.class);

        for (int i=0; i<10_000; i++) {
            TestRow row = table.newRow();
            row.id(rnd.nextLong());
            row.value(rnd.nextLong());
            table.insert(null, row);
        }

        for (int i=0; i<1000; i++) {
            rnd.nextBytes(key);
            var buf = new byte[1];
            long length = rnd.nextLong(10_000_000);
            try (Cursor c = ix.newCursor(null)) {
                c.find(key);
                c.valueLength(length);
                var positions = new HashSet<Long>();
                for (int j=0; j<100; j++) {
                    long pos;
                    do {
                        pos = rnd.nextLong(length);
                    } while (!positions.add(pos));
                    rnd.nextBytes(buf);
                    c.valueWrite(pos, buf, 0, 1);
                }
            }
        }

        db.close();

        try {
            Database.rebuild(config, config, 0);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("database already exists"));
        }

        DatabaseConfig newConfig = new DatabaseConfig()
            .pageSize(6000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .checksumPages(CRC32C::new)
            .baseFile(newTempBaseFile(getClass()));

        var newDb = Database.rebuild(config, newConfig, 0);

        long newIndexId;

        try {
            rnd = new Random(seed);

            ix = newDb.findIndex("test");

            for (int i=0; i<10_000; i++) {
                rnd.nextBytes(key);
                var expect = new byte[rnd.nextInt(500)];
                rnd.nextBytes(expect);
                fastAssertArrayEquals(expect, ix.load(null, key));
            }

            for (int i=0; i<1000; i++) {
                rnd.nextBytes(key);
                var expect = new byte[rnd.nextInt(500_000)];
                rnd.nextBytes(expect);
                fastAssertArrayEquals(expect, ix.load(null, key));
            }

            table = newDb.findTable(TestRow.class);

            Query<TestRow> query = table.query("value == ?");
            assertEquals("secondary index", ((QueryPlan.RangeScan) query.scannerPlan(null)).which);

            for (int i=0; i<10_000; i++) {
                TestRow row = table.newRow();
                row.id(rnd.nextLong());
                long expect = rnd.nextLong();
                table.load(null, row);
                assertEquals(expect, row.value());

                try (Scanner<TestRow> s = query.newScanner(null, expect)) {
                    assertEquals(row, s.row());
                }
            }

            for (int i=0; i<1000; i++) {
                rnd.nextBytes(key);
                var buf1 = new byte[1];
                var buf2 = new byte[1];
                long length = rnd.nextLong(10_000_000);
                try (Cursor c = ix.newCursor(null)) {
                    c.find(key);
                    assertEquals(length, c.valueLength());
                    var positions = new HashSet<Long>();
                    for (int j=0; j<100; j++) {
                        long pos;
                        do {
                            pos = rnd.nextLong(length);
                        } while (!positions.add(pos));
                        rnd.nextBytes(buf1);
                        c.valueRead(pos, buf2, 0, buf2.length);
                        assertArrayEquals(buf1, buf2);
                    }
                }
            }

            // The new database should have the same database id, and it should produce the
            // same index id sequence as the original database.
            newIndexId = newDb.openIndex("newIndex").id();
        } finally {
            newDb.close();
        }


        db = reopenTempDatabase(getClass(), db, config);

        assertEquals(newIndexId, db.openIndex("anotherIndex").id());
    }

    @PrimaryKey("id")
    @SecondaryIndex("value")
    public static interface TestRow {
        long id();
        void id(long id);

        long value();
        void value(long v);
    }
}
