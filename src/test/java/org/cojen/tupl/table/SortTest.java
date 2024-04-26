/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.table;

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SortTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SortTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = TestUtils.newTempDatabase(SortTest.class, new DatabaseConfig()
                                        .durabilityMode(DurabilityMode.NO_FLUSH)
                                        .maxCacheSize(100_000_000L));
    }

    @After
    public void teardown() throws Exception {
        TestUtils.deleteTempDatabases(SortTest.class);
        mDb = null;
    }

    private Database mDb;

    @Test
    public void basic() throws Exception {
        basic(1000);
    }

    @Test
    public void external() throws Exception {
        // Currently, an external sort is triggered for over 1 million rows.
        basic(1_500_000);
    }

    @Test
    public void empty() throws Exception {
        basic(0);
    }

    private void basic(int num) throws Exception {
        Table<TestRow> table = mDb.openTable(TestRow.class);
        var rnd = new Random(5309867);

        for (int i=0; i<num; i++) {
            TestRow row = table.newRow();
            row.v1((byte) rnd.nextInt());
            row.v2(rnd.nextInt());
            table.insert(null, row);
        }

        byte lastV1 = Byte.MIN_VALUE;
        int lastV2 = Integer.MIN_VALUE;
        int total = 0;

        try (Scanner<TestRow> s = table.newScanner(null, "{+v1, +v2}")) {
            for (TestRow row = s.row(); row != null; row = s.step(row)) {
                if (total == 0) {
                    try {
                        row.id();
                        fail();
                    } catch (IllegalStateException e) {
                    }
                }

                byte v1 = row.v1();
                int v2 = row.v2();

                if (v1 < lastV1) {
                    fail();
                } else if (v1 == lastV1) {
                    assertTrue(v2 >= lastV2);
                }

                lastV1 = v1;
                lastV2 = v2;
                total++;
            }
        }

        assertEquals(num, total);

        lastV1 = Byte.MAX_VALUE;
        lastV2 = Integer.MAX_VALUE;
        total = 0;

        try (Updater<TestRow> s = table.newUpdater(null, "{-v1, -v2, id}")) {
            for (TestRow row = s.row(); row != null; row = s.step(row)) {
                byte v1 = row.v1();
                int v2 = row.v2();

                if (v1 > lastV1) {
                    fail();
                } else if (v1 == lastV1) {
                    assertTrue(v2 <= lastV2);
                }

                lastV1 = v1;
                lastV2 = v2;
                total++;
            }
        }

        assertEquals(num, total);

        // Now actually update and delete some rows.

        int r1 = (int) (50e10 / num);
        int r2 = r1 * 3;

        long expected = 0;

        try (Updater<TestRow> s = table.newUpdater(null, "{+v1, -v2}")) {
            for (TestRow row = s.row(); row != null; ) {
                if (row.v2() == 0) {
                    row.v2(-1);
                    row = s.update(row);
                } else if (row.v2() > 0 && row.v2() < r1) {
                    row.v2(0);
                    row = s.update(row);
                    expected++;
                } else if (row.v2() >= r1 && row.v2() < r2) {
                    row = s.delete(row);
                    num--;
                } else {
                    row = s.step(row);
                }
            }
        }

        total = 0;

        try (Scanner<TestRow> s = table.newScanner(null, "{id} v2 == ?", 0)) {
            for (TestRow row = s.row(); row != null; row = s.step(row)) {
                total++;
            }
        }

        assertEquals(expected, total);

        total = 0;

        try (Scanner<TestRow> s = table.newScanner(null, "{id}")) {
            for (TestRow row = s.row(); row != null; row = s.step(row)) {
                total++;
            }
        }

        assertEquals(num, total);
    }

    @PrimaryKey("id")
    public static interface TestRow {
        @Automatic
        int id();
        void id(int id);

        byte v1();
        void v1(byte v);

        int v2();
        void v2(int v);
    }

    @Test
    public void nullLow() throws Exception {
        // Test sorting against a column which is ordered null low (default is null high).
        Table<TestRow2> table = mDb.openTable(TestRow2.class);

        {
            TestRow2 row = table.newRow();
            row.id(1);
            row.name("b");
            table.insert(null, row);
            row.id(1);
            row.name(null);
            table.insert(null, row);
            row.id(1);
            row.name("c");
            table.insert(null, row);
            row.id(1);
            row.name("a");
            table.insert(null, row);

        }

        try (Scanner<TestRow2> s = table.newScanner(null, "{+name}")) {
            TestRow2 row = s.row();
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals(null, row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow2> s = table.newScanner(null, "{+!name}")) {
            TestRow2 row = s.row();
            assertEquals(null, row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow2> s = table.newScanner(null, "{-name}")) {
            TestRow2 row = s.row();
            assertEquals(null, row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow2> s = table.newScanner(null, "{-!name}")) {
            TestRow2 row = s.row();
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals(null, row.name());
            row = s.step(row);
            assertNull(row);
        }
    }
 
    @PrimaryKey({"id", "+!name"})
    public static interface TestRow2 {
        int id();
        void id(int id);

        @Nullable
        String name();
        void name(String name);
    }

    @Test
    public void descending() throws Exception {
        // Test sorting against a column which is descending (default is ascending).
        Table<TestRow3> table = mDb.openTable(TestRow3.class);

        {
            TestRow3 row = table.newRow();
            row.id(1);
            row.name("b");
            table.insert(null, row);
            row.id(1);
            row.name(null);
            table.insert(null, row);
            row.id(1);
            row.name("c");
            table.insert(null, row);
            row.id(1);
            row.name("a");
            table.insert(null, row);

        }

        try (Scanner<TestRow3> s = table.newScanner(null, "{+name}")) {
            TestRow3 row = s.row();
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals(null, row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow3> s = table.newScanner(null, "{+!name}")) {
            TestRow3 row = s.row();
            assertEquals(null, row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow3> s = table.newScanner(null, "{-name}")) {
            TestRow3 row = s.row();
            assertEquals(null, row.name());
            row = s.step(row);
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertNull(row);
        }

        try (Scanner<TestRow3> s = table.newScanner(null, "{-!name}")) {
            TestRow3 row = s.row();
            assertEquals("c", row.name());
            row = s.step(row);
            assertEquals("b", row.name());
            row = s.step(row);
            assertEquals("a", row.name());
            row = s.step(row);
            assertEquals(null, row.name());
            row = s.step(row);
            assertNull(row);
        }
    }

    @PrimaryKey({"id", "-name"})
    public static interface TestRow3 {
        int id();
        void id(int id);

        @Nullable
        String name();
        void name(String name);
    }
}
