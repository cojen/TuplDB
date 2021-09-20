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

package org.cojen.tupl.rows;

import java.math.BigDecimal;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class IndexTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IndexTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        Database db = Database.open(new DatabaseConfig().lockTimeout(100, TimeUnit.MILLISECONDS));
        Table<TestRow> table = db.openTable(TestRow.class);

        Table<TestRow> alt = table.alternateKeyTable("path");
        Table<TestRow> ix1 = table.secondaryIndexTable("name");
        Table<TestRow> ix2 = table.secondaryIndexTable("num", "name");

        assertNull(ix1.alternateKeyTable("path"));
        assertNull(ix1.secondaryIndexTable("name"));

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("path1");
            row.name("name1");
            row.num(new BigDecimal("123"));
            table.store(null, row);

            row.id(2);
            row.path("path2");
            row.name("name2");
            row.num(new BigDecimal("123"));
            table.store(null, row);

            row.id(3);
            row.path("path3");
            row.name("name1");
            row.num(new BigDecimal("987"));
            table.store(null, row);

            row.id(4);
            row.path("path1");
            row.name("name4");
            row.num(new BigDecimal("555"));
            try {
                table.store(null, row);
                fail();
            } catch (UniqueConstraintException e) {
                // Alternate key constraint.
            }
        }

        {
            TestRow row = table.newRow();
            row.path("path2");
            assertTrue(alt.load(null, row));
            assertEquals(2, row.id());
            assertEquals("path2", row.path());
            try {
                row.name();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=2, path=path2}"));
            assertTrue(alt.exists(null, row));
            try {
                alt.store(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.id(3);
            try {
                ix1.load(null, row);
                fail();
            } catch (IllegalStateException e) {
            }
            row.name("name1");
            assertTrue(ix1.load(null, row));
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=3, name=name1}"));
            assertTrue(ix1.exists(null, row));
            try {
                ix1.insert(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.id(2);
            row.num(new BigDecimal("123"));
            try {
                ix2.load(null, row);
                fail();
            } catch (IllegalStateException e) {
            }
            row.name("name2");
            assertTrue(ix2.load(null, row));
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=2, name=name2, num=123}"));
            assertTrue(ix2.exists(null, row));
            try {
                ix2.delete(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        try {
            ix1.newRowUpdater(null);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        scanExpect(alt, "TestRow{id=1, path=path1}",
                   "TestRow{id=2, path=path2}", "TestRow{id=3, path=path3}");

        scanExpect(ix1, "TestRow{id=1, name=name1}",
                   "TestRow{id=3, name=name1}", "TestRow{id=2, name=name2}");

        scanExpect(ix2, "TestRow{id=2, name=name2, num=123}",
                   "TestRow{id=1, name=name1, num=123}", "TestRow{id=3, name=name1, num=987}");

        {
            TestRow row = table.newRow();
            row.id(2);
            assertTrue(table.delete(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=path1}", "TestRow{id=3, path=path3}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=3, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=123}", "TestRow{id=3, name=name1, num=987}");

        {
            Transaction txn = db.newTransaction();
            TestRow row = table.newRow();
            row.id(3);
            assertTrue(table.delete(txn, row));

            {
                TestRow rowx = table.newRow();
                rowx.path("path3");
                try {
                    alt.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            {
                TestRow rowx = table.newRow();
                rowx.name("name1");
                rowx.id(3);
                try {
                    ix1.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            {
                TestRow rowx = table.newRow();
                rowx.num(new BigDecimal("987"));
                rowx.name("name1");
                rowx.id(3);
                try {
                    ix2.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            txn.commit();

            {
                TestRow rowx = table.newRow();
                rowx.path("path3");
                assertFalse(alt.exists(null, rowx));
            }

            {
                TestRow rowx = table.newRow();
                rowx.name("name1");
                rowx.id(3);
                assertFalse(ix1.exists(null, rowx));
            }

            {
                TestRow rowx = table.newRow();
                rowx.num(new BigDecimal("987"));
                rowx.name("name1");
                rowx.id(3);
                assertFalse(ix2.exists(null, rowx));
            }
        }

        scanExpect(alt, "TestRow{id=1, path=path1}");

        scanExpect(ix1, "TestRow{id=1, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=123}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("no-path");
            row.name("no-name");
            row.num(BigDecimal.ZERO);
            TestRow oldRow = table.exchange(null, row);
            assertTrue(oldRow.toString().contains("{id=1, name=name1, num=123, path=path1}"));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("path5");
            row.name("name5");
            row.num(new BigDecimal("555"));
            assertTrue(table.insert(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}", "TestRow{id=5, path=path5}");

        scanExpect(ix1, "TestRow{id=5, name=name5}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}", "TestRow{id=5, name=name5, num=555}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("no-path");
            row.name("!name5");
            row.num(new BigDecimal("-5"));

            try {
                table.replace(null, row);
                fail();
            } catch (UniqueConstraintException e) {
                // Alternate key constraint.
            }

            row.path("path5");
            assertTrue(table.replace(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}", "TestRow{id=5, path=path5}");

        scanExpect(ix1, "TestRow{id=5, name=!name5}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=5, name=!name5, num=-5}", "TestRow{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("!path5");
            row.name("!name55");
            row.num(new BigDecimal("55"));
            assertTrue(table.update(null, row));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=5, name=!name55}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}",
                   "TestRow{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.name("name1");
            assertTrue(table.update(null, row));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=5, name=!name55}", "TestRow{id=1, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.name("name5");
            assertTrue(table.merge(null, row));
            assertTrue(row.toString().contains("TestRow{id=5, name=name5, num=55, path=!path5}"));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=5, name=name5}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=name5, num=55}");

        {
            // Lock all the index entries that shouldn't be updated.
            Transaction txn = db.newTransaction();
            RowScanner<TestRow> s = ix1.newRowScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}
            s = ix2.newRowScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}

            TestRow row = table.newRow();
            row.id(1);
            row.path("path-1");
            row.name("name1");        // no change
            row.num(BigDecimal.ZERO); // no change
            table.store(null, row);

            txn.exit();
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=path-1}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=5, name=name5}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=name5, num=55}");
    }

    private static void dump(Table<TestRow> table) throws Exception {
        RowScanner<TestRow> s = table.newRowScanner(null);
        for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {
            System.out.println(row);
        }
    }

    private static void scanExpect(Table<TestRow> table, String... expectRows) throws Exception {
        int pos = 0;
        RowScanner<TestRow> s = table.newRowScanner(null);
        for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {
            String expectRow = expectRows[pos++];
            assertTrue(row.toString().contains(expectRow));
        }
        assertEquals(expectRows.length, pos);
    }

    @PrimaryKey("id")
    @AlternateKey("path")
    @SecondaryIndex("name")
    @SecondaryIndex({"+num", "-name"})
    public interface TestRow {
        long id();
        void id(long id);

        String path();
        void path(String str);

        String name();
        void name(String str);

        BigDecimal num();
        void num(BigDecimal num);
    }
}
