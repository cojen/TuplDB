/*
 *  Copyright (C) 2023 Cojen.org
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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ViewedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ViewedTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        setup(Database.open(new DatabaseConfig()));
    }

    private void setup(Database db) throws Exception {
        mDb = db;
        mTable = db.openTable(TestRow.class);
        fill();
    }

    private void fill() throws Exception {
        for (int i = 1; i <= 10; i++) {
            var row = mTable.newRow();
            row.id(i);
            row.name("name-" + i);
            row.num(i * 2);
            mTable.insert(null, row);
        }
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
        TestUtils.deleteTempDatabases(getClass());
    }

    protected Database mDb;
    protected Table<TestRow> mTable;

    @PrimaryKey("id")
    @SecondaryIndex("name")
    public interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String name);

        int num();
        void num(int num);
    }

    @Test
    public void noFilter() throws Exception {
        var view = ViewedTable.view(mTable, "{-id, *}");

        // CRUD operations should work just fine and behave the same as before.

        var row = view.newRow();
        row.id(1);
        view.load(null, row);
        assertTrue(view.exists(null, row));
        row.name("hello");
        view.store(null, row);
        view.load(null, row);
        assertEquals("hello", row.name());
        row.name("world");
        var row2 = view.exchange(null, row);
        assertEquals("{id=1, num=2, name=hello}", row2.toString());
        try {
            view.insert(null, row);
            fail();
        } catch (UniqueConstraintException e) {
        }
        view.replace(null, row2);
        view.load(null, row2);
        assertEquals("{id=1, num=2, name=hello}", row2.toString());
        row = view.newRow();
        row.id(2);
        row.num(999);
        view.update(null, row);
        row = view.newRow();
        row.id(2);
        view.load(null, row);
        assertEquals("{id=2, num=999, name=name-2}", row.toString());
        row = view.newRow();
        row.id(2);
        row.num(9999);
        view.merge(null, row);
        assertEquals("{id=2, num=9999, name=name-2}", row.toString());
        var txn = view.newTransaction(null);
        view.delete(txn, row2);
        assertFalse(view.exists(txn, row2));
        txn.reset();

        // Scanners and Updaters should honor the view's natural order unless overridden.

        QueryPlan plan = view.queryAll().scannerPlan(null);
        assertEquals("""
- reverse full scan over primary key: org.cojen.tupl.table.ViewedTest$TestRow
  key columns: +id
                     """,
                     plan.toString());

        plan = view.queryAll().updaterPlan(null);
        assertEquals("""
- reverse full scan over primary key: org.cojen.tupl.table.ViewedTest$TestRow
  key columns: +id
                     """,
                     plan.toString());

        Query<TestRow> query = view.query("name == ?");
        assertEquals(TestRow.class, query.rowType());
        assertEquals(1, query.argumentCount());
        plan = query.scannerPlan(null);
        assertEquals("""
- sort: -id
  - primary join: org.cojen.tupl.table.ViewedTest$TestRow
    key columns: +id
    - range scan over secondary index: org.cojen.tupl.table.ViewedTest$TestRow
      key columns: +name, +id
      range: name >= ?1 .. name <= ?1
                     """,
                     plan.toString());

        query = view.query("{id, name} name == ?");
        assertEquals(1, query.argumentCount());
        plan = query.scannerPlan(null);
        assertEquals("""
- sort: -id
  - range scan over secondary index: org.cojen.tupl.table.ViewedTest$TestRow
    key columns: +name, +id
    range: name >= ?1 .. name <= ?1
                     """,
                     plan.toString());

        plan = view.query("{id, +name} name == ?").scannerPlan(null);
        assertEquals("""
- range scan over secondary index: org.cojen.tupl.table.ViewedTest$TestRow
  key columns: +name, +id
  range: name >= ?1 .. name <= ?1
                     """,
                     plan.toString());

        try (var scanner = view.newScanner(null)) {
            int expect = 10;
            for (row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect, row.id());
                expect--;
            }
        }

        try (var scanner = view.newScanner(null, "num >= ? && num < ?", 10, 100)) {
            int total = 0;
            int expect = 10;
            for (row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect, row.id());
                expect--;
                total++;
            }
            assertEquals(6, total);
        }

        try (var scanner = view.newUpdater(null)) {
            int expect = 10;
            for (row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect, row.id());
                expect--;
            }
        }

        try (var scanner = view.newUpdater(null, "num >= ? && num < ?", 10, 100)) {
            int total = 0;
            int expect = 10;
            for (row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect, row.id());
                expect--;
                total++;
            }
            assertEquals(6, total);
        }
    }

    @Test
    public void hasFilter() throws Exception {
        /*
          Restricted to these rows initially:

          {id=5, num=10, name=name-5}
          {id=4, num=8, name=name-4}
          {id=3, num=6, name=name-3}
        */
        var view = ViewedTable.view(mTable, "{-id, *} id >= ? && num <= ?", 3, 10);

        // CRUD operations are restricted.

        var row = view.newRow();

        row.id(3);
        view.load(null, row);
        assertTrue(view.exists(null, row));

        row.id(1);
        try {
            view.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        try {
            view.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        row.id(100);
        row.name("xxx");
        assertFalse(view.tryLoad(null, row));
        assertEquals("{*id=100}", row.toString());

        row.name("name-x");
        row.num(111);
        try {
            view.store(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.num(1);
        view.store(null, row);
        view.load(null, row);
        assertEquals("{id=100, num=1, name=name-x}", row.toString());

        row.num(111);
        try {
            view.exchange(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.num(2);
        var row2 = view.exchange(null, row);
        assertEquals("{id=100, num=2, name=name-x}", row.toString());
        assertEquals("{id=100, num=1, name=name-x}", row2.toString());

        row = view.newRow();
        row.id(200);
        row.num(200);
        row.name("hello");
        try {
            view.insert(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.num(2);
        view.insert(null, row);

        row.num(200);
        try {
            view.replace(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.num(10);
        view.replace(null, row);

        row = view.newRow();
        row.id(6);
        row.num(9);
        try {
            view.update(null, row);
            fail();
        } catch (NoSuchRowException e) {
        }
        row.id(2);
        try {
            view.update(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.id(5);
        row.num(100);
        try {
            view.update(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.num(9);
        view.update(null, row);
        assertEquals("{id=5, num=9}", row.toString());

        row = view.newRow();
        row.id(5);
        row.name("world");
        view.merge(null, row);
        assertEquals("{id=5, num=9, name=world}", row.toString());

        row = view.newRow();
        row.id(1);
        try {
            view.delete(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row.id(300);
        row.num(100);
        assertFalse(view.tryDelete(null, row));
        row.id(3);
        view.delete(null, row);

        try (var scanner = view.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=100, num=2, name=name-x}", row.toString());
            row = scanner.step();
            assertEquals("{id=5, num=9, name=world}", row.toString());
            row = scanner.step();
            assertEquals("{id=4, num=8, name=name-4}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row = updater.step(row);
            assertEquals("{id=100, num=2, name=name-x}", row.toString());
            row = updater.step();
            assertEquals("{id=5, num=9, name=world}", row.toString());
            row = updater.step();
            assertEquals("{id=4, num=8, name=name-4}", row.toString());
            row = updater.step();
            assertEquals(null, row);
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row.num(100);
            try {
                updater.update();
                fail();
            } catch (ViewConstraintException e) {
            }
            assertNull(updater.step());
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row.num(100);
            try {
                updater.update(row);
                fail();
            } catch (ViewConstraintException e) {
            }
            assertNull(updater.step());
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row.id(1);
            try {
                updater.delete();
                fail();
            } catch (ViewConstraintException e) {
            }
            assertNull(updater.step());
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row.id(1);
            try {
                updater.delete(row);
                fail();
            } catch (ViewConstraintException e) {
            }
            assertNull(updater.step());
        }

        try (var updater = view.newUpdater(null, "id != ?", 200)) {
            row = updater.row();
            assertEquals("{id=100, num=2, name=name-x}", row.toString());
            row.num(100);
            try {
                updater.update();
                fail();
            } catch (ViewConstraintException e) {
            }
            assertNull(updater.step());
        }

        try (var updater = view.newUpdater(null, "id != ?", 200)) {
            row = updater.row();
            assertEquals("{id=100, num=2, name=name-x}", row.toString());
            row = updater.delete(row);
            assertEquals("{id=5, num=9, name=world}", row.toString());
            row.name("world!!!");
            row = updater.update();
            assertEquals("{id=4, num=8, name=name-4}", row.toString());
        }

        try (var updater = view.newUpdater(null)) {
            row = updater.row();
            assertEquals("{id=200, num=10, name=hello}", row.toString());
            row = updater.step(row);
            assertEquals("{id=5, num=9, name=world!!!}", row.toString());
            row = updater.step();
            assertEquals("{id=4, num=8, name=name-4}", row.toString());
            row = updater.step();
            assertEquals(null, row);
        }
    }

    @Test
    public void hasFilterQuick() throws Exception {
        /*
          The filter only checks the primary key, and so the "load", "exists", and "delete"
          methods have quick checks.

          Restricted to these rows initially:

          {id=8, num=16, name=name-8}
          {id=9, num=18, name=name-9}
          {id=10, num=20, name=name-10}
        */
        var view = ViewedTable.view(mTable, "{id, *} id >= ?", 8);

        var row = view.newRow();

        row.id(8);
        view.load(null, row);
        assertTrue(view.exists(null, row));

        row.id(1);
        try {
            view.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        try {
            view.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.delete(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        row.id(9);
        view.delete(null, row);

        try (var scanner = view.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=8, num=16, name=name-8}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=10, num=20, name=name-10}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }
    }

    @Test
    public void hasProjectionAndNoFilter() throws Exception {
        var view = ViewedTable.view(mTable, "{id, name}");

        // Some CRUD operations are restricted.

        var row = view.newRow();
        row.id(1);
        view.load(null, row);
        assertEquals("{id=1, name=name-1}", row.toString());
        assertTrue(view.exists(null, row));
        row.name("hello");
        try {
            view.store(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        view.load(null, row);
        assertEquals("name-1", row.name());
        try {
            view.exchange(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        try {
            view.insert(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        try {
            view.replace(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row = view.newRow();
        row.id(2);
        row.num(999);
        try {
            view.update(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        row = view.newRow();
        row.id(2);
        row.name("hello");
        view.update(null, row);
        row = view.newRow();
        row.id(2);
        view.load(null, row);
        assertEquals("{id=2, name=hello}", row.toString());
        row = view.newRow();
        row.id(2);
        row.name("world");
        view.merge(null, row);
        assertEquals("{id=2, name=world}", row.toString());

        QueryPlan plan = view.query("id <= ?").scannerPlan(null, 2);
        assertEquals("""
- reverse range scan over primary key: org.cojen.tupl.table.ViewedTest$TestRow
  key columns: +id
  range: .. id <= ?1
                     """,
                     plan.toString());

        try (var scanner = view.newScanner(null, "id <= ?", 2)) {
            row = scanner.row();
            assertEquals("{id=2, name=world}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=1, name=name-1}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }
    }

    @Test
    public void hasProjectionAndHasFilter() throws Exception {
        /*
          Restricted to these rows initially:

          {id=5, name=name-5}
          {id=4, name=name-4}
          {id=3, name=name-3}
        */
        var view = ViewedTable.view(mTable, "{id, name} id >= ? && num <= ?", 3, 10);

        // CRUD operations are restricted.

        var row = view.newRow();

        row.id(3);
        view.load(null, row);
        assertEquals("{id=3, name=name-3}", row.toString());
        assertTrue(view.exists(null, row));

        row.id(1);
        try {
            view.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
        try {
            view.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        row.id(100);
        row.name("xxx");
        assertFalse(view.tryLoad(null, row));
        assertEquals("{*id=100}", row.toString());

        row.name("name-x");
        try {
            view.store(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.exchange(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.insert(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.replace(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        row.id(4);
        row.name("hello");
        view.merge(null, row);
        assertEquals("{id=4, name=hello}", row.toString());
    }

    @Test
    public void hasProjectionAndHasFilterNoQuick() throws Exception {
        /*
          Even when the filter only checks the primary key, the "load" method shouldn't perform
          a quick check. This is because the projection must always be applied.

          Restricted to these rows initially:

          {id=8, name=name-8}
          {id=9, name=name-9}
          {id=10, name=name-10}
        */
        var view = ViewedTable.view(mTable,  "{id, name} id >= ?", 8);

        var row = view.newRow();

        row.id(9);
        view.load(null, row);
        assertEquals("{id=9, name=name-9}", row.toString());
    }

    @Test
    public void noPrimaryKeyAndNoFilter() throws Exception {
        var view = ViewedTable.view(mTable, "{num, name}");

        try (var scanner = view.newScanner(null, "num >= ? && num <= ?", 6, 10)) {
            var row = scanner.row();
            assertEquals("{num=6, name=name-3}", row.toString());
            row = scanner.step(row);
            assertEquals("{num=8, name=name-4}", row.toString());
            row = scanner.step();
            assertEquals("{num=10, name=name-5}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }

        var row = view.newRow();
        try {
            view.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.store(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.update(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.merge(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.delete(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
    }

    @Test
    public void noPrimaryKeyAndHasFilter() throws Exception {
        /*
          Restricted to these rows initially:

          {num=6, name=name-3}
          {num=8, name=name-4}
          {num=10, name=name-5}
        */
        var view = ViewedTable.view(mTable, "{num, name} id >= ? && num <= ?", 3, 10);

        try (var scanner = view.newScanner(null)) {
            var row = scanner.row();
            assertEquals("{num=6, name=name-3}", row.toString());
            row = scanner.step(row);
            assertEquals("{num=8, name=name-4}", row.toString());
            row = scanner.step();
            assertEquals("{num=10, name=name-5}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }

        var row = view.newRow();
        try {
            view.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.store(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.update(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.merge(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            view.delete(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }
    }

    @Test
    public void multiView() throws Exception {
        var view = ViewedTable.view(mTable, "{id, name} id >= ?", 3);

        try {
            view = ViewedTable.view(view, "{id, name, num} id <= ?", 5);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unknown column"));
            assertTrue(e.getMessage().contains("num"));
        }

        view = ViewedTable.view(view, "{-id, *} id <= ?", 5);

        try (var scanner = view.newScanner(null)) {
            var row = scanner.row();
            assertEquals("{id=5, name=name-5}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=4, name=name-4}", row.toString());
            row = scanner.step();
            assertEquals("{id=3, name=name-3}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }

        try {
            view.newScanner(null, "num != ?", 1);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unknown column"));
            assertTrue(e.getMessage().contains("num"));
        }

        try (var scanner = view.newScanner(null, "name != ?", "name-4")) {
            var row = scanner.row();
            assertEquals("{id=5, name=name-5}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=3, name=name-3}", row.toString());
            row = scanner.step();
            assertEquals(null, row);
        }
    }
}
