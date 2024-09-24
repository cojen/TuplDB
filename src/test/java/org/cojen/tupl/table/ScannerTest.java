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

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.QueryException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ScannerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ScannerTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = Database.open(new DatabaseConfig());
    }

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
    }

    protected Database mDb;

    @Test
    public void projection() throws Exception {
        var table = mDb.openTable(TestRow.class);
        fill(table, 1, 5);

        verify(table.newScanner(null, "{}"), 1, 5);
        verify(table.newScanner(null, "{id}"), 1, 5, "id");

        try {
            verify(table.newScanner(null, "{id, id}"), 1, 5, "id");
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("Duplicate projection"));
            assertEquals(5, e.startPos());
            assertEquals(7, e.endPos());
        }

        verify(table.newScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(table.newScanner(null, "{*}"), 1, 5, "id", "name", "path", "state");

        verify(table.newScanner(null, "{~id, *}"), 1, 5, "id", "name", "path", "state");

        verify(table.newScanner(null, "{~name, *, ~state}"), 1, 5, "id", "name", "path");

        verify(table.newScanner(null, "{} name == ?", "name-3"), 3, 3);
        verify(table.newScanner(null, "{path} name == ?", "name-3"), 3, 3, "path");
        verify(table.newScanner(null, "{name} id == ?", 3), 3, 3, "name");
        verify(table.newScanner(null, "{name} id > ?", 3), 4, 5, "name");
        verify(table.newScanner
               (null, "{*, ~path, ~state} name == ?", "name-3"), 3, 3, "id", "name");

        if (table instanceof StoredTable<TestRow> btable) {
            checkSecondary(btable);
        }
    }

    @Test
    public void timeout() throws Exception {
        Table<TestRow> table = mDb.openTable(TestRow.class);
        fill(table, 1, 5);

        // Lock the first row.
        Transaction txn1 = mDb.newTransaction();
        {
            TestRow row = table.newRow();
            row.id(1);
            row.name("name-1");
            table.load(txn1, row);
        }

        Transaction txn2 = mDb.newTransaction();
        txn2.lockTimeout(10, TimeUnit.MILLISECONDS);

        try {
            table.newScanner(txn2);
            fail();
        } catch (LockTimeoutException e) {
        }

        try {
            table.newScanner(txn2, "id == ? && name == ?", 1, "name-1");
            fail();
        } catch (LockTimeoutException e) {
        }

        // Lock the second row.
        txn1.rollback();
        {
            TestRow row = table.newRow();
            row.id(2);
            row.name("name-2");
            table.load(txn1, row);
        }

        Scanner<TestRow> s = table.newScanner(txn2);
        assertEquals(1, s.row().id());

        try {
            s.step();
            fail();
        } catch (LockTimeoutException e) {
        }

        Query<TestRow> query = table.query("id == ? && name == ?");
        assertEquals(2, query.argumentCount());
        assertNull(query.newScanner(txn2, 1, "xxx").row());

        s = query.newScanner(txn2, 1, "name-1");
        assertEquals(1, s.row().id());

        query = table.query("id >= ?");
        s = query.newScanner(txn2, 1);
        assertEquals(1, s.row().id());

        try {
            s.step();
            fail();
        } catch (LockTimeoutException e) {
        }
    }

    private void checkSecondary(StoredTable<TestRow> table) throws Exception {
        var ix = table.viewSecondaryIndex("state");

        verify(ix.newScanner(null, "{}"), 1, 5);
        verify(ix.newScanner(null, "{id}"), 1, 5, "id");

        try {
            ix.newScanner(null, "{id, id}");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Duplicate projection"));
        }

        verify(ix.newScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(ix.newScanner(null, "{*}"), 1, 5, "id", "name", "path", "state");

        verify(ix.newScanner(null, "{~id, *}"), 1, 5, "id", "name", "path", "state");

        try {
            ix.newScanner(null, "{*, ~id, ~id}");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("already excluded"));
        }

        verify(ix.newScanner(null, "{*, ~id}"), 1, 5, "name", "path", "state");

        verify(ix.newScanner(null, "{*, ~name, ~state}"), 1, 5, "id", "path");

        verify(ix.newScanner(null, "{} name == ?", "name-3"), 3, 3);
        verify(ix.newScanner(null, "{path} name == ?", "name-3"), 3, 3, "path");
        verify(ix.newScanner(null, "{name} id == ?", 3), 3, 3, "name");
        verify(ix.newScanner(null, "{name} id > ?", 3), 4, 5, "name");
        verify(ix.newScanner(null, "{*, ~path, ~state} name == ?", "name-3"), 3, 3, "id", "name");

        var ix2 = ix.viewUnjoined();

        verify(ix2.newScanner(null, "{}"), 1, 5);
        verify(ix2.newScanner(null, "{id}"), 1, 5, "id");
        verify(ix2.newScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(ix2.newScanner(null, "{*}"), 1, 5, "id", "name", "state");
        verify(ix2.newScanner(null, "{*, ~id}"), 1, 5, "name", "state");
        verify(ix2.newScanner(null, "{*, ~name, ~state, id}"), 1, 5, "id");

        verify(ix2.newScanner(null, "{} name == ?", "name-3"), 3, 3);
        try {
            ix2.newScanner(null, "{path} name == ?", "name-3");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unknown column"));
            assertTrue(e.getMessage().contains("path"));
        }
        verify(ix2.newScanner(null, "{name} id == ?", 3), 3, 3, "name");
        verify(ix2.newScanner(null, "{name} id > ?", 3), 4, 5, "name");
        verify(ix2.newScanner(null, "{*, ~state} name == ?", "name-3"), 3, 3, "id", "name");
    }

    private static void verify(Scanner<TestRow> s, int start, int end, String... expect)
        throws Exception
    {
        var notExpect = new HashSet<>(Set.of("id", "name", "path", "state"));
        for (String name : expect) {
            notExpect.remove(name);
        }

        for (int i=start; i<=end; i++) {
            TestRow row = s.row();

            for (String name : expect) {
                switch (name) {
                case "id" -> assertEquals(i, row.id());
                case "name" -> assertEquals("name-" + i, row.name());
                case "path" -> assertEquals("path-" + i, row.path());
                case "state" -> assertEquals(100 + i, row.state());
                default -> fail();
                }
            }

            for (String name : notExpect) {
                try {
                    switch (name) {
                    case "id" -> row.id();
                    case "name" -> row.name();
                    case "path" -> row.path();
                    case "state" -> row.state();
                    }
                    fail("" + row + ", " + name);
                } catch (UnsetColumnException e) {
                    // Expected.
                }
            }

            s.step();
        }

        assertNull(s.step());
        s.close();
    }

    private static void fill(Table<TestRow> table, int start, int end) throws Exception {
        for (int i=start; i<=end; i++) {
            var row = table.newRow();
            row.id(i);
            row.name("name-" + i);
            row.path("path-" + i);
            row.state(100 + i);
            table.insert(null, row);
        }
    }

    private static <R> void dump(Table<R> table) throws Exception {
        Scanner<R> s = table.newScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            System.out.println(row);
        }
    }

    @PrimaryKey({"id", "name"})
    @SecondaryIndex("state")
    public interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String str);

        @Nullable
        String path();
        void path(String str);

        int state();
        void state(int x);
    }
}
