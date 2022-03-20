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

package org.cojen.tupl.rows;

import java.util.HashSet;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowScannerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowScannerTest.class.getName());
    }

    @Test
    public void projection() throws Exception {
        var db = Database.open(new DatabaseConfig());
        var table = db.openTable(TestRow.class);
        fill(table, 1, 5);

        verify(table.newRowScanner(null, "{}"), 1, 5);
        verify(table.newRowScanner(null, "{id}"), 1, 5, "id");
        verify(table.newRowScanner(null, "{id, id}"), 1, 5, "id");
        verify(table.newRowScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(table.newRowScanner(null, "~{}"), 1, 5, "id", "name", "path", "state");
        verify(table.newRowScanner(null, "~{id}"), 1, 5, "name", "path", "state");
        verify(table.newRowScanner(null, "~{id, id}"), 1, 5, "name", "path", "state");
        verify(table.newRowScanner(null, "~{name, state}"), 1, 5, "id", "path");

        verify(table.newRowScanner(null, "{}: name == ?", "name-3"), 3, 3);
        verify(table.newRowScanner(null, "{path}: name == ?", "name-3"), 3, 3, "path");
        verify(table.newRowScanner(null, "{name}: id == ?", 3), 3, 3, "name");
        verify(table.newRowScanner(null, "{name}: id > ?", 3), 4, 5, "name");
        verify(table.newRowScanner(null, "~{path, state}: name == ?", "name-3"), 3,3, "id", "name");

        var ix = table.viewSecondaryIndex("state");

        verify(ix.newRowScanner(null, "{}"), 1, 5);
        verify(ix.newRowScanner(null, "{id}"), 1, 5, "id");
        verify(ix.newRowScanner(null, "{id, id}"), 1, 5, "id");
        verify(ix.newRowScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(ix.newRowScanner(null, "~{}"), 1, 5, "id", "name", "path", "state");
        verify(ix.newRowScanner(null, "~{id}"), 1, 5, "name", "path", "state");
        verify(ix.newRowScanner(null, "~{id, id}"), 1, 5, "name", "path", "state");
        verify(ix.newRowScanner(null, "~{name, state}"), 1, 5, "id", "path");

        verify(ix.newRowScanner(null, "{}: name == ?", "name-3"), 3, 3);
        verify(ix.newRowScanner(null, "{path}: name == ?", "name-3"), 3, 3, "path");
        verify(ix.newRowScanner(null, "{name}: id == ?", 3), 3, 3, "name");
        verify(ix.newRowScanner(null, "{name}: id > ?", 3), 4, 5, "name");
        verify(ix.newRowScanner(null, "~{path, state}: name == ?", "name-3"), 3,3, "id", "name");

        ix = ix.viewUnjoined();

        verify(ix.newRowScanner(null, "{}"), 1, 5);
        verify(ix.newRowScanner(null, "{id}"), 1, 5, "id");
        verify(ix.newRowScanner(null, "{id, id}"), 1, 5, "id");
        verify(ix.newRowScanner(null, "{name, state}"), 1, 5, "name", "state");

        verify(ix.newRowScanner(null, "~{}"), 1, 5, "id", "name", "state");
        verify(ix.newRowScanner(null, "~{id}"), 1, 5, "name", "state");
        verify(ix.newRowScanner(null, "~{id, id}"), 1, 5, "name", "state");
        verify(ix.newRowScanner(null, "~{name, state}"), 1, 5, "id");

        verify(ix.newRowScanner(null, "{}: name == ?", "name-3"), 3, 3);
        try {
            ix.newRowScanner(null, "{path}: name == ?", "name-3");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("unavailable for selection: path"));
        }
        verify(ix.newRowScanner(null, "{name}: id == ?", 3), 3, 3, "name");
        verify(ix.newRowScanner(null, "{name}: id > ?", 3), 4, 5, "name");
        verify(ix.newRowScanner(null, "~{path, state}: name == ?", "name-3"), 3,3, "id", "name");
    }

    private static void verify(RowScanner<TestRow> s, int start, int end, String... expect)
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
                    fail(name);
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
        RowScanner<R> s = table.newRowScanner(null);
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
