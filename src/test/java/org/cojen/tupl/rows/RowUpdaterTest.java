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

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowUpdaterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowUpdaterTest.class.getName());
    }

    @Test
    public void projection() throws Exception {
        var db = Database.open(new DatabaseConfig());
        var table = db.openTable(TestRow.class);
        var ix = table.viewSecondaryIndex("state");
        fill(table, 1, 5);

        Set<TestRow> copy1 = copy(table);
        assertEquals(5, copy1.size());

        var u = table.newRowUpdater(null, "{name}: id == ? || name == ?", 2, "name-3");
        for (var row = u.row(); u.row() != null; ) {
            try {
                row.id();
                fail();
            } catch (UnsetColumnException e) {
            }

            String name = row.name();
            row.name(name + '!');
            row = u.update(row);
        }

        assertEquals(5, count(table));
        assertEquals(5, count(ix.viewUnjoined()));
        assertEquals(5, count(ix));

        {
            Set<TestRow> copy2 = copy(table);
            assertEquals(5, copy2.size());
            var it1 = copy1.iterator();
            var it2 = copy2.iterator();
            while (it1.hasNext()) {
                var row1 = it1.next();
                var row2 = it2.next();
                long id = row1.id();
                if (id == 1 || id == 4 || id == 5) {
                    assertEquals(row1, row2);
                } else {
                    assertEquals(id, row2.id());
                    assertEquals(row1.state(), row2.state());
                    assertEquals(row1.path(), row2.path());
                    assertEquals(row1.name() + '!', row2.name());
                }
            }
        }

        u = table.newRowUpdater(null, "~{path}: id == ? || name == ?", 2, "name-3!");
        for (var row = u.row(); u.row() != null; ) {
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }

            if (row.id() == 2) {
                row.path(null);
            } else {
                row.path("none");
            }

            row = u.update(row);
        }

        {
            Set<TestRow> copy2 = copy(table);
            assertEquals(5, copy2.size());
            var it1 = copy1.iterator();
            var it2 = copy2.iterator();
            while (it1.hasNext()) {
                var row1 = it1.next();
                var row2 = it2.next();
                long id = row1.id();
                if (id == 1 || id == 4 || id == 5) {
                    assertEquals(row1, row2);
                } else {
                    assertEquals(id, row2.id());
                    assertEquals(row1.state(), row2.state());
                    assertEquals(row1.name() + '!', row2.name());
                    if (id == 2) {
                        assertEquals(null, row2.path());
                    } else if (id == 3) {
                        assertEquals("none", row2.path());
                    }
                }
            }
        }

        u = table.newRowUpdater(null, "~{path}: id == ?", 4);
        for (var row = u.row(); u.row() != null; ) {
            row.state(row.state() + 1000);
            row = u.update(row);
        }

        {
            var s = ix.newRowScanner(null, "state == ?", 1104);
            var row = s.row();
            assertNull(s.step());
            assertEquals(4, row.id());
            assertEquals("name-4", row.name());
            assertEquals("path-4", row.path());
            assertEquals(1104, row.state());
        }

        try {
            ix.viewUnjoined().newRowUpdater(null, "{state}");
            fail();
        } catch (UnmodifiableViewException e) {
        }
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

    private static <R> Set<R> copy(Table<R> table) throws Exception {
        var set = new LinkedHashSet<R>();
        RowScanner<R> s = table.newRowScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step()) {
            set.add(row);
        }
        return set;
    }

    private static <R> long count(Table<R> table) throws Exception {
        long count = 0;
        RowScanner<R> s = table.newRowScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            count++;
        }
        return count;
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
