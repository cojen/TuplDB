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

package org.cojen.tupl.table.expr;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;

import org.cojen.tupl.table.IdentityTable;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class FunctionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FunctionTest.class.getName());
    }

    @PrimaryKey("id")
    public static interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String v);

        @Nullable
        String value();
        void value(String v);
    }

    @Test
    public void coalesce() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{c = coalesce()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("at least 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{c = coalesce(5, true)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("no common type"));
        }

        verify("{c = coalesce('hello')}", "{c=hello}");
        verify("{c = coalesce('hello', 3)}", "{c=hello}");
        verify("{c = coalesce(3, \"hello\")}", "{c=3}");
        verify("{c = coalesce(3, 4)}", "{c=3}");
        verify("{c = coalesce(null)}", "{c=null}");
        verify("{c = coalesce(null, 'hello')}", "{c=hello}");
        verify("{c = coalesce('hello', null)}", "{c=hello}");
        verify("{c = coalesce('hello', 'world')}", "{c=hello}");
        verify("{c = coalesce(null, null)}", "{c=null}");
        verify("{c = coalesce(null, 3) + 1}", "{c=4}");
        verify("{c = coalesce(null) + 1}", "{c=null}");
        verify("{c = coalesce(null, 3 + 1)}", "{c=4}");
        verify("{a=1} coalesce(null, 3 + 1) == 4", "{a=1}");
        verify("{a=1} coalesce(null, 3 + 1) != 4");

        verify("{c = coalesce(?, ?)}", new Object[] {null, 33}, "{c=33}");
        verify("{c = -?}", new Object[] {33}, "{c=-33}");
        verify("{c = -coalesce(?)}", new Object[] {33}, "{c=-33}");
        verify("{c = -coalesce(?, ?)}", new Object[] {null, 33}, "{c=-33}");

        Table<TestRow> table = fill(2);

        verify(table, "{id, v = coalesce(value, 'hello')}", "{id=1, v=value-1}", "{id=2, v=hello}");
        verify(table, "{+v = coalesce(value, 'hello')}", "{v=hello}", "{v=value-1}");
        verify(table, "{v = coalesce(value, ?)}", new Object[] {123}, "{v=value-1}", "{v=123}");
    }

    private static RelationExpr parse(String query) {
        return parse(IdentityTable.THE, query);
    }

    private static RelationExpr parse(Table table, String query) {
        return Parser.parse(table, query);
    }

    private static void verify(String query, String... expect) throws Exception {
        verify(parse(query), expect);
    }

    private static void verify(String query, Object[] args, String... expect) throws Exception {
        verify(parse(query), args, expect);
    }

    private static void verify(Table table, String query, String... expect) throws Exception {
        verify(parse(table, query), expect);
    }

    private static void verify(Table table, String query, Object[] args, String... expect)
        throws Exception
    {
        verify(parse(table, query), args, expect);
    }

    private static void verify(RelationExpr expr, String... expect) throws Exception {
        verify(expr, new Object[0], expect);
    }

    @SuppressWarnings("unchecked")
    private static void verify(RelationExpr expr, Object[] args, String... expect)
        throws Exception
    {
        int i = 0;

        try (Scanner s = expr.makeCompiledQuery().newScanner(null, args)) {
            for (Object row = s.row(); row != null; row = s.step(row)) {
                String rowStr = row.toString();
                if (i < expect.length) {
                    assertEquals(rowStr, expect[i++]);
                } else {
                    fail("too many rows: " + rowStr);
                }
            }
        }

        assertTrue("not enough rows", i == expect.length);
    }

    /*
     * {id=1, name=name-1, value=value-1}
     * {id=2, name=name-2, value=null}
     * {id=3, name=name-3, value=value-3}
     * {id=4, name=name-4, value=null}
     * ...
     */
    private static Table<TestRow> fill(int num) throws Exception {
        Database db = Database.open(new DatabaseConfig());
        Table<TestRow> table = db.openTable(TestRow.class);

        for (int i=1; i<=num; i++) {
            var row = table.newRow();
            row.id(i);
            row.name("name-" + i);
            row.value((i & 1) == 0 ? null : ("value-" + i)); 
            table.insert(null, row);
        }

        return table;
    }

}
