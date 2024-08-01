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

import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.Row;
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

    private List<Database> dbs = new ArrayList<>();

    @After
    public void teardown() throws Exception {
        for (Database db : dbs) {
            db.close();
        }
    }

    @PrimaryKey("id")
    public static interface TestRow {
        long id();
        void id(long id);

        int num();
        void num(int v);

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

    @Test
    public void iif() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = iif(1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("exactly 3 arguments"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = iif(1, 2, 3)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("boolean type"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = iif(true, 2, false)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("no common type"));
        }

        verify("{v = iif(true, 'hello', 'world')}", "{v=hello}");
        verify("{v = iif(false, 'hello', 'world')}", "{v=world}");
        verify("{v = iif(1 + 2 == 3, 10, 'world')}", "{v=10}");
        verify("{v = iif(1 + 2 == 3, null, 'world')}", "{v=null}");
        verify("{v = iif(1 + 2 == 3, 5, null) + 1}", "{v=6}");
        verify("{v = iif(1 + 2 == 3, null, 5) + 1}", "{v=null}");
        verify("{a=1} iif(1 + 2 == 3, 5, 6) == 5", "{a=1}");
        verify("{a=1} iif(1 + 2 != 3, 5, 6) == 5");

        verify("{v = iif(true, ? + 10, ? + 20)}", new Object[] {1, 2}, "{v=11}");
        verify("{v = iif(false, ? + 10, ? + 20)}", new Object[] {1, 2}, "{v=22}");
        verify("{v = iif(? == true, ? + 10, ? + 20)}", new Object[] {true, 1, 2}, "{v=11}");
        verify("{v = iif(? == false, ? + 10, ? + 20)}", new Object[] {true, 1, 2}, "{v=22}");

        Table<TestRow> table = fill(2);

        verify(table, "{id, v = iif(value == null, 'hello', value)}",
               "{id=1, v=value-1}", "{id=2, v=hello}");
        verify(table, "{id, +v = iif(value == null, 'hello', value)}",
               "{id=2, v=hello}", "{id=1, v=value-1}");
    }

    @Test
    public void random() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = random(1, 2, 3)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("at most 2 arguments"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = random(1, true)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("must be a number"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = random(99999999999999999999999999999)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("unsupported argument type"));
        }

        CompiledQuery q = parse
            ("{a = random(), b = random(), " + 
             "c = random(100), d = random(1000L), e = random(10f), f = random(-100, 10d) " +
             "}")
            .makeCompiledQuery();

        try (Scanner s = q.newScanner(null)) {
            var row = (Row) s.row();
            var rowStr = row.toString();

            double a = row.get_double("a");
            double b = row.get_double("b");

            assertTrue(rowStr, a != b); // extremely likely to be true
            assertTrue(rowStr, 0.0 <= a && a < 1.0);
            assertTrue(rowStr, 0.0 <= b && b < 1.0);

            int c = row.get_int("c");
            assertTrue(rowStr, 0 <= c && c < 100);

            long d = row.get_long("d");
            assertTrue(rowStr, 0 <= d && d < 1000);

            float e = row.get_float("e");
            assertTrue(rowStr, 0 <= e && e < 10f);

            double f = row.get_double("f");
            assertTrue(rowStr, -100d <= f && f < 10d);
        }

        q = parse("{a = random(? + 0) - 100}").makeCompiledQuery();

        try (Scanner s = q.newScanner(null, 100)) {
            var row = (Row) s.row();
            var rowStr = row.toString();
            int a = row.get_int("a");
            assertTrue(rowStr, -100 <= a && a < 0);
        }
    }

    @Test
    public void count() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = count(1, 2)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("at most 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = count()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("requires grouping"));
        }

        verify("{; v = count()}", "{v=1}");

        Table<TestRow> table = fill(4);

        verify(table, "{; v = count()}", "{v=4}");
        verify(table, "{; v = count(name)}", "{v=4}");
        verify(table, "{; v = count(value)}", "{v=2}");
        verify(table, "{value; v = count()}",
               "{value=value-1, v=1}", "{value=value-3, v=1}", "{value=null, v=2}");
        verify(table, "{value; +v = count(value)}",
               "{value=null, v=0}", "{value=value-1, v=1}", "{value=value-3, v=1}");

        verify(table, "{value; v = count(value, rows:..)}",
               "{value=value-1, v=1}", "{value=value-3, v=1}",
               "{value=null, v=0}", "{value=null, v=0}");

        verify(table, "{id = (id + 1) / 2; v = count(coalesce(value, 'hello'), rows:..0)}",
               "{id=1, v=1}", "{id=1, v=2}", "{id=2, v=1}", "{id=2, v=2}");

        verify(table, "{id = (id + 1) / 2; v = count(rows:0..)}",
               "{id=1, v=2}", "{id=1, v=1}", "{id=2, v=2}", "{id=2, v=1}");
    }

    @Test
    public void first() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = first()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("exactly 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = first(1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("requires grouping"));
        }

        Table<TestRow> table = fill(4);

        verify(table, "{; v = first(name)}", "{v=name-1}");

        verify(table, "{value; v = first(name)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-2}");

        verify(table, "{-value; v = first(name)}",
               "{value=null, v=name-2}",
               "{value=value-3, v=name-3}",
               "{value=value-1, v=name-1}");

        verify(table, "{value; v = first(name, rows:0..)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-2}",
               "{value=null, v=name-4}");

        verify(table, "{value; v = first(name, rows:..0)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-2}",
               "{value=null, v=name-2}");
    }

    @Test
    public void last() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = last()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("exactly 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = last(1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("requires grouping"));
        }

        Table<TestRow> table = fill(4);

        verify(table, "{; v = last(name)}", "{v=name-4}");

        verify(table, "{value; v = last(name)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-4}");

        verify(table, "{-value; v = last(name)}",
               "{value=null, v=name-4}",
               "{value=value-3, v=name-3}",
               "{value=value-1, v=name-1}");

        verify(table, "{value; v = last(name, rows:0..)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-4}",
               "{value=null, v=name-4}");

        verify(table, "{value; v = last(name, rows:..0)}",
               "{value=value-1, v=name-1}",
               "{value=value-3, v=name-3}",
               "{value=null, v=name-2}",
               "{value=null, v=name-4}");
    }

    @Test
    public void min() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = min()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("exactly 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = min(1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("requires grouping"));
        }

        Table<TestRow> table = fill(4);

        verify(table, "{; v = min(num)}", "{v=10}");

        verify(table, "{value; v = min(num)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=10}");

        verify(table, "{-value; v = min(num)}",
               "{value=null, v=10}",
               "{value=value-3, v=40}",
               "{value=value-1, v=20}");

        verify(table, "{value; v = min(num, rows:0..)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=10}",
               "{value=null, v=30}");

        verify(table, "{value; v = min(num, rows:..0)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=10}",
               "{value=null, v=10}");

        verify(table, "{value; v = min(num, rows:1..1)}",
               "{value=value-1, v=null}",
               "{value=value-3, v=null}",
               "{value=null, v=30}",
               "{value=null, v=null}");

        verify(table, "{value; v = min(num, rows:-1..-1)}",
               "{value=value-1, v=null}",
               "{value=value-3, v=null}",
               "{value=null, v=null}",
               "{value=null, v=10}");
    }

    @Test
    public void max() throws Exception {
        try {
            Parser.parse(IdentityTable.THE, "{v = max()}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("exactly 1 argument"));
        }

        try {
            Parser.parse(IdentityTable.THE, "{v = max(1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("requires grouping"));
        }

        Table<TestRow> table = fill(4);

        verify(table, "{; v = max(num)}", "{v=40}");

        verify(table, "{value; v = max(num)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=30}");

        verify(table, "{-value; v = max(num)}",
               "{value=null, v=30}",
               "{value=value-3, v=40}",
               "{value=value-1, v=20}");

        verify(table, "{value; v = max(num, rows:0..)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=30}",
               "{value=null, v=30}");

        verify(table, "{value; v = max(num, rows:..0)}",
               "{value=value-1, v=20}",
               "{value=value-3, v=40}",
               "{value=null, v=10}",
               "{value=null, v=30}");

        verify(table, "{value; v = max(num, rows:1..1)}",
               "{value=value-1, v=null}",
               "{value=value-3, v=null}",
               "{value=null, v=30}",
               "{value=null, v=null}");

        verify(table, "{value; v = max(num, rows:-1..-1)}",
               "{value=value-1, v=null}",
               "{value=value-3, v=null}",
               "{value=null, v=null}",
               "{value=null, v=10}");
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
                    assertEquals(expect[i++], rowStr);
                } else {
                    fail("too many rows: " + rowStr);
                }
            }
        }

        assertTrue("not enough rows", i == expect.length);
    }

    /*
     * {id=1, num=20, name=name-1, value=value-1}
     * {id=2, num=10, name=name-2, value=null}
     * {id=3, num=40, name=name-3, value=value-3}
     * {id=4, num=30, name=name-4, value=null}
     * ...
     */
    private Table<TestRow> fill(int num) throws Exception {
        Database db = Database.open(new DatabaseConfig());
        dbs.add(db);
        Table<TestRow> table = db.openTable(TestRow.class);

        for (int i=1; i<=num; i++) {
            var row = table.newRow();
            row.id(i);
            row.num(((i & 1) == 0 ? (i - 1) : (i + 1)) * 10);
            row.name("name-" + i);
            row.value((i & 1) == 0 ? null : ("value-" + i));
            table.insert(null, row);
        }

        return table;
    }

}
