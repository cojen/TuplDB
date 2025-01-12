/*
 *  Copyright (C) 2025 Cojen.org
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

import java.util.regex.Matcher;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ConcatTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ConcatTest.class.getName());
    }

    private Database mDatabase;
    private Table<TestRow1> mTable1;
    private Table<TestRow2> mTable2;

    @Before
    public void before() throws Exception {
        mDatabase = Database.open(new DatabaseConfig());
        mTable1 = mDatabase.openTable(TestRow1.class);
        mTable2 = mDatabase.openTable(TestRow2.class);
    }

    @After
    public void teardown() throws Exception {
        if (mDatabase != null) {
            mDatabase.close();
        }
    }

    @Test
    public void basic() throws Exception {
        basic(false);
    }

    @Test
    public void basicAutoType() throws Exception {
        basic(true);
    }

    private void basic(boolean autoType) throws Exception {
        for (int i=1; i<=2; i++) {
            TestRow1 row = mTable1.newRow();
            row.id(i);
            row.a(10 + i);
            row.b(100 - (i * 2));
            row.c(100L * i);
            mTable1.insert(null, row);
        }

        for (int i=1; i<=3; i++) {
            TestRow2 row = mTable2.newRow();
            row.id(i);
            row.b("" + (100 - (i * 2 + 1)));
            row.d("" + (100L * i));
            mTable2.insert(null, row);
        }

        Table<?> concat;
        if (!autoType) {
            concat = Table.concat(ConcatRow.class, mTable1, mTable2);
        } else {
            concat = Table.concat(mTable1, mTable2);
        }

        Query<?> query;
        String plan, expect;

        query = concat.query("{*} id != ?");
        plan = query.scannerPlan(null, 100_000).toString();

        expect = """
- concat
  - map: TARGET
    - filter: id != ?1
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
        key columns: +id
  - map: TARGET
    - filter: id != ?2
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
        key columns: +id
""";

        expect = expect.replaceAll("TARGET", Matcher.quoteReplacement(concat.rowType().getName()));

        assertEquals(expect, plan);

        try (var s = query.newScanner(null, 100_000)) {
            verify(s, new String[] {
                    "{a=11, id=1, c=100, b=98, d=}",
                    "{a=12, id=2, c=200, b=96, d=}",
                    "{a=0, id=1, c=null, b=97, d=100}",
                    "{a=0, id=2, c=null, b=95, d=200}",
                    "{a=0, id=3, c=null, b=93, d=300}",
                   });
        }

        try (var s = query.newScanner(null, 2)) {
            verify(s, new String[] {
                    "{a=11, id=1, c=100, b=98, d=}",
                    "{a=0, id=1, c=null, b=97, d=100}",
                    "{a=0, id=3, c=null, b=93, d=300}",
                   });
        }

        query = concat.query("{a, +b, c, d='dee'} id != ?");
        plan = query.scannerPlan(null, 100_000).toString();

        expect = """
- merge
  - map: TARGET
    - sort: +b
      - map: TARGET
        - filter: id != ?1
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
            key columns: +id
  - map: TARGET
    - map: TARGET
      - sort: +b
        - filter: id != ?2
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
            key columns: +id
""";

        expect = expect.replaceAll("TARGET", Matcher.quoteReplacement(concat.rowType().getName()));

        assertEquals(expect, plan);

        try (var s = query.newScanner(null, 100_000)) {
            verify(s, new String[] {
                    "{a=0, c=null, b=93, d=dee}",
                    "{a=0, c=null, b=95, d=dee}",
                    "{a=12, c=200, b=96, d=dee}",
                    "{a=0, c=null, b=97, d=dee}",
                    "{a=11, c=100, b=98, d=dee}",
                   });
        }

        try (var s = query.newScanner(null, 2)) {
            verify(s, new String[] {
                    "{a=0, c=null, b=93, d=dee}",
                    "{a=0, c=null, b=97, d=dee}",
                    "{a=11, c=100, b=98, d=dee}",
                   });
        }
    }

    @Test
    public void concatNoneAuto() throws Exception {
        Table<Row> empty = Table.concat();
        assertTrue(empty.isEmpty());
        Row r = empty.newRow();
        try {
            empty.store(null, r);
            fail();
        } catch (UnmodifiableViewException e) {
        }
    }

    @Test
    public void concatOneAuto() throws Exception {
        Table<Row> table = Table.concat(mTable1);

        Row r = table.newRow();
        r.set("id", 1);
        r.set("a", 2);
        r.set("b", 3);
        r.set("c", 4);
        table.insert(null, r);

        TestRow1 r2 = mTable1.newRow();
        r2.id(1);
        mTable1.load(null, r2);
        assertEquals("{id=1, a=2, b=3, c=4}", r2.toString());
    }

    @Test
    public void concatMany() throws Exception {
        for (int i=1; i<=2; i++) {
            TestRow1 row = mTable1.newRow();
            row.id(i);
            row.a(10 + i);
            row.b(100 - (i * 2));
            row.c(100L * i);
            mTable1.insert(null, row);
        }

        for (int i=1; i<=3; i++) {
            TestRow2 row = mTable2.newRow();
            row.id(i);
            row.b("" + (100 - (i * 2 + 1)));
            row.d("" + (100L * i));
            mTable2.insert(null, row);
        }

        Table<?> concat = Table.concat(mTable1, mTable2);
        concat = Table.concat(concat, mTable1, mTable2);

        Query<?> query;
        String plan, expect;

        query = concat.query("{*} id != ?");
        plan = query.scannerPlan(null, 100_000).toString();

        expect = """
- concat
  - map: TARGET
    - filter: id != ?1
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
        key columns: +id
  - map: TARGET
    - filter: id != ?2
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
        key columns: +id
  - map: TARGET
    - filter: id != ?1
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
        key columns: +id
  - map: TARGET
    - filter: id != ?2
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
        key columns: +id
""";

        expect = expect.replaceAll("TARGET", Matcher.quoteReplacement(concat.rowType().getName()));

        assertEquals(expect, plan);

        try (var s = query.newScanner(null, 100_000)) {
            verify(s, new String[] {
                    "{a=11, id=1, c=100, b=98, d=}",
                    "{a=12, id=2, c=200, b=96, d=}",
                    "{a=0, id=1, c=null, b=97, d=100}",
                    "{a=0, id=2, c=null, b=95, d=200}",
                    "{a=0, id=3, c=null, b=93, d=300}",
                    "{a=11, id=1, c=100, b=98, d=}",
                    "{a=12, id=2, c=200, b=96, d=}",
                    "{a=0, id=1, c=null, b=97, d=100}",
                    "{a=0, id=2, c=null, b=95, d=200}",
                    "{a=0, id=3, c=null, b=93, d=300}",
                   });
        }
        query = concat.query("{a, +b, c = c / 100, d} id != ?");
        plan = query.scannerPlan(null, 100_000).toString();

        expect = """
- merge
  - map: TARGET
    - sort: +b
      - map: TARGET
        - filter: id != ?1
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
            key columns: +id
  - map: TARGET
    - map: TARGET
      - sort: +b
        - filter: id != ?2
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
            key columns: +id
  - map: TARGET
    - sort: +b
      - map: TARGET
        - filter: id != ?1
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
            key columns: +id
  - map: TARGET
    - map: TARGET
      - sort: +b
        - filter: id != ?2
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
            key columns: +id
""";

        expect = expect.replaceAll("TARGET", Matcher.quoteReplacement(concat.rowType().getName()));

        assertEquals(expect, plan);

        try (var s = query.newScanner(null, 100_000)) {
            verify(s, new String[] {
                    "{a=0, c=null, b=93, d=300}",
                    "{a=0, c=null, b=93, d=300}",
                    "{a=0, c=null, b=95, d=200}",
                    "{a=0, c=null, b=95, d=200}",
                    "{a=12, c=2, b=96, d=}",
                    "{a=12, c=2, b=96, d=}",
                    "{a=0, c=null, b=97, d=100}",
                    "{a=0, c=null, b=97, d=100}",
                    "{a=11, c=1, b=98, d=}",
                    "{a=11, c=1, b=98, d=}",
                   });
        }
    }

    private void verify(Scanner<?> s, String[] expect) throws Exception {
        Object current = null;

        for (int i=0; i<expect.length; i++) {
            Object r = s.row();
            assertEquals(expect[i], r.toString());
            if (current != null) {
                assertSame(current, r);
            }
            current = s.step();
        }

        assertNull(s.step());
    }

    @PrimaryKey("id")
    public interface TestRow1 {
        long id();
        void id(long id);

        int a();
        void a(int a);

        int b();
        void b(int b);

        @Nullable
        Long c();
        void c(Long c);
    }

    @PrimaryKey("id")
    public interface TestRow2 {
        int id();
        void id(int id);

        String b();
        void b(String b);

        String d();
        void d(String d);
    }

    public interface ConcatRow {
        long id();
        void id(long id);

        int a();
        void a(int a);

        String b();
        void b(String b);

        @Nullable
        Long c();
        void c(Long c);

        String d();
        void d(String d);
    }
}
