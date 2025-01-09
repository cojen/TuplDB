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

        Table<ConcatRow> concat = Table.concat(ConcatRow.class, mTable1, mTable2);

        Query<ConcatRow> query;
        QueryPlan plan;

        query = concat.query("{*} id != ?");
        plan = query.scannerPlan(null, 100_000);

        assertEquals("""
- concat
  - map: org.cojen.tupl.table.ConcatTest$ConcatRow
    - filter: id != ?1
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
        key columns: +id
  - map: org.cojen.tupl.table.ConcatTest$ConcatRow
    - filter: id != ?2
      - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
        key columns: +id
""", plan.toString()
        );

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
        plan = query.scannerPlan(null, 100_000);

        assertEquals("""
- merge
  - map: org.cojen.tupl.table.ConcatTest$ConcatRow
    - sort: +b
      - map: org.cojen.tupl.table.ConcatTest$ConcatRow
        - filter: id != ?1
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow1
            key columns: +id
  - map: org.cojen.tupl.table.ConcatTest$ConcatRow
    - map: org.cojen.tupl.table.ConcatTest$ConcatRow
      - sort: +b
        - filter: id != ?2
          - full scan over primary key: org.cojen.tupl.table.ConcatTest$TestRow2
            key columns: +id
""", plan.toString()
        );

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
