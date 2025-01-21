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

package org.cojen.tupl.table;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class DeriveTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(QueryTest.class.getName());
    }

    protected Database mDb;

    protected Table<TestRow> mTable;

    @Before
    public void before() throws Exception {
        createTempDb();
        mTable = mDb.openTable(TestRow.class);
    }

    protected void createTempDb() throws Exception {
        mDb = Database.open(new DatabaseConfig());
    }

    protected boolean isLocalTest() {
        return true;
    }

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
        }
    }

    @Test
    public void derivedType() throws Exception {
        // Very basic tests.

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.a(2);
            row.b("hello");
            row.c(3L);
            mTable.insert(null, row);
        }

        try {
            mTable.derive(DerivedRow1.class, "{*}");
            fail();
        } catch (QueryException e) {
            // Must be Nullable.
            assertTrue(e.getMessage().contains("Long c cannot be converted to String"));
        }

        try {
            mTable.derive(DerivedRow2.class, "{*}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("String b cannot be converted to int"));
        }

        Table<DerivedRow3> derived = mTable.derive(DerivedRow3.class, "{*}");
        assertEquals(DerivedRow3.class, derived.rowType());
        if (isLocalTest()) {
            assertTrue(isMapped(derived));
        }

        try (Scanner<DerivedRow3> s = derived.newScanner(null)) {
            DerivedRow3 row = s.row();

            assertEquals(1L, row.id());
            assertEquals(2L, row.a());
            assertEquals("hello", row.b());
            assertEquals("3", row.c());

            assertTrue(s.step() == null);
        }

        {
            DerivedRow3 row = derived.newRow();

            try {
                derived.load(null, row);
                fail();
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("Primary key isn't fully specified"));
            }

            try {
                derived.tryLoad(null, row);
                fail();
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("Primary key isn't fully specified"));
            }

            row.id(123);
            assertFalse(derived.tryLoad(null, row));

            try {
                derived.load(null, row);
                fail();
            } catch (NoSuchRowException e) {
            }

            row.id(1);
            derived.load(null, row);

            assertEquals("{id=1, a=2, b=hello, c=3}", row.toString());

            row.b("hello!!!");

            try {
                derived.store(null, row);
                fail();
            } catch (UnmodifiableViewException e) {
            }

            derived.update(null, row);

            {
                DerivedRow3 r = derived.newRow();
                r.id(1);
                derived.load(null, r);
                assertEquals("{id=1, a=2, b=hello!!!, c=3}", r.toString());
            }

            {
                TestRow r = mTable.newRow();
                r.id(1);
                r.c(333L);
                mTable.update(null, r);
            }

            row.b("hello");

            derived.merge(null, row);

            assertEquals("{id=1, a=2, b=hello, c=333}", row.toString());
        }

        Table<TestRow> notDerived = mTable.derive(TestRow.class, "{*}");
        assertEquals(TestRow.class, notDerived.rowType());
        if (isLocalTest()) {
            assertFalse(isMapped(notDerived));
        }

        derived = mTable.derive(DerivedRow3.class, "{id = id + 100}");

        try (Scanner<DerivedRow3> s = derived.newScanner(null)) {
            DerivedRow3 row = s.row();
            assertEquals("{id=101}", row.toString());
            assertTrue(s.step() == null);
        }

        try (Scanner<DerivedRow3> s = derived.newScanner(null, "id > ?", 100)) {
            DerivedRow3 row = s.row();
            assertEquals("{id=101}", row.toString());
            assertTrue(s.step() == null);
        }

        Table<Row> empty = mTable.derive(Row.class, "{}");
        try (Scanner<Row> s = empty.newScanner(null)) {
            Row row = s.row();
            assertEquals("{}", row.toString());
            assertTrue(s.step() == null);
        }

        Table<DerivedRowBig> big = mTable.derive
            (DerivedRowBig.class, "{a00=id, a05=5, a10=10, a16=16, a19=19}");
        try (Scanner<DerivedRowBig> s = big.newScanner(null)) {
            DerivedRowBig row = s.row();
            assertEquals("{a00=1, a05=5, a10=10, a16=16, a19=19}", row.toString());
            assertTrue(s.step() == null);
        }

        {
            TestRow row = mTable.newRow();
            row.id(2);
            row.a(12);
            row.b("world");
            row.c(13L);
            mTable.insert(null, row);
        }

        derived = mTable.derive(DerivedRow3.class, "{+id = id + 200, *}");

        try (Scanner<DerivedRow3> s = derived.newScanner(null)) {
            DerivedRow3 row = s.row();
            assertEquals("{id=201, a=2, b=hello, c=333}", row.toString());
            row = s.step(row);
            assertEquals("{id=202, a=12, b=world, c=13}", row.toString());
            assertTrue(s.step() == null);
        }
    }

    @Test
    public void automaticRowType() throws Exception {
        // Very basic tests.

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.a(2);
            row.b("hello");
            row.c(3L);
            mTable.insert(null, row);

            row.id(2);
            row.a(12);
            row.b("world");
            row.c(13L);
            mTable.insert(null, row);
        }

        Table<Row> derived = mTable.derive("{*}");

        assertTrue(derived.hasPrimaryKey());

        String[] pk = derived.rowType().getAnnotation(PrimaryKey.class).value();
        assertTrue(pk.length == 1);
        assertTrue("id".equals(pk[0]) || "+id".equals(pk[0]));

        try (Scanner<Row> s = derived.newScanner(null)) {
            Row row = s.row();
            assertEquals("{id=1, a=2, c=3, b=hello}", row.toString());
            row = s.step(row);
            assertEquals("{id=2, a=12, c=13, b=world}", row.toString());
            assertTrue(s.step() == null);
        }

        derived = mTable.derive("{id, sum = a + c} id > 1");

        try (Scanner<Row> s = derived.newScanner(null)) {
            Row row = s.row();
            assertEquals("{id=2, sum=25}", row.toString());
            assertTrue(s.step() == null);
        }
    }

    private static boolean isMapped(Table<?> table) {
        return table.getClass().getName().toLowerCase().contains("mapped");
    }

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        int a();
        void a(int a);

        String b();
        void b(String b);

        @Nullable
        Long c();
        void c(Long c);
    }

    @PrimaryKey("id")
    public interface DerivedRow1 {
        long id();
        void id(long id);

        long a();
        void a(long a);

        String b();
        void b(String b);

        String c();
        void c(String c);
    }

    @PrimaryKey("id")
    public interface DerivedRow2 {
        long id();
        void id(long id);

        long a();
        void a(long a);

        int b();
        void b(int b);

        @Nullable
        String c();
        void c(String c);
    }

    @PrimaryKey("id")
    public interface DerivedRow3 {
        long id();
        void id(long id);

        long a();
        void a(long a);

        String b();
        void b(String b);

        @Nullable
        String c();
        void c(String c);
    }

    public interface DerivedRowBig {
        long a00();
        void a00(long v);

        long a01();
        void a01(long v);

        long a02();
        void a02(long v);

        long a03();
        void a03(long v);

        long a04();
        void a04(long v);

        long a05();
        void a05(long v);

        long a06();
        void a06(long v);

        long a07();
        void a07(long v);

        long a08();
        void a08(long v);

        long a09();
        void a09(long v);

        long a10();
        void a10(long v);

        long a11();
        void a11(long v);

        long a12();
        void a12(long v);

        long a13();
        void a13(long v);

        long a14();
        void a14(long v);

        long a15();
        void a15(long v);

        long a16();
        void a16(long v);

        long a17();
        void a17(long v);

        long a18();
        void a18(long v);

        long a19();
        void a19(long v);
    }
}
