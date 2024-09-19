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

import org.cojen.tupl.table.expr.QueryException;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class DeriveTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(QueryTest.class.getName());
    }

    private Database mDatabase;
    private Table<TestRow> mTable;

    @Before
    public void before() throws Exception {
        mDatabase = Database.open(new DatabaseConfig());
        mTable = mDatabase.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        if (mDatabase != null) {
            mDatabase.close();
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

        try (Scanner<DerivedRow3> s = derived.newScanner(null)) {
            DerivedRow3 row = s.row();
            assertEquals(1L, row.id());
            assertEquals(2L, row.a());
            assertEquals("hello", row.b());
            assertEquals("3", row.c());
        }
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
}
