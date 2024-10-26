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

import java.math.BigDecimal;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.QueryException;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class QueryTest {
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
    public void columnConversion() throws Exception {
        // Allow column type conversions, if it's guaranteed to never fail.

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.a(2);
            row.b("hello");
            row.c(null);
            row.d(null);
            mTable.insert(null, row);
        }

        Query<TestRow> query = mTable.query("{b=1, c=b+b, d=random()}");

        try (Scanner<TestRow> s = query.newScanner(null)) {
            TestRow row = s.row();

            try {
                row.a();
                fail();
            } catch (UnsetColumnException e) {
            }

            assertEquals("1", row.b());
            assertEquals((Long) 2L, row.c());

            BigDecimal bd = row.d();

            assertTrue(bd.compareTo(BigDecimal.ZERO) >= 0);
            assertTrue(bd.compareTo(BigDecimal.ONE) < 0);
        }

        try {
            mTable.query("{a='hello'}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("derives new or mismatched columns"));
        }

        try {
            mTable.query("{a=1L}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("derives new or mismatched columns"));
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

        @Nullable
        BigDecimal d();
        void d(BigDecimal d);
    }
}
