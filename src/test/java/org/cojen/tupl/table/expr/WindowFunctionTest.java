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

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(WindowFunctionTest.class.getName());
    }

    @PrimaryKey("id")
    public static interface TestRow extends Row {
        long id();
        void id(long id);

        int a();
        void a(int a);

        int b();
        void b(int b);

        int c();
        void c(int c);

        int d();
        void d(int d);
    }

    private Database mDb;
    private Table<TestRow> mTable;

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
        }
    }

    private void fill() throws Exception {
        fill(1);
    }

    private void fill(int scalar) throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = mDb.openTable(TestRow.class);

        int[][] data = {
            {1,  1 * scalar,  3,  1,  1},
            {2,  1 * scalar, -3, -3,  0},
            {3,  1 * scalar, -3,  2,  4},
            {4,  1 * scalar,  0, -3, -3},
            {5,  1 * scalar,  2, -3,  2},
            {6,  2 * scalar,  4, -1,  1},
            {7,  2 * scalar,  1,  0,  4},
            {8,  2 * scalar,  1, -3,  0},
            {9,  2 * scalar,  4, -2,  3},
            {10, 2 * scalar, -4, -1,  0},
            {11, 3 * scalar,  3,  1,  4},
            {12, 3 * scalar,  2,  0,  4},
            {13, 3 * scalar,  0, -3, -1},
            {14, 3 * scalar,  0, -3,  3},
            {15, 3 * scalar,  1, -4,  2},
            {16, 4 * scalar,  3, -3, -1},
            {17, 4 * scalar,  0, -4,  1},
            {18, 4 * scalar, -4, -3,  2},
            {19, 4 * scalar,  4, -4,  3},
            {20, 4 * scalar,  4, -3, -1},
        };

        for (int[] dataRow : data) {
            TestRow row = mTable.newRow();
            row.id(dataRow[0]);
            row.a(dataRow[1]);
            row.b(dataRow[2]);
            row.c(dataRow[3]);
            row.d(dataRow[4]);
            mTable.insert(null, row);
        }
    }

    @Test
    public void frameRowsVariableRange() throws Exception {
        fill();

        try {
            mTable.derive("{a; c = count(b, rows:c..d), s = sum(b, rows:c..d)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains
                       ("depends on an expression which accumulates group results"));
        }

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,   -3,   -3,  -3,  1, -3.0},
                {1,   -3,    3,   0,  2,  0.0},
                {1,    2,    2,   2,  1,  2.0},
                {1,    3,    3,   3,  1,  3.0},
                {1,   -3,    2,  -4,  4, -1.0},
                {2,    1,    4,   5,  2,  2.5},
                {2,   -4,    4,   2,  4,  0.5},
                {2,    1,    4,   6,  3,  2.0},
                {2,   -4,    4,   2,  4,  0.5},
                {2,   -4,    4,   0,  2,  0.0},
                {3,    0,    2,   3,  4,  0.75},
                {3,    0,    2,   3,  4,  0.75},
                {3,    2,    3,   5,  2,  2.5},
                {3,    0,    3,   6,  5,  1.2},
                {3,    0,    3,   6,  5,  1.2},
                {4, null, null,   0,  0, null},
                {4,   -4,    3,  -1,  3, -0.3333333333333333},
                {4,   -4,    4,   7,  5,  1.4},
                {4,   -4,    4,   7,  5,  1.4},
                {4,   -4,    4,   0,  3,  0.0},
            };

            String query = "{a; min = min(b, rows:c..d), max = max(b, rows:c..d), " +
                "sum = sum(b, rows:c..d), cnt = count(b, rows:c..d), avg = avg(b, rows:c..d)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,   -3,   3,   0,   2,  0.0},
                {1,   -3,  -3,  -3,   1, -3.0},
                {1,   -3,   2,  -1,   3, -0.3333333333333333},
                {1,    0,   2,   2,   2,  1.0},
                {1,    2,   2,   2,   1,  2.0},
                {2,    1,   4,   5,   2,  2.5},
                {2,   -4,   4,   2,   4,  0.5},
                {2,    1,   1,   1,   1,  1.0},
                {2,   -4,   4,   0,   2,  0.0},
                {2,   -4,  -4,  -4,   1, -4.0},
                {3,    0,   3,   6,   5,  1.2},
                {3,    0,   2,   3,   4,  0.75},
                {3,    0,   0,   0,   2,  0.0},
                {3,    0,   1,   1,   2,  0.5},
                {3,    1,   1,   1,   1,  1.0},
                {4,    0,   3,   3,   2,  1.5},
                {4,   -4,   0,  -4,   2, -2.0},
                {4,   -4,   4,   4,   3,  1.3333333333333333},
                {4,    4,   4,   8,   2,  4.0},
                {4,    4,   4,   4,   1,  4.0},
            };

            String query = "{a; ~e = iif(d >= 0, d, -d), " +
                "min = min(b, rows:iif(true, 0, 0)..e), max = max(b, rows:0..e), " +
                "sum = sum(b, rows:0..e), cnt = count(b, rows:0..e), avg = avg(b, rows:0..e)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,    3,   3,   3,   1,  3.0},
                {1,   -3,   3,   0,   2,  0.0},
                {1,   -3,   3,  -3,   3, -1.0},
                {1,   -3,   3,  -3,   4, -0.75},
                {1,   -3,   2,  -4,   4, -1.0},
                {2,    4,   4,   4,   1,  4.0},
                {2,    1,   1,   1,   1,  1.0},
                {2,    1,   4,   6,   3,  2.0},
                {2,    1,   4,   6,   3,  2.0},
                {2,   -4,   4,   0,   2,  0.0},
                {3,    3,   3,   3,   1,  3.0},
                {3,    2,   2,   2,   1,  2.0},
                {3,    0,   3,   5,   3,  1.6666666666666667},
                {3,    0,   3,   5,   4,  1.25},
                {3,    0,   3,   6,   5,  1.2},
                {4,    3,   3,   3,   1,  3.0},
                {4,    0,   3,   3,   2,  1.5},
                {4,   -4,   3,  -1,   3, -0.3333333333333333},
                {4,   -4,   4,   3,   4,  0.75},
                {4,   -4,   4,   4,   4,  1.0},
            };

            String query = "{a; ~s = iif(c <= 0, c, -c), " +
                "min = min(b, rows:s..iif(true, 0, 0)), max = max(b, rows:s..0), " +
                "sum = sum(b, rows:s..0), cnt = count(b, rows:s..0), avg = avg(b, rows:s..0)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, v1, v2, v3, v4,  v5
                {1,    1, -3,  0, -4,  0},
                {1,    0,  0, -2,  0,  0},
                {1,    0,  0, -6,  0,  4},
                {1,    2,  3,  0, -3,  0},
                {1,    3,  0,  0,  0,  0},
                {2,    8,  0,  0, -6,  0},
                {2,    4,  0, -6,  0,  0},
                {2,    8,  0,  0, -5,  0},
                {2,    7,  0,  0,  0,  0},
                {2,    0,  0, -7,  0,  8},
                {3,    6,  0, -7,  0,  0},
                {3,    6,  0, -7,  0,  0},
                {3,    7,  0,  0, -3,  0},
                {3,   10,  0, -7,  0,  0},
                {3,   12,  0,  0,  0,  0},
                {4,    5,  0,  0, -14, 0},
                {4,    0,  0, -7,  0,  0},
                {4,    0,  0, -17, 0,  0},
                {4,    4,  0,  0,  0,  0},
                {4,    5,  0,  0, -7,  0},
            };

            String query = "{a; v1 = sum(d, rows:c..b), v2 = sum(b, rows:d..c), " +
                "v3 = sum(c, rows:b..d), v4 = sum(c, rows:d..b), v5 = sum(d, rows:b..c)}";

            verify2(expect, query);
        }

        {
            Object[][] expect = {
                // a, v1, v2, v3, v4, v5
                {1, 0.3333333333333333, -3.0, null, -1.3333333333333333, null},
                {1, null, null, -1.0, null, null},
                {1, null, null, -1.2, null, 0.8},
                {1, 0.5, 3.0, null, -0.75, null},
                {1, 0.75, null, null, null, null},
                {2, 1.6, null, null, -1.5, null},
                {2, 2.0, null, -2.0, null, null},
                {2, 2.0, null, null, -2.5, null},
                {2, 1.75, null, null, null, null},
                {2, null, null, -1.4, null, 2.0},
                {3, 2.0, null, -3.5, null, null},
                {3, 2.0, null, -3.5, null, null},
                {3, 2.3333333333333335, null, null, -1.5, null},
                {3, 2.5, null, -3.5, null, null},
                {3, 2.4, null, null, null, null},
                {4, 1.25, null, null, -3.5, null},
                {4, 0.0, null, -3.5, null, null},
                {4, null, null, -3.4, null, null},
                {4, 0.8, null, null, null, null},
                {4, 1.25, null, null, -3.5, null},
            };

            String query = "{a; v1 = avg(d, rows:c..b), v2 = avg(b, rows:d..c), " +
                "v3 = avg(c, rows:b..d), v4 = avg(c, rows:d..b), v5 = avg(d, rows:b..c)}";

            verify3(expect, query);
        }
    }

    @Test
    public void frameGroups() throws Exception {
        fill();

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,    1,   1,   5,   5,  1.0},
                {1,    1,   1,   5,   5,  1.0},
                {1,    1,   1,   5,   5,  1.0},
                {1,    1,   1,   5,   5,  1.0},
                {1,    1,   1,   5,   5,  1.0},
                {2,    2,   2,  10,   5,  2.0},
                {2,    2,   2,  10,   5,  2.0},
                {2,    2,   2,  10,   5,  2.0},
                {2,    2,   2,  10,   5,  2.0},
                {2,    2,   2,  10,   5,  2.0},
                {3,    3,   3,  15,   5,  3.0},
                {3,    3,   3,  15,   5,  3.0},
                {3,    3,   3,  15,   5,  3.0},
                {3,    3,   3,  15,   5,  3.0},
                {3,    3,   3,  15,   5,  3.0},
                {4,    4,   4,  20,   5,  4.0},
                {4,    4,   4,  20,   5,  4.0},
                {4,    4,   4,  20,   5,  4.0},
                {4,    4,   4,  20,   5,  4.0},
                {4,    4,   4,  20,   5,  4.0},
            };

            String query = "{; +a, min = min(a, groups:0..0), max = max(a, groups:0..0), " +
                "sum = sum(a, groups:0..0), cnt = count(a, groups:0..0), " +
                "avg = avg(a, groups:0..0)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
            };

            String query = "{; +a, min = min(a, groups:-1..1), max = max(a, groups:-1..1), " +
                "sum = sum(a, groups:-1..1), cnt = count(a, groups:-1..1), " +
                "avg = avg(a, groups:-1..1)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min, max, sum, cnt, avg
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {1,    1,   2,  15,  10,  1.5},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {2,    1,   3,  30,  15,  2.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {3,    2,   4,  45,  15,  3.0},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
                {4,    3,   4,  35,  10,  3.5},
            };

            String query = "{; +a, ~s=-1, ~e=1, " +
                "min = min(a, groups:s..e), max = max(a, groups:s..e), " +
                "sum = sum(a, groups:s..e), cnt = count(a, groups:s..e), " +
                "avg = avg(a, groups:s..e)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min,  max,  sum, cnt, avg
                {1,   null, null,  0,   0,  null},
                {1,   null, null,  0,   0,  null},
                {1,   null, null,  0,   0,  null},
                {1,   null, null,  0,   0,  null},
                {1,   null, null,  0,   0,  null},
                {2,    1,    1,    5,   5,  1.0},
                {2,    1,    1,    5,   5,  1.0},
                {2,    1,    1,    5,   5,  1.0},
                {2,    1,    1,    5,   5,  1.0},
                {2,    1,    1,    5,   5,  1.0},
                {3,    1,    2,   15,  10,  1.5},
                {3,    1,    2,   15,  10,  1.5},
                {3,    1,    2,   15,  10,  1.5},
                {3,    1,    2,   15,  10,  1.5},
                {3,    1,    2,   15,  10,  1.5},
                {4,    2,    3,   25,  10,  2.5},
                {4,    2,    3,   25,  10,  2.5},
                {4,    2,    3,   25,  10,  2.5},
                {4,    2,    3,   25,  10,  2.5},
                {4,    2,    3,   25,  10,  2.5},
            };

            String query = "{; +a, min = min(a, groups:-2..-1), max = max(a, groups:-2..-1), " +
                "sum = sum(a, groups:-2..-1), cnt = count(a, groups:-2..-1), " +
                "avg = avg(a, groups:-2..-1)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                // a, min,  max,  sum, cnt, avg
                {1,    2,    3,   25,  10,  2.5},
                {1,    2,    3,   25,  10,  2.5},
                {1,    2,    3,   25,  10,  2.5},
                {1,    2,    3,   25,  10,  2.5},
                {1,    2,    3,   25,  10,  2.5},
                {2,    3,    4,   35,  10,  3.5},
                {2,    3,    4,   35,  10,  3.5},
                {2,    3,    4,   35,  10,  3.5},
                {2,    3,    4,   35,  10,  3.5},
                {2,    3,    4,   35,  10,  3.5},
                {3,    4,    4,   20,   5,  4.0},
                {3,    4,    4,   20,   5,  4.0},
                {3,    4,    4,   20,   5,  4.0},
                {3,    4,    4,   20,   5,  4.0},
                {3,    4,    4,   20,   5,  4.0},
                {4,   null, null,  0,   0,  null},
                {4,   null, null,  0,   0,  null},
                {4,   null, null,  0,   0,  null},
                {4,   null, null,  0,   0,  null},
                {4,   null, null,  0,   0,  null},
            };

            String query = "{; +a, min = min(a, groups:1..2), max = max(a, groups:1..2), " +
                "sum = sum(a, groups:1..2), cnt = count(a, groups:1..2), " +
                "avg = avg(a, groups:1..2)}";

            verify(expect, query);
        }
    }

    @Test
    public void frameRanges() throws Exception {
        fill(10);

        try {
            mTable.derive("{a; x = count(b, range:-1..1)}");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("range frame type requires ordered values"));
        }

        {
            Object[][] expect = {
                //a, min,max,sum,cnt,avg
                {10, 10, 10, 50,  5, 10.0},
                {10, 10, 10, 50,  5, 10.0},
                {10, 10, 10, 50,  5, 10.0},
                {10, 10, 10, 50,  5, 10.0},
                {10, 10, 10, 50,  5, 10.0},
                {20, 20, 20, 100, 5, 20.0},
                {20, 20, 20, 100, 5, 20.0},
                {20, 20, 20, 100, 5, 20.0},
                {20, 20, 20, 100, 5, 20.0},
                {20, 20, 20, 100, 5, 20.0},
                {30, 30, 30, 150, 5, 30.0},
                {30, 30, 30, 150, 5, 30.0},
                {30, 30, 30, 150, 5, 30.0},
                {30, 30, 30, 150, 5, 30.0},
                {30, 30, 30, 150, 5, 30.0},
                {40, 40, 40, 200, 5, 40.0},
                {40, 40, 40, 200, 5, 40.0},
                {40, 40, 40, 200, 5, 40.0},
                {40, 40, 40, 200, 5, 40.0},
                {40, 40, 40, 200, 5, 40.0},
            };

            String query = "{; +a, min = min(a, range:0..0), max = max(a, range:0..0), " +
                "sum = sum(a, range:0..0), cnt = count(a, range:0..0), " +
                "avg = avg(a, range:0..0)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
            };

            String query = "{; +a, min = min(a, range:-15..15), max = max(a, range:-15..14.5), " +
                "sum = sum(a, range:-12.1..15), cnt = count(a, range:-15..15), " +
                "avg = avg(a, range:-15..15)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
            };

            String query = "{; -a, min = min(a, range:-15..15), max = max(a, range:-15..14.5), " +
                "sum = sum(a, range:-12.1..15), cnt = count(a, range:-15..15), " +
                "avg = avg(a, range:-15..15)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
            };

            String query = "{; +a, ~s=-10, ~e=15, " +
                "min = min(a, range:s..e), max = max(a, range:s..e), " +
                "sum = sum(a, range:s..e), cnt = count(a, range:s..e), " +
                "avg = avg(a, range:s..e)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,  max,  sum, cnt, avg
                {10, null, null,   0,  0,  null},
                {10, null, null,   0,  0,  null},
                {10, null, null,   0,  0,  null},
                {10, null, null,   0,  0,  null},
                {10, null, null,   0,  0,  null},
                {20, 10,   10,    50,  5,  10.0},
                {20, 10,   10,    50,  5,  10.0},
                {20, 10,   10,    50,  5,  10.0},
                {20, 10,   10,    50,  5,  10.0},
                {20, 10,   10,    50,  5,  10.0},
                {30, 10,   20,   150, 10,  15.0},
                {30, 10,   20,   150, 10,  15.0},
                {30, 10,   20,   150, 10,  15.0},
                {30, 10,   20,   150, 10,  15.0},
                {30, 10,   20,   150, 10,  15.0},
                {40, 20,   30,   250, 10,  25.0},
                {40, 20,   30,   250, 10,  25.0},
                {40, 20,   30,   250, 10,  25.0},
                {40, 20,   30,   250, 10,  25.0},
                {40, 20,   30,   250, 10,  25.0},
            };

            String query = "{; +a, min = min(a, range:-20..-8), max = max(a, range:-20..-8), " +
                "sum = sum(a, range:-20..-8), cnt = count(a, range:-20..-8), " +
                "avg = avg(a, range:-20..-8)}";

            verify(expect, query);
        }


        {
            Object[][] expect = {
                //a, min,  max,  sum, cnt, avg
                {10, 20,   30,   250, 10,  25.0},
                {10, 20,   30,   250, 10,  25.0},
                {10, 20,   30,   250, 10,  25.0},
                {10, 20,   30,   250, 10,  25.0},
                {10, 20,   30,   250, 10,  25.0},
                {20, 30,   40,   350, 10,  35.0},
                {20, 30,   40,   350, 10,  35.0},
                {20, 30,   40,   350, 10,  35.0},
                {20, 30,   40,   350, 10,  35.0},
                {20, 30,   40,   350, 10,  35.0},
                {30, 40,   40,   200,  5,  40.0},
                {30, 40,   40,   200,  5,  40.0},
                {30, 40,   40,   200,  5,  40.0},
                {30, 40,   40,   200,  5,  40.0},
                {30, 40,   40,   200,  5,  40.0},
                {40, null, null,  0,   0,  null},
                {40, null, null,  0,   0,  null},
                {40, null, null,  0,   0,  null},
                {40, null, null,  0,   0,  null},
                {40, null, null,  0,   0,  null},
            };

            String query = "{; +a, min = min(a, range:10..20), max = max(a, range:10..20), " +
                "sum = sum(a, range:10..20), cnt = count(a, range:10..20), " +
                "avg = avg(a, range:10..20)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
            };

            String query = "{; +a, min = min(a, range:-15..), max = max(a, range:-15..), " +
                "sum = sum(a, range:-12.1..), cnt = count(a, range:-15..), " +
                "avg = avg(a, range:-15..)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {40, 30, 40, 350, 10, 35.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {30, 20, 40, 450, 15, 30.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {20, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
                {10, 10, 40, 500, 20, 25.0},
            };

            String query = "{; -a, min = min(a, range:-15..), max = max(a, range:-15..), " +
                "sum = sum(a, range:-12.1..), cnt = count(a, range:-15..), " +
                "avg = avg(a, range:-15..)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
            };

            String query = "{; +a, min = min(a, range:..15), max = max(a, range:..14.5), " +
                "sum = sum(a, range:..15), cnt = count(a, range:..15), " +
                "avg = avg(a, range:..15)}";

            verify(expect, query);
        }

        {
            Object[][] expect = {
                //a, min,max,sum, cnt,avg
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {40, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {30, 10, 40, 500, 20, 25.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {20, 10, 30, 300, 15, 20.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
                {10, 10, 20, 150, 10, 15.0},
            };

            String query = "{; -a, min = min(a, range:..15), max = max(a, range:..14.5), " +
                "sum = sum(a, range:..15), cnt = count(a, range:..15), " +
                "avg = avg(a, range:..15)}";

            verify(expect, query);
        }
    }

    @Test
    public void noGrouping() throws Exception {
        fill();

        {
            String query = "{;}";

            int num = 0;
            try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
                for (Row row = s.row(); row != null; row = s.step(row)) {
                    assertEquals("{}", row.toString());
                    num++;
                }
            }

            assertEquals(20, num);
        }

        {
            String query = "{; q = 1, ~b = random()} iif(b == 0, true, true)";

            int num = 0;
            try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
                for (Row row = s.row(); row != null; row = s.step(row)) {
                    assertEquals("{q=1}", row.toString());
                    num++;
                }
            }

            assertEquals(20, num);
        }
    }

    private void dump(String query) throws Exception {
        try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
            for (Row row = s.row(); row != null; row = s.step(row)) {
                System.out.print('{');
                System.out.print(row.get_int("a") + ", ");
                System.out.print(row.getInteger("min") + ", ");
                System.out.print(row.getInteger("max") + ", ");
                System.out.print(row.getInteger("sum") + ", ");
                System.out.print(row.getInteger("cnt") + ", ");
                System.out.print(row.getDouble("avg"));
                System.out.println("},");
            }
        }
    }

    private void verify(Object[][] expect, String query) throws Exception {
        int i = 0;

        try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
            for (Row row = s.row(); row != null; row = s.step(row)) {
                Object[] expectRow = expect[i++];
                assertEquals(expectRow[0], row.get_int("a"));
                assertEquals(expectRow[1], row.getInteger("min"));
                assertEquals(expectRow[2], row.getInteger("max"));
                assertEquals(expectRow[3], row.get_int("sum"));
                assertEquals(expectRow[4], row.get_int("cnt"));
                assertEquals(expectRow[5], row.getDouble("avg"));
            }
        }

        assertEquals(i, expect.length);
    }

    private void verify2(Object[][] expect, String query) throws Exception {
        int i = 0;

        try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
            for (Row row = s.row(); row != null; row = s.step(row)) {
                Object[] expectRow = expect[i++];
                assertEquals(expectRow[0], row.get_int("a"));
                assertEquals(expectRow[1], row.get_int("v1"));
                assertEquals(expectRow[2], row.get_int("v2"));
                assertEquals(expectRow[3], row.get_int("v3"));
                assertEquals(expectRow[4], row.get_int("v4"));
                assertEquals(expectRow[5], row.get_int("v5"));
            }
        }

        assertEquals(i, expect.length);
    }

    private void verify3(Object[][] expect, String query) throws Exception {
        int i = 0;

        try (Scanner<Row> s = mTable.derive(query).newScanner(null)) {
            for (Row row = s.row(); row != null; row = s.step(row)) {
                Object[] expectRow = expect[i++];
                assertEquals(expectRow[0], row.get_int("a"));
                assertEquals(expectRow[1], row.getDouble("v1"));
                assertEquals(expectRow[2], row.getDouble("v2"));
                assertEquals(expectRow[3], row.getDouble("v3"));
                assertEquals(expectRow[4], row.getDouble("v4"));
                assertEquals(expectRow[5], row.getDouble("v5"));
            }
        }

        assertEquals(i, expect.length);
    }
}
