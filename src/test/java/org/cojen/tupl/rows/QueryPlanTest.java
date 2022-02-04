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

import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class QueryPlanTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(QueryPlanTest.class.getName());
    }

    private Database mDatabase;
    private Table<TestRow> mTable, mIndexA, mIndexB, mIndexC;

    @Before
    public void before() throws Exception {
        mDatabase = Database.open(new DatabaseConfig());
        mTable = mDatabase.openTable(TestRow.class);

        mIndexA = mTable.viewAlternateKey("a");
        mIndexB = mTable.viewSecondaryIndex("b");

        try {
            mIndexC = mTable.viewSecondaryIndex("c");
            fail();
        } catch (IllegalStateException e) {
        }

        mIndexC = mTable.viewSecondaryIndex("c", "b");
    }

    @Test
    public void primaryKey() throws Exception {
        QueryPlan plan = mTable.queryPlan("id == ?0 && id != ?0");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = mTable.queryPlan("id == ?0 && id != ?0 && a == ?");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = mTable.queryPlan(null);
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false),
                     plan);

        plan = mTable.queryPlan("id == ?0 || id != ?0");
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false),
                     plan);

        plan = mTable.queryPlan("id < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, null, "id < ?0"),
                     plan);

        plan = mTable.queryPlan("id >= ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, "id >= ?0", null),
                     plan);

        plan = mTable.queryPlan("id >= ? && id < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, "id >= ?0", "id < ?1"),
                     plan);

        // This test should fail when LoadOne is supported.
        plan = mTable.queryPlan("id == ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, "id >= ?0", "id <= ?0"),
                     plan);

        plan = mTable.queryPlan("a == ?");
        assertEquals(new QueryPlan.Filter
                     ("a == ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false)),
                     plan);

        plan = mTable.queryPlan("id == ?0 && id != ?0 || b == ?1");
        assertEquals(new QueryPlan.Filter
                     ("b == ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false)),
                     plan);

        plan = mTable.queryPlan("a == ? && id > ?");
        assertEquals(new QueryPlan.Filter
                     ("a == ?0", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?1", null)),
                     plan);

        plan = mTable.queryPlan("id != ?");
        assertEquals(new QueryPlan.Filter
                     ("id != ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false)),
                     plan);

        plan = mTable.queryPlan("id >= ? && id < ? || id > ? && id <= ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?0", "id < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?2", "id <= ?3")
                      ), plan);

        plan = mTable.queryPlan("id >= ? && id < ? || !(id > ? && id <= ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?0", "id < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, null, "id <= ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?3", null)
                      ), plan);
                     
        plan = mTable.queryPlan("id >= ? && id < ? || (id > ? && id <= ? && c != ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?0", "id < ?1"),
                      new QueryPlan.Filter("c != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?2", "id <= ?3"))
                      ), plan);

        plan = mTable.queryPlan("(id >= ? && id < ? || id > ? && id <= ?) && c != ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("c != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?0", "id < ?1")),
                      new QueryPlan.Filter("c != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?2", "id <= ?3"))
                      ), plan);
    }

    @Test
    public void alternateKeyUnjoined() throws Exception {
        Table<TestRow> indexA = mIndexA.viewUnjoined();

        QueryPlan plan = indexA.queryPlan("a == ?0 && a != ?0");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = indexA.queryPlan("a == ?0 && a != ?0 && id == ?");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = indexA.queryPlan(null);
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false),
                     plan);

        plan = indexA.queryPlan("a == ?0 || a != ?0");
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false),
                     plan);

        plan = indexA.queryPlan("a < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, null, "a < ?0"),
                     plan);

        plan = indexA.queryPlan("a >= ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, "a >= ?0", null),
                     plan);

        plan = indexA.queryPlan("a >= ? && a < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, "a >= ?0", "a < ?1"),
                     plan);

        // This test should fail when LoadOne is supported.
        plan = indexA.queryPlan("a == ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, "a >= ?0", "a <= ?0"),
                     plan);

        try {
            indexA.queryPlan("b == ?");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unavailable for filtering: b"));
        }

        plan = indexA.queryPlan("id == ?");
        assertEquals(new QueryPlan.Filter
                     ("id == ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.queryPlan("a == ?0 && a != ?0 || id == ?1");
        assertEquals(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.queryPlan("id == ? && a > ?");
        assertEquals(new QueryPlan.Filter
                     ("id == ?0", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?1", null)),
                     plan);

        plan = indexA.queryPlan("a != ?");
        assertEquals(new QueryPlan.Filter
                     ("a != ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.queryPlan("a >= ? && a < ? || a > ? && a <= ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?0", "a < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?2", "a <= ?3")
                      ), plan);

        plan = indexA.queryPlan("a >= ? && a < ? || !(a > ? && a <= ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?0", "a < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, null, "a <= ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?3", null)
                      ), plan);
                     
        plan = indexA.queryPlan("a >= ? && a < ? || (a > ? && a <= ? && id != ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?0", "a < ?1"),
                      new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?2", "a <= ?3"))
                      ), plan);

        plan = indexA.queryPlan("(a >= ? && a < ? || a > ? && a <= ?) && id != ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?0", "a < ?1")),
                      new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?2", "a <= ?3"))
                      ), plan);
    }

    @Test
    public void alternateKey() throws Exception {
        QueryPlan plan = mIndexA.queryPlan("a == ?0 && a != ?0");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = mIndexA.queryPlan("a == ?0 && a != ?0 && id == ?");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = mIndexA.queryPlan(null);
        assertEquals(new QueryPlan.NaturalJoin
                     (TestRow.class.getName(), "primary key", new String[] {"+id"},
                      new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = mIndexA.queryPlan("b == ?");
        assertEquals(new QueryPlan.Filter
                     ("b == ?0", new QueryPlan.NaturalJoin
                      (TestRow.class.getName(), "primary key", new String[] {"+id"},
                       new QueryPlan.FullScan
                       (TestRow.class.getName(), "alternate key",
                        new String[] {"+a"}, false))),
                     plan);

        plan = mIndexA.queryPlan("a >= ? && a < ? && b == ?");
        assertEquals(new QueryPlan.Filter
                     ("b == ?2", new QueryPlan.NaturalJoin
                      (TestRow.class.getName(), "primary key", new String[] {"+id"},
                       new QueryPlan.RangeScan
                       (TestRow.class.getName(), "alternate key",
                        new String[] {"+a"}, false, "a >= ?0", "a < ?1"))),
                     plan);
    }

    @Test
    public void secondaryIndexUnjoined() throws Exception {
        Table<TestRow> indexB = mIndexB.viewUnjoined();

        QueryPlan plan = indexB.queryPlan("b == ?0 && b != ?0");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = indexB.queryPlan("b == ?0 && b != ?0 && id == ?");
        assertEquals(new QueryPlan.Empty(), plan);

        plan = indexB.queryPlan(null);
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false),
                     plan);

        plan = indexB.queryPlan("b == ?0 || b != ?0");
        assertEquals(new QueryPlan.FullScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false),
                     plan);

        plan = indexB.queryPlan("b < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, null, "b < ?0"),
                     plan);

        plan = indexB.queryPlan("b >= ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?0", null),
                     plan);

        plan = indexB.queryPlan("b >= ? && b < ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?0", "b < ?1"),
                     plan);

        plan = indexB.queryPlan("b == ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?0", "b <= ?0"),
                     plan);

        plan = indexB.queryPlan("b == ? && id > ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b == ?0 && id > ?1", "b <= ?0"),
                     plan);

        // Double check that the above plan is correct.
        {
            var row = mTable.newRow();
            row.id(1);
            row.a(1);
            row.b("b");
            row.c(null);
            mTable.insert(null, row);

            row.id(2);
            row.a(2);
            assertTrue(mTable.insert(null, row));

            row.id(3);
            row.a(3);
            row.b("c");
            assertTrue(mTable.insert(null, row));

            List<TestRow> results = indexB.newStream(null, "b == ? && id > ?", "b", 0).toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(0).id());
            assertEquals(2, results.get(1).id());
        }

        // This test should fail when LoadOne is supported.
        plan = indexB.queryPlan("b == ? && id == ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false,
                      "b == ?0 && id >= ?1", "b == ?0 && id <= ?1"),
                     plan);

        try {
            indexB.queryPlan("a == ?");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unavailable for filtering: a"));
        }

        plan = indexB.queryPlan("id == ?");
        assertEquals(new QueryPlan.Filter
                     ("id == ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.queryPlan("b == ?0 && b != ?0 || id == ?1");
        assertEquals(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.queryPlan("id == ? && b > ?");
        assertEquals(new QueryPlan.Filter
                     ("id == ?0", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?1", null)),
                     plan);

        plan = indexB.queryPlan("b != ?");
        assertEquals(new QueryPlan.Filter
                     ("b != ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.queryPlan("b < ?0 || b > ?0");
        assertEquals(new QueryPlan.Filter
                     ("b != ?0", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.queryPlan("b >= ? && b < ? || b > ? && b <= ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?0", "b < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?2", "b <= ?3")
                      ), plan);

        plan = indexB.queryPlan("b >= ? && b < ? || !(b > ? && b <= ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?0", "b < ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, null, "b <= ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?3", null)
                      ), plan);
                     
        plan = indexB.queryPlan("b >= ? && b < ? || (b > ? && b <= ? && id != ?)");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?0", "b < ?1"),
                      new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?2", "b <= ?3"))
                      ), plan);

        plan = indexB.queryPlan("(b >= ? && b < ? || b > ? && b <= ?) && id != ?");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?0", "b < ?1")),
                      new QueryPlan.Filter("id != ?4", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?2", "b <= ?3"))
                      ), plan);
    }

    @Test
    public void secondaryIndex() throws Exception {
        QueryPlan plan = mIndexB.queryPlan("(c > ? || c <= ?) && b != ? && a != ?");

        assertEquals(new QueryPlan.Filter
                     ("(c > ?0 || c <= ?1) && a != ?3", new QueryPlan.NaturalJoin
                      (TestRow.class.getName(), "primary key", new String[] {"+id"},
                       new QueryPlan.Filter
                       ("b != ?2", new QueryPlan.FullScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false)))),
                     plan);

        plan = mIndexB.queryPlan("(b == ? && id != ? && c != ?) || (b == ? && c > ?)");

        //System.out.println(plan);

        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("c != ?2", new QueryPlan.NaturalJoin
                       (TestRow.class.getName(), "primary key", new String[] {"+id"},
                        new QueryPlan.Filter
                        ("id != ?1", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                          false, "b >= ?0", "b <= ?0")))),
                      new QueryPlan.Filter
                      ("c > ?4", new QueryPlan.NaturalJoin
                       (TestRow.class.getName(), "primary key", new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?3", "b <= ?3")))),
                     plan);

        // With this plan, the range over 'b' is the same, and so it doesn't open multiple
        // cursors which do the exact same thing.
        plan = mIndexB.queryPlan("(b == ? && id != ? && c != ?) || (b == ?0 && c > ?)");

        //System.out.println(plan);

        assertEquals(new QueryPlan.Filter
                     ("c > ?3 || (id != ?1 && c != ?2)", new QueryPlan.NaturalJoin
                      (TestRow.class.getName(), "primary key", new String[] {"+id"},
                       new QueryPlan.RangeScan
                       (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                        false, "b >= ?0", "b <= ?0"))),
                     plan);
    }

    @Test
    public void secondaryIndex2Unjoined() throws Exception {
        Table<TestRow> indexC = mIndexC.viewUnjoined();

        QueryPlan plan = indexC.queryPlan("c >= ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, false, null, "c <= ?0"),
                     plan);

        plan = indexC.queryPlan("c > ?");
        assertEquals(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, false, null, "c < ?0"),
                     plan);

        plan = indexC.queryPlan("b == ? && c > ? && c <= ?");
        assertEquals(new QueryPlan.Filter
                     ("b == ?0", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false, "c >= ?2", "c < ?1")),
                     plan);

        // Double check that the above plans are correct.
        {
            for (int i = 1; i <= 3; i++) {
                var row = mTable.newRow();
                row.id(i);
                row.a(i);
                row.b("b" + i);
                row.c((long) i);
                assertTrue(mTable.insert(null, row));
            }

            List<TestRow> results = indexC.newStream(null, "c >= ?", 2).toList();
            assertEquals(2, results.size());
            assertEquals(3, results.get(0).id());
            assertEquals(2, results.get(1).id());

            results = indexC.newStream(null, "c > ?", 1).toList();
            assertEquals(2, results.size());
            assertEquals(3, results.get(0).id());
            assertEquals(2, results.get(1).id());

            results = indexC.newStream(null, "b == ? && c > ? && c <= ?", "b2", 1, 2).toList();
            assertEquals(1, results.size());
            assertEquals(2, results.get(0).id());
        }

        plan = indexC.queryPlan
            ("c == ?6 && b == ?5 && id <= ?4 || c == ?3 && b >= ?2 && b <= ?1 || c == ?0");
        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c == ?6 && b >= ?5", "c == ?6 && b == ?5 && id <= ?4"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c == ?3 && b >= ?2", "c == ?3 && b <= ?1"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c >= ?0", "c <= ?0")
                      ), plan);

        plan = indexC.queryPlan("c < ? && b == ?");
        assertEquals(new QueryPlan.Filter
                     ("b == ?1", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false, "c > ?0", null)),
                     plan);
    }

    @Test
    public void secondaryIndex2() throws Exception {
        QueryPlan plan = mIndexC.queryPlan("(c > ? || c <= ?) && b != ? && a != ?");

        //System.out.println(plan);

        assertEquals(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("a != ?3", new QueryPlan.NaturalJoin
                       (TestRow.class.getName(), "primary key", new String[] {"+id"},
                        new QueryPlan.Filter
                        ("b != ?2", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[]{"-c","+b","+id"},
                          false, null, "c < ?0")))),
                     (new QueryPlan.Filter
                      ("a != ?3", new QueryPlan.NaturalJoin
                       (TestRow.class.getName(), "primary key", new String[] {"+id"},
                        new QueryPlan.Filter
                        ("b != ?2", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[]{"-c","+b","+id"},
                          false, "c >= ?1", null)))))),
                     plan);

        // Double check that the above plan is correct.
        {
            for (int i = 1; i <= 5; i++) {
                var row = mTable.newRow();
                row.id(i);
                row.a(i);
                row.b("b" + i);
                row.c((long) i);
                assertTrue(mTable.insert(null, row));
            }

            Transaction txn = mDatabase.newTransaction();

            List<TestRow> results = mIndexC.newStream
                (txn, "(c > ? || c <= ?) && b != ? && a != ?", 3, 2, "b2", 4).toList();

            assertEquals(2, results.size());
            assertEquals(5, results.get(0).id());
            assertEquals(1, results.get(1).id());
        }
    }

    @PrimaryKey("id")
    @AlternateKey("a")
    @SecondaryIndex("b")
    @SecondaryIndex({"-c", "b"})
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
}