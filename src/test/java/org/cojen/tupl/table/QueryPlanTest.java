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

package org.cojen.tupl.table;

import java.math.BigDecimal;

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
    private StoredTable<TestRow> mTable, mIndexA, mIndexB, mIndexC;

    @Before
    public void before() throws Exception {
        mDatabase = Database.open(new DatabaseConfig());
        mTable = (StoredTable<TestRow>) mDatabase.openTable(TestRow.class);

        mIndexA = mTable.viewAlternateKey("a");
        mIndexB = mTable.viewSecondaryIndex("b");

        try {
            mIndexC = mTable.viewSecondaryIndex("c");
            fail();
        } catch (IllegalStateException e) {
        }

        mIndexC = mTable.viewSecondaryIndex("c", "b");
    }

    @After
    public void teardown() throws Exception {
        mDatabase.close();
    }

    @Test
    public void primaryKey() throws Exception {
        Query<TestRow> query = mTable.query("id == ?1 && id != ?1");
        assertEquals(TestRow.class, query.rowType());
        assertEquals(0, query.argumentCount());
        QueryPlan plan = query.scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        query = mTable.query("id == ?1 && id != ?1 && a == ?");
        assertEquals(0, query.argumentCount());
        plan = query.scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = mTable.queryAll().scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false),
                     plan);

        plan = mTable.query("id == ?1 || id != ?1").scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false),
                     plan);

        plan = mTable.query("id < ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, true, null, "id < ?1"),
                     plan);

        plan = mTable.query("id >= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, "id >= ?1", null),
                     plan);

        query = mTable.query("id >= ? && id < ?");
        assertEquals(2, query.argumentCount());
        plan = query.scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false, "id >= ?1", "id < ?2"),
                     plan);

        plan = mTable.query("id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.LoadOne
                     (TestRow.class.getName(), "primary key", new String[] {"+id"}, "id == ?1"),
                     plan);

        plan = mTable.query("id != ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id != ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false)),
                     plan);

        plan = mTable.query("id >= ? && id < ? || id > ? && id <= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?1", "id < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?3", "id <= ?4")
                      ), plan);

        plan = mTable.query("id >= ? && id < ? || !(id > ? && id <= ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?1", "id < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, null, "id <= ?3"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?4", null)
                      ), plan);
                     
        plan = mTable.query("id >= ? && id < ? || (id > ? && id <= ? && c != ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?1", "id < ?2"),
                      new QueryPlan.Filter("c != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?3", "id <= ?4"))
                      ), plan);

        plan = mTable.query("(id >= ? && id < ? || id > ? && id <= ?) && c != ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("c != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id >= ?1", "id < ?2")),
                      new QueryPlan.Filter("c != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "primary key",
                       new String[] {"+id"}, false, "id > ?3", "id <= ?4"))
                      ), plan);
    }

    @Test
    public void alternateKeyUnjoined() throws Exception {
        Table<TestRow> indexA = mIndexA.viewUnjoined();

        QueryPlan plan = indexA.query("a == ?1 && a != ?1").scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = indexA.query("a == ?1 && a != ?1 && id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = indexA.queryAll().scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false),
                     plan);

        plan = indexA.query("a == ?1 || a != ?1").scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false),
                     plan);

        plan = indexA.query("a < ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, null, "a < ?1"),
                     plan);

        plan = indexA.query("a >= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, "a >= ?1", null),
                     plan);

        plan = indexA.query("a >= ? && a < ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, false, "a >= ?1", "a < ?2"),
                     plan);

        plan = indexA.query("a == ?").scannerPlan(null);
        comparePlans(new QueryPlan.LoadOne
                     (TestRow.class.getName(), "alternate key",
                      new String[] {"+a"}, "a == ?1"),
                     plan);

        try {
            indexA.query("b == ?").scannerPlan(null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unavailable for filtering: b"));
        }

        plan = indexA.query("id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.query("a == ?1 && a != ?1 || id == ?2").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?2", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.query("id == ? && a > ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?2", null)),
                     plan);

        plan = indexA.query("a != ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("a != ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = indexA.query("a >= ? && a < ? || a > ? && a <= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?1", "a < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?3", "a <= ?4")
                      ), plan);

        plan = indexA.query("a >= ? && a < ? || !(a > ? && a <= ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?1", "a < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, null, "a <= ?3"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?4", null)
                      ), plan);
                     
        plan = indexA.query("a >= ? && a < ? || (a > ? && a <= ? && id != ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?1", "a < ?2"),
                      new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?3", "a <= ?4"))
                      ), plan);

        plan = indexA.query("(a >= ? && a < ? || a > ? && a <= ?) && id != ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a >= ?1", "a < ?2")),
                      new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false, "a > ?3", "a <= ?4"))
                      ), plan);
    }

    @Test
    public void alternateKey() throws Exception {
        Query<TestRow> query = mIndexA.query("a == ?1 && a != ?1");
        assertEquals(TestRow.class, query.rowType());
        assertEquals(0, query.argumentCount());
        QueryPlan plan = query.scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = mIndexA.query("a == ?1 && a != ?1 && id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = mIndexA.queryAll().scannerPlan(null);
        comparePlans(new QueryPlan.PrimaryJoin
                     (TestRow.class.getName(), new String[] {"+id"},
                      new QueryPlan.FullScan
                      (TestRow.class.getName(), "alternate key",
                       new String[] {"+a"}, false)),
                     plan);

        plan = mIndexA.query("b == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b == ?1", new QueryPlan.PrimaryJoin
                      (TestRow.class.getName(), new String[] {"+id"},
                       new QueryPlan.FullScan
                       (TestRow.class.getName(), "alternate key",
                        new String[] {"+a"}, false))),
                     plan);

        plan = mIndexA.query("a >= ? && a < ? && b == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b == ?3", new QueryPlan.PrimaryJoin
                      (TestRow.class.getName(), new String[] {"+id"},
                       new QueryPlan.RangeScan
                       (TestRow.class.getName(), "alternate key",
                        new String[] {"+a"}, false, "a >= ?1", "a < ?2"))),
                     plan);

        plan = mIndexA.query("a > ? && id != ?").scannerPlan(null);
        comparePlans(new QueryPlan.PrimaryJoin
                     (TestRow.class.getName(), new String[] {"+id"},
                      new QueryPlan.Filter
                      ("id != ?2", new QueryPlan.RangeScan
                       (TestRow.class.getName(), "alternate key",
                        new String[] {"+a"}, false, "a > ?1", null))),
                     plan);
    }

    @Test
    public void secondaryIndexUnjoined() throws Exception {
        Table<TestRow> indexB = mIndexB.viewUnjoined();

        QueryPlan plan = indexB.query("b == ?1 && b != ?1").scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = indexB.query("b == ?1 && b != ?1 && id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Empty(), plan);

        plan = indexB.queryAll().scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false),
                     plan);

        plan = indexB.query("b == ?1 || b != ?1").scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false),
                     plan);

        plan = indexB.query("b < ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, null, "b < ?1"),
                     plan);

        plan = indexB.query("b >= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?1", null),
                     plan);

        plan = indexB.query("b >= ? && b < ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?1", "b < ?2"),
                     plan);

        plan = indexB.query("b == ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b >= ?1", "b <= ?1"),
                     plan);

        plan = indexB.query("b == ? && id > ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, false, "b == ?1 && id > ?2", "b <= ?1"),
                     plan);

        // Double check that the above plan is correct.
        {
            var row = mTable.newRow();
            row.id(1);
            row.a(1);
            row.b("b");
            row.c(null);
            row.d(null);
            mTable.insert(null, row);

            row.id(2);
            row.a(2);
            mTable.insert(null, row);

            row.id(3);
            row.a(3);
            row.b("c");
            mTable.insert(null, row);

            List<TestRow> results = indexB.newStream(null, "b == ? && id > ?", "b", 0).toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(0).id());
            assertEquals(2, results.get(1).id());
        }

        plan = indexB.query("b == ? && id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.LoadOne
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, "b == ?1 && id == ?2"),
                     plan);

        try {
            indexB.query("a == ?").scannerPlan(null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unavailable for filtering: a"));
        }

        plan = indexB.query("id == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.query("b == ?1 && b != ?1 || id == ?2").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?2", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.query("id == ? && b > ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("id == ?1", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?2", null)),
                     plan);

        plan = indexB.query("b != ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b != ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.query("b < ?1 || b > ?1").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b != ?1", new QueryPlan.FullScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false)),
                     plan);

        plan = indexB.query("b >= ? && b < ? || b > ? && b <= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?1", "b < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?3", "b <= ?4")
                      ), plan);

        plan = indexB.query("b >= ? && b < ? || !(b > ? && b <= ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?1", "b < ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, null, "b <= ?3"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?4", null)
                      ), plan);
                     
        plan = indexB.query("b >= ? && b < ? || (b > ? && b <= ? && id != ?)").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?1", "b < ?2"),
                      new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?3", "b <= ?4"))
                      ), plan);

        plan = indexB.query("(b >= ? && b < ? || b > ? && b <= ?) && id != ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b >= ?1", "b < ?2")),
                      new QueryPlan.Filter("id != ?5", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"+b", "+id"}, false, "b > ?3", "b <= ?4"))
                      ), plan);
    }

    @Test
    public void secondaryIndex() throws Exception {
        QueryPlan plan = mIndexB.query("(c > ? || c <= ?) && b != ? && a != ?").scannerPlan(null);

        comparePlans(new QueryPlan.Filter
                     ("(c > ?1 || c <= ?2) && a != ?4", new QueryPlan.PrimaryJoin
                      (TestRow.class.getName(), new String[] {"+id"},
                       new QueryPlan.Filter
                       ("b != ?3", new QueryPlan.FullScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false)))),
                     plan);

        plan = mIndexB.query("(b == ? && id != ? && c != ?) || (b == ? && c > ?)")
            .scannerPlan(null);

        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("c != ?3", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.Filter
                        ("id != ?2", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                          false, "b >= ?1", "b <= ?1")))),
                      new QueryPlan.Filter
                      ("c > ?5", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?4", "b <= ?4")))),
                     plan);

        // With this plan, the range over 'b' is the same, and so it doesn't open multiple
        // cursors which do the exact same thing.
        plan = mIndexB.query("(b == ? && id != ? && c != ?) || (b == ?1 && c > ?)")
            .scannerPlan(null);

        comparePlans(new QueryPlan.Filter
                     ("c > ?4 || (id != ?2 && c != ?3)", new QueryPlan.PrimaryJoin
                      (TestRow.class.getName(), new String[] {"+id"},
                       new QueryPlan.RangeScan
                       (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                        false, "b >= ?1", "b <= ?1"))),
                     plan);
    }

    @Test
    public void secondaryIndex2Unjoined() throws Exception {
        Table<TestRow> indexC = mIndexC.viewUnjoined();

        QueryPlan plan = indexC.query("c >= ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, false, null, "c <= ?1"),
                     plan);

        plan = indexC.query("c > ?").scannerPlan(null);
        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, false, null, "c < ?1"),
                     plan);

        plan = indexC.query("b == ? && c > ? && c <= ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b == ?1", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false, "c >= ?3", "c < ?2")),
                     plan);

        // Double check that the above plans are correct.
        {
            for (int i = 1; i <= 3; i++) {
                var row = mTable.newRow();
                row.id(i);
                row.a(i);
                row.b("b" + i);
                row.c((long) i);
                row.d(null);
                mTable.insert(null, row);
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

        plan = indexC.query
            ("c == ?7 && b == ?6 && id <= ?5 || c == ?4 && b >= ?3 && b <= ?2 || c == ?1")
            .scannerPlan(null);
        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c == ?7 && b >= ?6", "c == ?7 && b == ?6 && id <= ?5"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c == ?4 && b >= ?3", "c == ?4 && b <= ?2"),
                      new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false,
                       "c >= ?1", "c <= ?1")
                      ), plan);

        plan = indexC.query("c < ? && b == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("b == ?2", new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index",
                       new String[] {"-c", "+b", "+id"}, false, "c > ?1", null)),
                     plan);
    }

    @Test
    public void secondaryIndex2() throws Exception {
        QueryPlan plan = mIndexC.query("(c > ? || c <= ?) && b != ? && a != ?").scannerPlan(null);

        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("a != ?4", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.Filter
                        ("b != ?3", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[]{"-c","+b","+id"},
                          false, null, "c < ?1")))),
                     (new QueryPlan.Filter
                      ("a != ?4", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.Filter
                        ("b != ?3", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[]{"-c","+b","+id"},
                          false, "c >= ?2", null)))))),
                     plan);

        // Double check that the above plan is correct.
        {
            for (int i = 1; i <= 5; i++) {
                var row = mTable.newRow();
                row.id(i);
                row.a(i);
                row.b("b" + i);
                row.c((long) i);
                row.d(null);
                mTable.insert(null, row);
            }

            Transaction txn = mDatabase.newTransaction();

            List<TestRow> results = mIndexC.newStream
                (txn, "(c > ? || c <= ?) && b != ? && a != ?", 3, 2, "b2", 4).toList();

            assertEquals(2, results.size());
            assertEquals(5, results.get(0).id());
            assertEquals(1, results.get(1).id());
        }
    }

    @Test
    public void joinedDoubleCheck() throws Exception {
        Query<TestRow> query = mIndexB.query("(b == ? && id != ? && c != ?) || (b == ? && c > ?)");

        assertEquals(TestRow.class, query.rowType());
        assertEquals(5, query.argumentCount());

        QueryPlan plan = query.scannerPlan(Transaction.BOGUS);

        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("c != ?3 && b == ?1", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.Filter
                        ("id != ?2", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                          false, "b >= ?1", "b <= ?1")))),
                      new QueryPlan.Filter
                      ("c > ?5 && b == ?4", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?4", "b <= ?4")))),
                     plan);

        plan = mIndexB.query
            ("(b == ? && id != c && c != ?) || (b == ? && c > ?)")
            .scannerPlan(Transaction.BOGUS);

        comparePlans(new QueryPlan.RangeUnion
                     (new QueryPlan.Filter
                      ("id != c && c != ?2 && b == ?1", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?1", "b <= ?1"))),
                      new QueryPlan.Filter
                      ("c > ?4 && b == ?3", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?3", "b <= ?3")))),
                     plan);
    }

    @Test
    public void disjointAndSort() throws Exception {
        {
            var row = mTable.newRow();
            row.id(1);
            row.a(1);
            row.b("hello1");
            row.c(101L);
            row.d(null);
            mTable.insert(null, row);

            row = mTable.newRow();
            row.id(2);
            row.a(2);
            row.b("hello2");
            row.c(102L);
            row.d(null);
            mTable.insert(null, row);

            row = mTable.newRow();
            row.id(3);
            row.a(3);
            row.b("hello3");
            row.c(103L);
            row.d(null);
            mTable.insert(null, row);

            row = mTable.newRow();
            row.id(4);
            row.a(4);
            row.b("world");
            row.c(104L);
            row.d(null);
            mTable.insert(null, row);

            row = mTable.newRow();
            row.id(555);
            row.a(-555);
            row.b("-world");
            row.c(-555L);
            row.d(null);
            mTable.insert(null, row);
        }

        String query = "{+a} a == ? || a == ? || (b == ? && b != ?) || id >= ?";
        QueryPlan plan = mTable.query(query).scannerPlan(null);

        comparePlans(new QueryPlan.Sort
                     (new String[] {"+a"}, new QueryPlan.DisjointUnion
                      (new QueryPlan.RangeScan
                       (TestRow.class.getName(), "primary key", new String[] {"+id"}, false,
                        "id >= ?5", null), new QueryPlan.RangeUnion
                       (new QueryPlan.Filter
                        ("id < ?5", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "alternate key",
                          new String[] {"+a"}, false, "a >= ?1", "a <= ?1")),
                        new QueryPlan.Filter
                        ("id < ?5", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "alternate key", new String[] {"+a"}, false,
                          "a >= ?2", "a <= ?2"))),
                       new QueryPlan.Filter
                       ("a != ?1 && a != ?2", new QueryPlan.PrimaryJoin
                        (TestRow.class.getName(), new String[] {"+id"},
                         new QueryPlan.Filter
                         ("b != ?4", new QueryPlan.RangeScan
                          (TestRow.class.getName(), "secondary index", new String[] {"+b", "+id"},
                           false, "b >= ?3", "b == ?3 && id < ?5")))))),
                     plan);

        List<TestRow> results = mTable.newStream(null, query, 1, 2, "world", "x", 100).toList();
        assertEquals(4, results.size());
        assertEquals(-555, results.get(0).a());
        assertEquals(1, results.get(1).a());
        assertEquals(2, results.get(2).a());
        assertEquals(4, results.get(3).a());

        query = "{+c} c == ? || c == ? || (b == ? && b != ?) || id >= ?";
        plan = mTable.query(query).scannerPlan(null);

        comparePlans(new QueryPlan.Sort
                     (new String[] {"+c"}, new QueryPlan.DisjointUnion
                      (new QueryPlan.RangeScan
                       (TestRow.class.getName(), "primary key", new String[] {"+id"}, false,
                        "id >= ?5", null), new QueryPlan.RangeUnion
                       (new QueryPlan.Filter
                        ("id < ?5", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index",
                          new String[] {"-c", "+b", "+id"}, true, "c >= ?2", "c <= ?2")),
                        new QueryPlan.Filter
                        ("id < ?5", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index",
                          new String[] {"-c", "+b", "+id"}, true, "c >= ?1", "c <= ?1"))),
                       new QueryPlan.Filter
                       ("c != ?1 && c != ?2",
                        new QueryPlan.PrimaryJoin
                        (TestRow.class.getName(), new String[] {"+id"},
                         new QueryPlan.Filter
                         ("b != ?4", new QueryPlan.RangeScan
                          (TestRow.class.getName(), "secondary index", new String[] {"+b", "+id"},
                           false, "b >= ?3", "b == ?3 && id < ?5")))))), plan);

        results = mTable.newStream(null, query, 101, 102, "world", "x", 100).toList();
        assertEquals(4, results.size());
        assertEquals(-555, (long) results.get(0).c());
        assertEquals(101, (long) results.get(1).c());
        assertEquals(102, (long) results.get(2).c());
        assertEquals(104, (long) results.get(3).c());

        query = "{+b} b == ? || b == ? || (a == ? && a != ?) || id >= ?";
        plan = mTable.query(query).scannerPlan(null);

        comparePlans(new QueryPlan.Sort
                     (new String[] {"+b"}, new QueryPlan.DisjointUnion
                      (new QueryPlan.RangeScan
                       (TestRow.class.getName(), "primary key", new String[] {"+id"}, false,
                        "id >= ?5", null), new QueryPlan.RangeUnion
                       (new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index",
                         new String[] {"+b", "+id"}, false, "b >= ?1", "b == ?1 && id < ?5"),
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] {"+b", "+id"},
                         false, "b >= ?2", "b == ?2 && id < ?5")),
                       new QueryPlan.Filter
                       ("b != ?1 && b != ?2",
                        new QueryPlan.PrimaryJoin
                        (TestRow.class.getName(), new String[] {"+id"},
                         new QueryPlan.Filter
                         ("a != ?4 && id < ?5", new QueryPlan.RangeScan
                          (TestRow.class.getName(), "alternate key", new String[] {"+a"},
                           false, "a >= ?3", "a <= ?3"))))))
                     , plan);

        results = mTable.newStream(null, query, "hello1", "hello2", 4, 1, 4).toList();
        assertEquals(4, results.size());
        assertEquals("-world", results.get(0).b());
        assertEquals("hello1", results.get(1).b());
        assertEquals("hello2", results.get(2).b());
        assertEquals("world", results.get(3).b());
    }

    @Test
    public void covering() throws Exception {
        var table = mDatabase.openTable(TestRow.class);

        QueryPlan plan = table.query("{b, id} b == ? && c == ?").scannerPlan(null);

        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, false,
                      "c == ?2 && b >= ?1", "c == ?2 && b <= ?1"),
                     plan);
    }

    @Test
    public void covering2() throws Exception {
        var table = (StoredTable<TestRow2>) mDatabase.openTable(TestRow2.class);
        var index = table.viewSecondaryIndex("b", "a", "c");

        QueryPlan plan = index.query("b == ? && c == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("c == ?2", new QueryPlan.RangeScan
                      (TestRow2.class.getName(), "secondary index",
                       new String[] {"+b", "+a"}, false, "b >= ?1", "b <= ?1")),
                     plan);

        plan = table.query("b == ? && c == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Filter
                     ("c == ?2", new QueryPlan.RangeScan
                      (TestRow2.class.getName(), "secondary index",
                       new String[] {"+b", "+a"}, false, "b >= ?1", "b <= ?1")),
                     plan);
    }

    @Test
    public void covering3() throws Exception {
        // An index is covering when it satisfies the projection and all remainder filters.

        QueryPlan plan = mTable.query("{id} b == ? && d == ?").scannerPlan(null);
        
        comparePlans(new QueryPlan.Filter
                     ("d == ?2", new QueryPlan.PrimaryJoin
                      (TestRow.class.getName(), new String[] {"+id"},
                       new QueryPlan.RangeScan
                       (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                        false, "b >= ?1", "b <= ?1"))),
                     plan);
    }

    @Test
    public void reverseScanOverSecondaryIndex() throws Exception {
        // Even though no ordering is specified, a reverse scan over a secondary index is
        // preferred when no join needs to be performed, and the query specifies an open range.
        // A search is performed for positioning the cursor initially, but then no predicate
        // needs to be checked for each row. If the scan wasn't reversed, the cursor would be
        // positioned at the start of the index and a predicate must be checked to determine
        // when to stop scanning.

        QueryPlan plan = mTable.query("{b, id} b <= ?").scannerPlan(null);

        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"+b", "+id"}, true, null, "b <= ?1"),
                     plan);

        plan = mTable.query("{c, b} c >= ?").scannerPlan(null);

        comparePlans(new QueryPlan.RangeScan
                     (TestRow.class.getName(), "secondary index",
                      new String[] {"-c", "+b", "+id"}, true, null, "c <= ?1"),
                     plan);

        for (int i=1; i<=5; i++) {
            var row = mTable.newRow();
            row.id(i);
            row.a(i);
            row.b("" + i);
            row.c((long) i);
            row.d(null);
            mTable.insert(null, row);
        }

        List<TestRow> results = mTable.newStream(null, "{b, id} b <= ?", 3).toList();
        assertEquals(3, results.size());
        assertEquals("3", results.get(0).b());
        assertEquals("2", results.get(1).b());
        assertEquals("1", results.get(2).b());

        results = mTable.newStream(null, "{c, b} c >= ?", 3).toList();
        assertEquals(3, results.size());
        assertEquals(3L, (long) results.get(0).c());
        assertEquals(4L, (long) results.get(1).c());
        assertEquals(5L, (long) results.get(2).c());
    }

    @Test
    public void orderByPrimaryKey() throws Exception {
        // If the ordering fully specifies the primary key up front, no sort is required.
        QueryPlan plan = mTable.query("{+id, +b}").scannerPlan(null);
        comparePlans(new QueryPlan.FullScan
                     (TestRow.class.getName(), "primary key",
                      new String[] {"+id"}, false),
                     plan);

        // No need to sort by columns after all primary or alternate keys are specified.
        plan = mTable.query("{+id, +b} b == ?").scannerPlan(null);
        comparePlans(new QueryPlan.Sort
                     (new String[] {"+id"}, new QueryPlan.RangeScan
                      (TestRow.class.getName(), "secondary index", new String[] {"+b", "+id"},
                       false, "b >= ?1", "b <= ?1")),
                     plan);
    }

    @Test
    public void orderByFullMatched() throws Exception {
        // No need to sort by columns which are fully matched. Column b is dropped from the
        // ordering specification.
        QueryPlan plan = mTable.query("{+c, +b} b == ? && c != ?").scannerPlan(null);
        comparePlans(new QueryPlan.Sort
                     (new String[] {"+c"},
                      new QueryPlan.Filter
                      ("c != ?2", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.RangeScan
                        (TestRow.class.getName(), "secondary index", new String[] { "+b", "+id"},
                         false, "b >= ?1", "b <= ?1")))),
                     plan);

        // BigDecimal equal matching is fuzzy, and so multiple matches are possible. Column d
        // isn't dropped from the order specification.
        plan = mTable.query("{+c, +d} d == ? && c != ?").scannerPlan(null);
        comparePlans(new QueryPlan.Sort
                     (new String[] {"+c", "+d"},
                      new QueryPlan.Filter
                      ("c != ?2", new QueryPlan.PrimaryJoin
                       (TestRow.class.getName(), new String[] {"+id"},
                        new QueryPlan.Filter
                        ("d == ?1", new QueryPlan.RangeScan
                         (TestRow.class.getName(), "secondary index", new String[] { "+d", "+id"},
                          false, "d == ?1", "d == ?1"))))),
                     plan);
    }

    /**
     * @throws AssertionError
     */
    private static void comparePlans(QueryPlan expect, QueryPlan actual) {
        assertTrue(expect != actual);
        assertEquals(expect, actual);
        assertEquals(expect.hashCode(), actual.hashCode());
        assertEquals(expect.toString(), actual.toString());
    }

    @PrimaryKey("id")
    @AlternateKey("a")
    @SecondaryIndex("b")
    @SecondaryIndex({"-c", "b"})
    @SecondaryIndex("d")
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

    @PrimaryKey({"a", "b"})
    @SecondaryIndex({"b", "a", "c"})
    public interface TestRow2 {
        int a();
        void a(int a);

        String b();
        void b(String b);

        @Nullable
        Long c();
        void c(Long c);
    }
}
