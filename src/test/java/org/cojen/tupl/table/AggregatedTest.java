/*
 *  Copyright (C) 2023 Cojen.org
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

import java.util.Comparator;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class AggregatedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AggregatedTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        setup(Database.open(new DatabaseConfig()));
    }

    private void setup(Database db) throws Exception {
        mDb = db;
        mTable = db.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
        TestUtils.deleteTempDatabases(getClass());
    }

    protected Database mDb;
    protected Table<TestRow> mTable;

    @PrimaryKey("id")
    @SecondaryIndex("name")
    public interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String name);

        int num();
        void num(int num);
    }

    public interface TestRowAgg {
        long count();
        void count(long count);

        long minNum();
        void minNum(long num);

        long maxNum();
        void maxNum(long num);

        long totalNum();
        void totalNum(long num);

        double avgNum();
        void avgNum(double num);
    }

    @PrimaryKey("name")
    public interface TestRowAggByName extends TestRowAgg {
        String name();
        void name(String name);
    }

    public static class Aggregator1<T extends TestRowAgg> implements Aggregator<TestRow, T> {
        private long count, minNum, maxNum, totalNum;

        @Override
        public TestRow begin(TestRow source) {
            count = 1;
            long num = source.num();
            totalNum = maxNum = minNum = num <= 0 ? 0 : num;
            return source;
        }

        @Override
        public TestRow accumulate(TestRow source) {
            count++;
            long num = source.num();
            if (num >= 0) {
                minNum = Math.min(minNum, num);
                maxNum = Math.max(maxNum, num);
                totalNum += num;
            }
            return source;
        }

        @Override
        public T finish(T target) {
            target.count(count);
            target.minNum(minNum);
            target.maxNum(maxNum);
            target.totalNum(totalNum);
            target.avgNum(((double) totalNum) / count);
            return target;
        }
    }

    public static class Agg1Factory<T extends TestRowAgg>
        implements Aggregator.Factory<TestRow, T>
    {
        @Override
        public Aggregator1<T> newAggregator() {
            return new Aggregator1<T>();
        }

        @Override
        public String sourceProjection() {
            return "num";
        }

        @Override
        public QueryPlan.Aggregator plan(QueryPlan.Aggregator plan) {
            return plan.withOperation("Aggregator1");
        }
    }

    public static class Aggregator2<T extends TestRowAgg> extends Aggregator1<T> {
        final String mExclude;
        String mCurrent;

        Aggregator2(String exclude) {
            mExclude = exclude;
        }

        @Override
        public TestRow begin(TestRow source) {
            mCurrent = source.name();
            return super.begin(source);
        }

        @Override
        public T finish(T target) {
            if (mExclude.equals(mCurrent)) {
                return null;
            }
            return super.finish(target);
        }
    }

    public static class Broken<T extends TestRowAgg> implements Aggregator<TestRow, T> {
        @Override
        public TestRow begin(TestRow source) {
            return source;
        }

        @Override
        public TestRow accumulate(TestRow source) {
            return source;
        }

        @Override
        public T finish(T target) {
            return target;
        }
    }

    @Test
    public void toOneRow() throws Exception {
        Table<TestRowAgg> aggregated = mTable.aggregate(TestRowAgg.class, new Agg1Factory<>());

        assertTrue(aggregated.isEmpty());
        assertFalse(aggregated.anyRows(null));
        assertFalse(aggregated.anyRows(null, "count != ?", 999));

        var row = aggregated.newRow();
        row.count(10);
        assertFalse(aggregated.tryLoad(null, row));
        assertEquals("{}", row.toString());
        row.count(10);
        assertFalse(aggregated.exists(null, row));
        assertEquals("{*count=10}", row.toString());

        fill();

        assertFalse(aggregated.isEmpty());
        assertTrue(aggregated.anyRows(null));
        assertTrue(aggregated.anyRows(null, "count != ?", 999));

        row = aggregated.newRow();
        row.count(10);
        assertTrue(aggregated.tryLoad(null, row));
        assertEquals("{avgNum=6.0, count=6, maxNum=21, minNum=1, totalNum=36}", row.toString());
        row = aggregated.newRow();
        row.count(10);
        assertTrue(aggregated.exists(null, row));
        assertEquals("{*count=10}", row.toString());

        QueryPlan plan = aggregated.queryAll().scannerPlan(null);
        assertEquals("""
- aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAgg
  operation: Aggregator1
  - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
    key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null)) {
            row = scanner.row();
            assertEquals("{avgNum=6.0, count=6, maxNum=21, minNum=1, totalNum=36}", row.toString());
            assertNull(scanner.step());
        }

        String query = "{avgNum, +count}";

        Query<TestRowAgg> q = aggregated.query(query);
        assertEquals(TestRowAgg.class, q.rowType());
        assertEquals(0, q.argumentCount());
        plan = q.scannerPlan(null);
        assertEquals("""
- aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAgg
  operation: Aggregator1
  - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
    key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query)) {
            row = scanner.row();
            assertEquals("{avgNum=6.0, count=6}", row.toString());
            assertNull(scanner.step());
        }

        query = "{avgNum, +count} minNum == ?";

        plan = aggregated.query(query).scannerPlan(null);
        assertEquals("""
- filter: minNum == ?1
  - aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAgg
    operation: Aggregator1
    - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, 1)) {
            row = scanner.row();
            assertEquals("{avgNum=6.0, count=6}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = aggregated.newScanner(null, query, 100)) {
            assertNull(scanner.row());
        }

        query = "minNum == ?";

        plan = aggregated.query(query).scannerPlan(null);
        assertEquals("""
- filter: minNum == ?1
  - aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAgg
    operation: Aggregator1
    - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, 1)) {
            row = scanner.row();
            assertEquals("{avgNum=6.0, count=6, maxNum=21, minNum=1, totalNum=36}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = aggregated.newScanner(null, query, 100)) {
            assertNull(scanner.row());
        }
    }

    @Test
    public void byName() throws Exception {
        Table<TestRowAggByName> aggregated =
            mTable.aggregate(TestRowAggByName.class, new Agg1Factory<>());

        assertTrue(aggregated.isEmpty());
        assertFalse(aggregated.anyRows(null));
        assertFalse(aggregated.anyRows(null, "count != ?", 999));

        var row = aggregated.newRow();
        row.count(10);
        try {
            aggregated.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Primary key"));
        }

        row.name("hello");
        assertFalse(aggregated.tryLoad(null, row));
        assertEquals("{*name=hello}", row.toString());

        fill();

        assertFalse(aggregated.isEmpty());
        assertTrue(aggregated.anyRows(null));
        assertTrue(aggregated.anyRows(null, "count != ?", 999));
        assertTrue(aggregated.anyRows(null, "count == ?", 2));

        QueryPlan plan = aggregated.queryAll().scannerPlan(null);
        assertEquals("""
- aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAggByName
  operation: Aggregator1
  group by: name
  - sort: +name
    - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        TestRowAggByName lastRow = null;

        try (var scanner = aggregated.newScanner(null)) {
            Comparator<? super TestRowAggByName> cmp = scanner.getComparator();

            row = scanner.row();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1, totalNum=3}", row);
            assertEquals(0, cmp.compare(row, row));
            lastRow = aggregated.cloneRow(row);

            row = scanner.step(row);
            expect("{name=name, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);
            assertTrue(cmp.compare(row, lastRow) > 0);
            lastRow = aggregated.cloneRow(row);

            row = scanner.step();
            expect("{name=readme, avgNum=15.5, count=2, maxNum=21, minNum=10, totalNum=31}", row);
            assertTrue(cmp.compare(row, lastRow) > 0);
            lastRow = aggregated.cloneRow(row);

            row = scanner.step();
            expect("{name=world, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);
            assertTrue(cmp.compare(row, lastRow) > 0);

            assertNull(scanner.step());
        }

        String query = "name >= ? && name < ?";

        Query<TestRowAggByName> q = aggregated.query(query);
        assertEquals(TestRowAggByName.class, q.rowType());
        assertEquals(2, q.argumentCount());

        plan = q.scannerPlan(null);
        assertEquals("""
- aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAggByName
  operation: Aggregator1
  group by: name
  - primary join: org.cojen.tupl.table.AggregatedTest$TestRow
    key columns: +id
    - range scan over secondary index: org.cojen.tupl.table.AggregatedTest$TestRow
      key columns: +name, +id
      range: name >= ?1 .. name < ?2
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, "readme", "readmf")) {
            row = scanner.row();
            expect("{name=readme, avgNum=15.5, count=2, maxNum=21, minNum=10, totalNum=31}", row);

            assertNull(scanner.step());
        }

        query = "maxNum >= ? && maxNum <= ?";

        plan = aggregated.query(query).scannerPlan(null);
        assertEquals("""
- filter: maxNum >= ?1 && maxNum <= ?2
  - aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAggByName
    operation: Aggregator1
    group by: name
    - sort: +name
      - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
        key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, 1, 2)) {
            row = scanner.row();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1, totalNum=3}", row);

            row = scanner.step(row);
            expect("{name=name, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            row = scanner.step();
            expect("{name=world, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            assertNull(scanner.step());
        }

        query = "{+avgNum, *} name >= ? && name < ? && maxNum >= ? && maxNum <= ?";

        plan = aggregated.query(query).scannerPlan(null);
        assertEquals("""
- sort: +avgNum
  - filter: maxNum >= ?3 && maxNum <= ?4
    - aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAggByName
      operation: Aggregator1
      group by: name
      - primary join: org.cojen.tupl.table.AggregatedTest$TestRow
        key columns: +id
        - range scan over secondary index: org.cojen.tupl.table.AggregatedTest$TestRow
          key columns: +name, +id
          range: name >= ?1 .. name < ?2
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, "a", "z", 1, 2)) {
            row = scanner.row();
            expect("{name=name, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            row = scanner.step(row);
            expect("{name=world, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            row = scanner.step();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1, totalNum=3}", row);

            assertNull(scanner.step());
        }

        try (var scanner = aggregated.newScanner(null, query, "readme", "readmef", 1, 2)) {
            assertNull(scanner.row());
        }

        query = "{-avgNum, *, ~totalNum} count >= ? && avgNum >= ?";

        plan = aggregated.query(query).scannerPlan(null);
        assertEquals("""
- sort: -avgNum
  - filter: count >= ?1 && avgNum >= ?2
    - aggregate: org.cojen.tupl.table.AggregatedTest$TestRowAggByName
      operation: Aggregator1
      group by: name
      - sort: +name
        - full scan over primary key: org.cojen.tupl.table.AggregatedTest$TestRow
          key columns: +id
                     """,
                     plan.toString());

        try (var scanner = aggregated.newScanner(null, query, 1, 1.5)) {
            row = scanner.row();
            expect("{name=readme, avgNum=15.5, count=2, maxNum=21, minNum=10}", row);

            row = scanner.step();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1}", row);

            assertNull(scanner.step());
        }
    }

    private static void expect(String expect, Object obj) {
        assertEquals(expect, obj.toString());
    }

    @Test
    public void byNameFiltered() throws Exception {
        Table<TestRowAggByName> aggregated =
            mTable.aggregate(TestRowAggByName.class, () -> new Aggregator2<>("readme"));

        fill();

        try (var scanner = aggregated.newScanner(null)) {
            var row = scanner.row();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1, totalNum=3}", row);

            row = scanner.step(row);
            expect("{name=name, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            row = scanner.step();
            expect("{name=world, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            assertNull(scanner.step());
        }

        aggregated = mTable.aggregate(TestRowAggByName.class, () -> new Aggregator2<>("world"));

        try (var scanner = aggregated.newScanner(null)) {
            var row = scanner.row();
            expect("{name=hello, avgNum=1.5, count=2, maxNum=2, minNum=1, totalNum=3}", row);

            row = scanner.step(row);
            expect("{name=name, avgNum=1.0, count=1, maxNum=1, minNum=1, totalNum=1}", row);

            row = scanner.step();
            expect("{name=readme, avgNum=15.5, count=2, maxNum=21, minNum=10, totalNum=31}", row);

            assertNull(scanner.step());
        }
    }

    @Test
    public void brokenAggregate() throws Exception {
        Table<TestRowAggByName> aggregated = mTable.aggregate(TestRowAggByName.class, Broken::new);

        fill();

        try {
            aggregated.newScanner(null, "count == ?", 0);
            fail();
        } catch (UnsetColumnException e) {
        }
    }

    private void fill() throws Exception {
        Object[][] data = {
            {1, "hello", 1},
            {2, "world", 1},
            {3, "readme", 10},
            {4, "hello", 2},
            {5, "readme", 21},
            {6, "name", 1},
        };

        for (Object[] r : data) {
            var row = mTable.newRow();
            row.id((int) r[0]);
            row.name((String) r[1]);
            row.num((int) r[2]);
            mTable.insert(null, row);
        }
    }
}
