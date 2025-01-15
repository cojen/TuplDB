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

import java.io.IOException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class GroupedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(GroupedTest.class.getName());
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

    public interface TestRowGroup {
        String name();
        void name(String name);

        int count();
        void count(int count);

        long sumNum();
        void sumNum(long num);

        double avgNum();
        void avgNum(double avg);
    }

    public static class Grouped1 implements Grouper<TestRow, TestRowGroup> {
        final boolean mByName;
        final boolean mNoId;

        List<TestRow> mRows = new ArrayList<>();
        long mSum;
        int mPos = Integer.MAX_VALUE;

        Grouped1(boolean byName, boolean noId) {
            mByName = byName;
            mNoId = noId;
        }

        @Override
        public TestRow begin(TestRow source) throws IOException {
            checkSource(source);
            mRows.clear();
            mRows.add(source);
            return null;
        }

        @Override
        public TestRow accumulate(TestRow source) throws IOException {
            checkSource(source);
            if (mByName) {
                assertEquals(mRows.get(0).name(), source.name());
            }
            mRows.add(source);
            return null;
        }

        private void checkSource(TestRow source) {
            if (mNoId) {
                try {
                    source.id();
                    fail();
                } catch (UnsetColumnException e) {
                    // Expected.
                }
            }
        }

        @Override
        public void finished() throws IOException {
            mSum = 0;
            mPos = 0;
        }

        @Override
        public TestRowGroup step(TestRowGroup target) throws IOException {
            if (mPos >= mRows.size()) {
                mPos = Integer.MAX_VALUE;
                return null;
            }
            target.name(mRows.get(0).name());
            mSum += mRows.get(mPos).num();
            mPos++;
            target.count(mPos);
            target.sumNum(mSum);
            target.avgNum(mSum / (double) mPos);
            return target;
        }
    }

    public static class Grouped1Factory implements Grouper.Factory<TestRow, TestRowGroup> {
        final boolean mByName;
        final boolean mNoId;

        Grouped1Factory(boolean byName, boolean noId) {
            mByName = byName;
            mNoId = noId;
        }

        @Override
        public Grouper<TestRow, TestRowGroup> newGrouper() throws IOException {
            return new Grouped1(mByName, mNoId);
        }

        @Override
        public String sourceProjection() {
            return "name, num";
        }

        @Override
        public QueryPlan.Grouper plan(QueryPlan.Grouper plan) {
            return plan.withOperation("Grouped1");
        }

        @Untransformed
        public static String name_to_name(String name) {
            return name;
        }
    }

    public static class Grouped2 implements Grouper<TestRow, TestRowGroup> {
        @Override
        public TestRow begin(TestRow source) throws IOException {
            return source;
        }

        @Override
        public TestRow accumulate(TestRow source) throws IOException {
            return source;
        }

        @Override
        public TestRowGroup step(TestRowGroup target) throws IOException {
            return null;
        }

        @Override
        public String toString() {
            return "Grouped2";
        }
    }

    public static class RollingAvg implements Grouper<TestRow, TestRowGroup> {
        private final ArrayDeque<Long> mNums = new ArrayDeque<>();
        private final int mSize;
        private long mSum;
        private String mName;
        private boolean mReady;

        RollingAvg(int size) {
            mSize = size;
        }

        @Override
        public TestRow begin(TestRow source) throws IOException {
            mReady = true;
            mNums.clear();
            long num = source.num();
            mNums.add(num);
            mSum = num;
            mName = source.name();
            return source;
        }

        @Override
        public TestRow accumulate(TestRow source) throws IOException {
            mReady = true;
            long num = source.num();
            mNums.add(num);
            mSum += num;
            if (mNums.size() >= mSize) {
                mReady = true;
                if (mNums.size() > mSize) {
                    mSum -= mNums.remove();
                }
            }
            return source;
        }

        @Override
        public TestRowGroup step(TestRowGroup target) throws IOException {
            if (!mReady) {
                return null;
            }
            mReady = false;
            if (mNums.size() < mSize) {
                target.avgNum(Double.NaN);
            } else {
                target.avgNum(mSum / (double) mSize);
            }
            target.name(mName);
            return target;
        }
    }

    public static class RollingAvgFactory implements Grouper.Factory<TestRow, TestRowGroup> {
        private final int mSize;

        RollingAvgFactory(int size) {
            mSize = size;
        }

        @Override
        public Grouper<TestRow, TestRowGroup> newGrouper() {
            return new RollingAvg(mSize);
        }
    }

    @Test
    public void basic() throws Exception {
        Table<TestRowGroup> grouped = mTable.group
            ("", "", TestRowGroup.class, new Grouped1Factory(false, true));

        assertFalse(grouped.hasPrimaryKey());
        assertTrue(grouped.isEmpty());
        assertFalse(grouped.anyRows(null));
        assertFalse(grouped.anyRows(null, "count == ?", 3));

        fill();

        assertFalse(grouped.isEmpty());
        assertTrue(grouped.anyRows(null));
        assertTrue(grouped.anyRows(null, "count == ?", 3));
        assertFalse(grouped.anyRows(null, "count == ?", 300));

        QueryPlan plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
    key columns: +id
                     """,
                     plan.toString());

        var expect = new String[] {
            "{avgNum=1.0, count=1, sumNum=1, name=hello}",
            "{avgNum=1.0, count=2, sumNum=2, name=hello}",
            "{avgNum=4.0, count=3, sumNum=12, name=hello}",
            "{avgNum=3.5, count=4, sumNum=14, name=hello}",
            "{avgNum=7.0, count=5, sumNum=35, name=hello}",
            "{avgNum=6.0, count=6, sumNum=36, name=hello}",
        };

        try (var scanner = grouped.newScanner(null)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        grouped = mTable.group
            ("", "-id", TestRowGroup.class, new Grouped1Factory(false, false));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  order by: -id
  - reverse full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
    key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=11.0, count=2, sumNum=22, name=name}",
            "{avgNum=8.0, count=3, sumNum=24, name=name}",
            "{avgNum=8.5, count=4, sumNum=34, name=name}",
            "{avgNum=7.0, count=5, sumNum=35, name=name}",
            "{avgNum=6.0, count=6, sumNum=36, name=name}",
        };

        try (var scanner = grouped.newScanner(null)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        grouped = mTable.group
            ("+name", "", TestRowGroup.class, new Grouped1Factory(true, true));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  group by: +name
  - sort: +name
    - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=1.0, count=1, sumNum=1, name=hello}",
            "{avgNum=1.5, count=2, sumNum=3, name=hello}",
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=10.0, count=1, sumNum=10, name=readme}",
            "{avgNum=15.5, count=2, sumNum=31, name=readme}",
            "{avgNum=1.0, count=1, sumNum=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        grouped = mTable.group
            ("+name", "-id", TestRowGroup.class, new Grouped1Factory(true, false));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  group by: +name
  order by: -id
  - sort: +name, -id
    - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=2.0, count=1, sumNum=2, name=hello}",
            "{avgNum=1.5, count=2, sumNum=3, name=hello}",
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=21.0, count=1, sumNum=21, name=readme}",
            "{avgNum=15.5, count=2, sumNum=31, name=readme}",
            "{avgNum=1.0, count=1, sumNum=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        Query<TestRowGroup> query = grouped.query("count == ?");
        assertEquals(TestRowGroup.class, query.rowType());
        assertEquals(1, query.argumentCount());
        plan = query.scannerPlan(null, 1);
        assertEquals("""
- filter: count == ?1
  - group: org.cojen.tupl.table.GroupedTest$TestRowGroup
    operation: Grouped1
    group by: +name
    order by: -id
    - sort: +name, -id
      - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
        key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=2.0, count=1, sumNum=2, name=hello}",
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=21.0, count=1, sumNum=21, name=readme}",
            "{avgNum=1.0, count=1, sumNum=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null, "count == ?", 1)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        plan = grouped.query("{*, ~sumNum}").scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  group by: +name
  order by: -id
  - sort: +name, -id
    - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=2.0, count=1, name=hello}",
            "{avgNum=1.5, count=2, name=hello}",
            "{avgNum=1.0, count=1, name=name}",
            "{avgNum=21.0, count=1, name=readme}",
            "{avgNum=15.5, count=2, name=readme}",
            "{avgNum=1.0, count=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null, "{*, ~sumNum}")) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        plan = grouped.query("{*, ~sumNum} count == ?").scannerPlan(null, 1);
        assertEquals("""
- filter: count == ?1
  - group: org.cojen.tupl.table.GroupedTest$TestRowGroup
    operation: Grouped1
    group by: +name
    order by: -id
    - sort: +name, -id
      - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
        key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=2.0, count=1, name=hello}",
            "{avgNum=1.0, count=1, name=name}",
            "{avgNum=21.0, count=1, name=readme}",
            "{avgNum=1.0, count=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null, "{*, ~sumNum} count == ?", 1)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }
    }

    @Test
    public void discard() throws Exception {
        Table<TestRowGroup> grouped = mTable.group("", "", TestRowGroup.class, Grouped2::new);

        assertTrue(grouped.isEmpty());
        assertFalse(grouped.anyRows(null));
        assertFalse(grouped.anyRows(null, "count == ?", 3));

        fill();

        assertTrue(grouped.isEmpty());
        assertFalse(grouped.anyRows(null));
        assertFalse(grouped.anyRows(null, "count == ?", 3));

        grouped = mTable.group("-name", "", TestRowGroup.class, Grouped2::new);

        assertTrue(grouped.isEmpty());
        assertFalse(grouped.anyRows(null));
        assertFalse(grouped.anyRows(null, "count == ?", 3));
    }

    @Test
    public void sort() throws Exception {
        fill();

        // Note: Effective groupBy should be "+name" and effective orderBy should be "+id".
        Table<TestRowGroup> grouped = mTable.group
            ("+name-name", "+id+name-id", TestRowGroup.class, new Grouped1Factory(true, false));

        QueryPlan plan = grouped.query("{*, -avgNum}").scannerPlan(null);
        assertEquals("""
- sort: -avgNum
  - group: org.cojen.tupl.table.GroupedTest$TestRowGroup
    operation: Grouped1
    group by: +name
    order by: +id
    - sort: +name, +id
      - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
        key columns: +id
                     """,
                     plan.toString());

        var expect = new String[] {
            "{avgNum=15.5, count=2, sumNum=31, name=readme}",
            "{avgNum=10.0, count=1, sumNum=10, name=readme}",
            "{avgNum=1.5, count=2, sumNum=3, name=hello}",
            "{avgNum=1.0, count=1, sumNum=1, name=hello}",
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=1.0, count=1, sumNum=1, name=world}",
        };

        try (var scanner = grouped.newScanner(null, "{*, -avgNum}")) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        plan = grouped.query("{*, -name}").scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.table.GroupedTest$TestRowGroup
  operation: Grouped1
  group by: +name
  order by: +id
  - sort: -name, +id
    - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
      key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=1.0, count=1, sumNum=1, name=world}",
            "{avgNum=10.0, count=1, sumNum=10, name=readme}",
            "{avgNum=15.5, count=2, sumNum=31, name=readme}",
            "{avgNum=1.0, count=1, sumNum=1, name=name}",
            "{avgNum=1.0, count=1, sumNum=1, name=hello}",
            "{avgNum=1.5, count=2, sumNum=3, name=hello}",
        };

        try (var scanner = grouped.newScanner(null, "{*, -name}")) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }

        plan = grouped.query("{*, -name, +avgNum, ~sumNum} count > ?").scannerPlan(null, 1);
        assertEquals("""
- sort: -name, +avgNum
  - filter: count > ?1
    - group: org.cojen.tupl.table.GroupedTest$TestRowGroup
      operation: Grouped1
      group by: +name
      order by: +id
      - sort: -name, +id
        - full scan over primary key: org.cojen.tupl.table.GroupedTest$TestRow
          key columns: +id
                     """,
                     plan.toString());

        expect = new String[] {
            "{avgNum=15.5, count=2, name=readme}",
            "{avgNum=1.5, count=2, name=hello}",
        };

        try (var scanner = grouped.newScanner(null, "{*, -name, +avgNum, ~sumNum} count > ?", 1)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
        }
    }

    @Test
    public void rollingAvg() throws Exception {
        Table<TestRowGroup> grouped = mTable.group
            ("+name", "", TestRowGroup.class, new RollingAvgFactory(3));

        assertTrue(grouped.isEmpty());
        assertFalse(grouped.anyRows(null));
        assertFalse(grouped.anyRows(null, "avgNum == ?", 33));

        Object[][] data = {
            {1, "hello", 11},
            {2, "world", 22},
            {3, "hello", 33},
            {4, "world", 44},
            {5, "hello", 55},
            {6, "world", 66},
            {7, "hello", 77},
            {8, "world", 88},
            {9, "hello", 99},
            {10, "world", 1010},
        };

        fill(data);

        assertFalse(grouped.isEmpty());
        assertTrue(grouped.anyRows(null));
        assertTrue(grouped.anyRows(null, "avgNum == ?", 33));
        assertFalse(grouped.anyRows(null, "avgNum == ?", 300));

        var expect = new String[] {
            "{avgNum=NaN, name=hello}",
            "{avgNum=NaN, name=hello}",
            "{avgNum=33.0, name=hello}",
            "{avgNum=55.0, name=hello}",
            "{avgNum=77.0, name=hello}",
            "{avgNum=NaN, name=world}",
            "{avgNum=NaN, name=world}",
            "{avgNum=44.0, name=world}",
            "{avgNum=66.0, name=world}",
            "{avgNum=388.0, name=world}",
        };

        try (var scanner = grouped.newScanner(null)) {
            int i = 0;
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(expect[i++], row.toString());
            }
            assertNull(scanner.row());
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

        fill(data);
    }

    private void fill(Object[][] data) throws Exception {
        for (Object[] r : data) {
            var row = mTable.newRow();
            row.id((int) r[0]);
            row.name((String) r[1]);
            row.num((int) r[2]);
            mTable.insert(null, row);
        }
    }
}
