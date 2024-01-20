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
        int mPos;
        long mSum;

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
        public TestRowGroup process(TestRowGroup target) throws IOException {
            TestRow first = mRows.get(0);
            target.name(first.name());
            mSum = first.num();
            mPos = 1;
            target.count(1);
            target.sumNum(mSum);
            target.avgNum(mSum);
            return target;
        }

        @Override
        public TestRowGroup step(TestRowGroup target) throws IOException {
            if (mPos >= mRows.size()) {
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

        @Override
        public String toString() {
            return "Grouped1";
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
        public TestRowGroup process(TestRowGroup target) throws IOException {
            return null;
        }

        @Override
        public TestRowGroup step(TestRowGroup target) throws IOException {
            fail();
            return null;
        }

        @Override
        public String toString() {
            return "Grouped2";
        }
    }

    @Test
    public void basic() throws Exception {
        Table<TestRowGroup> grouped = mTable.group
            ("", "", TestRowGroup.class, new Grouped1Factory(false, true));

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
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        grouped = mTable.group
            ("", "-id", TestRowGroup.class, new Grouped1Factory(false, false));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  order by: -id
  - reverse full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        grouped = mTable.group
            ("+name", "", TestRowGroup.class, new Grouped1Factory(true, true));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  group by: +name
  - sort: +name
    - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        grouped = mTable.group
            ("+name", "-id", TestRowGroup.class, new Grouped1Factory(true, false));

        plan = grouped.queryAll().scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  group by: +name
  order by: -id
  - sort: +name, -id
    - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        plan = grouped.query("count == ?").scannerPlan(null, 1);
        assertEquals("""
- filter: count == ?1
  - group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
    using: Grouped1
    group by: +name
    order by: -id
    - sort: +name, -id
      - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        plan = grouped.query("{*, ~sumNum}").scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  group by: +name
  order by: -id
  - sort: +name, -id
    - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        plan = grouped.query("{*, ~sumNum} count == ?").scannerPlan(null, 1);
        assertEquals("""
- filter: count == ?1
  - group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
    using: Grouped1
    group by: +name
    order by: -id
    - sort: +name, -id
      - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
  - group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
    using: Grouped1
    group by: +name
    order by: +id
    - sort: +name, +id
      - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        plan = grouped.query("{*, -name}").scannerPlan(null);
        assertEquals("""
- group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
  using: Grouped1
  group by: +name
  order by: +id
  - sort: -name, +id
    - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
        }

        plan = grouped.query("{*, -name, +avgNum, ~sumNum} count > ?").scannerPlan(null, 1);
        assertEquals("""
- sort: -name, +avgNum
  - filter: count > ?1
    - group: org.cojen.tupl.rows.GroupedTest$TestRowGroup
      using: Grouped1
      group by: +name
      order by: +id
      - sort: -name, +id
        - full scan over primary key: org.cojen.tupl.rows.GroupedTest$TestRow
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
