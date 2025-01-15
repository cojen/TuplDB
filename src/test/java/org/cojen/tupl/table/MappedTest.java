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

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class MappedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(MappedTest.class.getName());
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
    @SecondaryIndex("str")
    public interface TestRow {
        long id();
        void id(long id);

        String str();
        void str(String str);

        int num();
        void num(int num);
    }

    @Test
    public void noInverse() throws Exception {
        // Swap the str and num columns.
        Table<TestRow> mapped = mTable.map(TestRow.class, (source, target) -> {
            assertEquals("{}", target.toString());
            target.id(source.id());
            target.str(String.valueOf(source.num()));
            try {
                target.num(Integer.parseInt(source.str()));
            } catch (NumberFormatException e) {
                return null;
            }
            return target;
        });

        assertFalse(mapped.hasPrimaryKey());
        assertTrue(mapped.isEmpty());

        TestRow row = mTable.newRow();
        row.id(1);
        row.str("hello");
        row.num(123);
        mTable.store(null, row);

        // String cannot be parsed as an integer.
        assertTrue(mapped.isEmpty());

        row.id(2);
        row.str("234");
        mTable.store(null, row);

        assertFalse(mapped.isEmpty());

        try {
            mapped.load(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            mapped.exists(null, row);
            fail();
        } catch (ViewConstraintException e) {
        }

        try {
            mapped.store(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.exchange(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.insert(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.replace(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.update(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.merge(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            mapped.delete(null, row);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        QueryPlan.Mapper plan = (QueryPlan.Mapper) mapped.queryAll().scannerPlan(null);
        assertEquals(TestRow.class.getName(), plan.target);
        assertNull(plan.operation);
        assertEquals(mTable.queryAll().scannerPlan(null), plan.source);

        QueryPlan plan2 = mapped.query("{*}").scannerPlan(null);
        assertEquals(plan, plan2);
        assertEquals(plan.hashCode(), plan2.hashCode());
        assertEquals(plan.toString(), plan2.toString());

        row = mTable.newRow();
        row.id(3);
        row.str("33");
        row.num(333);
        mTable.store(null, row);

        try {
            try (var updater = mapped.newUpdater(null)) {
                for (row = updater.row(); row != null; ) {
                    row = updater.delete(row);
                }
            }
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            try (var updater = mapped.newUpdater(null, "{*}")) {
                for (row = updater.row(); row != null; ) {
                    row = updater.delete(row);
                }
            }
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try (var scanner = mapped.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=2, num=234, str=123}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=3, num=33, str=333}", row.toString());
            assertNull(scanner.step(row));
        }

        try (var scanner = mapped.newScanner(null, "{*}")) {
            row = scanner.row();
            assertEquals("{id=2, num=234, str=123}", row.toString());
            row = scanner.step();
            assertEquals("{id=3, num=33, str=333}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newScanner(null, "{id, num}")) {
            row = scanner.row();
            assertEquals("{id=2, num=234}", row.toString());
            row = scanner.step();
            assertEquals("{id=3, num=33}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newScanner(mapped.newRow(), null)) {
            row = scanner.row();
            assertEquals("{id=2, num=234, str=123}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=3, num=33, str=333}", row.toString());
            assertNull(scanner.step(row));
        }

        try (var scanner = mapped.newScanner(mapped.newRow(), null, "{*}")) {
            row = scanner.row();
            assertEquals("{id=2, num=234, str=123}", row.toString());
            row = scanner.step();
            assertEquals("{id=3, num=33, str=333}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newScanner(null, "num >= ?", 100)) {
            row = scanner.row();
            assertEquals("{id=2, num=234, str=123}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newScanner(null, "{id, str} num >= ?", 100)) {
            row = scanner.row();
            assertEquals("{id=2, str=123}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newUpdater(null, "num < ?", 100)) {
            row = scanner.row();
            assertEquals("{id=3, num=33, str=333}", row.toString());
            assertNull(scanner.step());
        }

        var plan3 = (QueryPlan.Sort) mapped.query("{-str, id}").scannerPlan(null);
        assertEquals("[-str]", Arrays.toString(plan3.sortColumns));
        assertEquals(mTable.queryAll().scannerPlan(null), ((QueryPlan.Mapper) plan3.source).source);

        try (var scanner = mapped.newScanner(null, "{-str, id}")) {
            row = scanner.row();
            assertEquals("{id=3, str=333}", row.toString());
            row = scanner.step();
            assertEquals("{id=2, str=123}", row.toString());
            assertNull(scanner.step());
        }

        mapped.close();
        assertFalse(mapped.isClosed());
    }

    @Test
    public void withInverse() throws Exception {
        // Swap the str and num columns.
        Table<TestRow> mapped = mTable.map(TestRow.class, new Swapper());

        assertTrue(mapped.hasPrimaryKey());
        assertTrue(mapped.isEmpty());

        TestRow row = mTable.newRow();
        row.id(1);
        row.str("hello");
        row.num(123);
        mTable.store(null, row);

        // String cannot be parsed as an integer.
        assertTrue(mapped.isEmpty());

        assertFalse(mapped.exists(null, row));
        assertEquals("{id=1, num=123, str=hello}", row.toString()); // no side effect

        assertFalse(mapped.tryLoad(null, row));
        // When the load fails, only the target columns which map to source primary key columns
        // should still be set. This guards against accidentally using a row when the caller
        // failed to check the load return value. Loading against a plain table behaves in a
        // similar fashion -- it only keeps the primary key columns.
        assertEquals("{id=1}", row.toString());

        row = mTable.newRow();
        row.id(2);
        row.str("234");
        row.num(123);
        mTable.store(null, row);

        assertFalse(mapped.isEmpty());

        mapped.load(null, row);
        assertEquals("{id=2, num=234, str=123}", row.toString());

        assertTrue(mapped.exists(null, row));

        try {
            mapped.insert(null, row);
            fail();
        } catch (UniqueConstraintException e) {
        }

        try {
            row.str("xxx");
            mapped.store(null, row);
            fail();
        } catch (NumberFormatException e) {
        }

        row.str("2");
        mapped.store(null, row);
        mapped.load(null, row);
        assertEquals("{id=2, num=234, str=2}", row.toString());

        row.num(111);
        TestRow old = mapped.exchange(null, row);
        assertEquals("{id=2, num=111, str=2}", row.toString());
        assertEquals("{id=2, num=234, str=2}", old.toString());
        mapped.load(null, row);
        assertEquals("{id=2, num=111, str=2}", row.toString());

        row = mapped.newRow();
        row.id(999);
        try {
            mapped.exchange(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unset: num, str"));
        }
        assertEquals("{*id=999}", row.toString());

        row.num(999);
        row.str("9999");
        assertNull(mapped.exchange(null, row));
        assertEquals("{id=999, num=999, str=9999}", row.toString());
        mapped.load(null, row);
        assertEquals("{id=999, num=999, str=9999}", row.toString());

        row.str("hello");
        mTable.store(null, row);
        row.str("8");
        assertNull(mapped.exchange(null, row));
        mapped.load(null, row);
        assertEquals("{id=999, num=999, str=8}", row.toString());

        row = mapped.newRow();
        row.id(1000);
        row.num(1000);
        row.str("1000");
        mapped.insert(null, row);
        assertEquals("{id=1000, num=1000, str=1000}", row.toString());
        mapped.load(null, row);
        assertEquals("{id=1000, num=1000, str=1000}", row.toString());

        row.num(1);
        mapped.replace(null, row);
        assertEquals("{id=1000, num=1, str=1000}", row.toString());
        mapped.load(null, row);
        assertEquals("{id=1000, num=1, str=1000}", row.toString());

        row.id(0);
        try {
            mapped.replace(null, row);
            fail();
        } catch (NoSuchRowException e) {
        }
        assertEquals("{*id=0, num=1, str=1000}", row.toString());

        row.id(1000);
        row.num(2);
        mapped.update(null, row);
        assertEquals("{id=1000, num=2, str=1000}", row.toString());
        mapped.load(null, row);
        assertEquals("{id=1000, num=2, str=1000}", row.toString());

        row.id(0);
        row.num(9);
        try {
            mapped.update(null, row);
            fail();
        } catch (NoSuchRowException e) {
        }
        assertEquals("{*id=0, *num=9, str=1000}", row.toString());

        row = mapped.newRow();
        row.id(1000);
        row.num(99);
        mapped.merge(null, row);
        assertEquals("{id=1000, num=99, str=1000}", row.toString());
        mapped.load(null, row);
        assertEquals("{id=1000, num=99, str=1000}", row.toString());

        row.id(0);
        row.num(91);
        try {
            mapped.merge(null, row);
            fail();
        } catch (NoSuchRowException e) {
        }
        assertEquals("{*id=0, *num=91, str=1000}", row.toString());

        row.id(1000);
        row.str("hello"); // invalid num
        mTable.store(null, row);

        row.str("111");
        mapped.merge(null, row);
        assertEquals("{}", row.toString());
        row.id(1000);
        assertFalse(mapped.tryLoad(null, row));

        assertTrue(mapped.tryDelete(null, row));
        assertFalse(mapped.tryDelete(null, row));

        try (var scanner = mapped.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=2, num=111, str=2}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=999, num=999, str=8}", row.toString());
            assertNull(scanner.step(row));
        }

        Query<TestRow> query = mapped.query("num == ? && str == ?");
        assertEquals(TestRow.class, query.rowType());
        assertEquals(2, query.argumentCount());
        var plan = (QueryPlan.Mapper) query.scannerPlan(null);
        var sub1 = (QueryPlan.Filter) plan.source;
        assertEquals("num == ?4", sub1.expression);
        var sub2 = (QueryPlan.PrimaryJoin) sub1.source;
        var sub3 = (QueryPlan.RangeScan) sub2.source;
        assertEquals("str >= ?3", sub3.low);
        assertEquals("str <= ?3", sub3.high);

        try (var scanner = mapped.newScanner(null, "num == ? && str == ?", 111, 2)) {
            row = scanner.row();
            assertEquals("{id=2, num=111, str=2}", row.toString());
            assertNull(scanner.step(row));
        }

        try (var updater = mapped.newUpdater(null)) {
            for (row = updater.row(); row != null; ) {
                row.num(row.num() - 1);
                row = updater.update(row);
            }
            try {
                updater.update(row);
                fail();
            } catch (IllegalStateException e) {
            }
        }

        try (var scanner = mapped.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=2, num=110, str=2}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=999, num=998, str=8}", row.toString());
            assertNull(scanner.step(row));
        }

        var plan2 = (QueryPlan.Sort) mapped.query("{-str, id}").scannerPlan(null);
        assertEquals("[-str]", Arrays.toString(plan2.sortColumns));
        assertEquals(mTable.queryAll().scannerPlan(null), ((QueryPlan.Mapper) plan2.source).source);

        try (var scanner = mapped.newScanner(null, "{-str, id}")) {
            row = scanner.row();
            assertEquals("{id=999, str=8}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=2, str=2}", row.toString());
            assertNull(scanner.step(row));
        }

        // This is because the Swapper doesn't override the performsFiltering method. The map
        // method can filter out results if a NumberFormatException is caught.
        assertTrue(mapped.query("{-id}").scannerPlan(null) instanceof QueryPlan.Sort);

        try (var scanner = mapped.newScanner(null, "{-id, *}")) {
            row = scanner.row();
            assertEquals("{id=999, num=998, str=8}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=2, num=110, str=2}", row.toString());
            assertNull(scanner.step(row));
        }

        try (var updater = mapped.newUpdater(Transaction.BOGUS, "num == ?", 998)) {
            row = updater.row();
            row.str("888");
            assertNull(updater.update(row));
        }

        try (var scanner = mapped.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=2, num=110, str=2}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=999, num=998, str=888}", row.toString());
            assertNull(scanner.step(row));
        }

        try (var updater = mapped.newUpdater(null, "{-num, *}")) {
            row = updater.row();
            assertEquals("{id=999, num=998, str=888}", row.toString());
            row = updater.step(row);
            assertEquals("{id=2, num=110, str=2}", row.toString());
            assertNull(updater.step(row));
        }

        Transaction txn = mapped.newTransaction(null);
        try {
            try (var updater = mapped.newUpdater(null, "{-num, *}")) {
                for (row = updater.row(); row != null; ) {
                    row.num(row.num() - 1);
                    row = updater.update(row);
                }
                try {
                    updater.update(row);
                    fail();
                } catch (IllegalStateException e) {
                }
            }

            try (var scanner = mapped.newScanner(null)) {
                row = scanner.row();
                assertEquals("{id=2, num=109, str=2}", row.toString());
                row = scanner.step(row);
                assertEquals("{id=999, num=997, str=888}", row.toString());
                assertNull(scanner.step(row));
            }
        } finally {
            txn.reset();
        }

        row = mTable.newRow();
        row.id(999);
        row.str("hello");
        mTable.update(null, row);

        try (var updater = mapped.newUpdater(null)) {
            row = updater.row();
            row.num(row.num() - 1);
            assertNull(updater.update(row));
        }

        try (var updater = mapped.newUpdater(null)) {
            for (row = updater.row(); row != null; row = updater.delete());
        }

        assertTrue(mapped.isEmpty());
        assertFalse(mTable.isEmpty());

        try (var scanner = mTable.newScanner(null)) {
            row = scanner.row();
            assertEquals("{id=1, num=123, str=hello}", row.toString());
            row = scanner.step(row);
            assertEquals("{id=999, num=888, str=hello}", row.toString());
            assertNull(scanner.step(row));
        }
    }

    /**
     * Swaps the str and num columns.
     */
    public static class Swapper implements Mapper<TestRow, TestRow> {
        @Override
        public TestRow map(TestRow source, TestRow target) {
            assertEquals("{}", target.toString());
            target.id(source.id());
            target.str(String.valueOf(source.num()));
            try {
                target.num(Integer.parseInt(source.str()));
            } catch (NumberFormatException e) {
                return null;
            }
            return target;
        }

        @Override
        public void checkStore(Table<TestRow> table, TestRow row) throws ViewConstraintException {
            try {
                Integer.parseInt(row.str());
            } catch (NumberFormatException e) {
                throw new ViewConstraintException();
            }
        }

        @Override
        public void checkUpdate(Table<TestRow> table, TestRow row) throws ViewConstraintException {
            if (table.isSet(row, "str")) {
                try {
                    Integer.parseInt(row.str());
                } catch (NumberFormatException e) {
                    throw new ViewConstraintException();
                }
            }
        }

        @Override
        public void checkDelete(Table<TestRow> table, TestRow row) throws ViewConstraintException {
        }

        @Untransformed
        public static long id_to_id(long id) {
            return id;
        }

        public static String num_to_str(int num) {
            return String.valueOf(num);
        }

        public static int str_to_num(String str) {
            return Integer.parseInt(str);
        }
    }

    @Test
    public void rename() throws Exception {
        Table<Renamed> mapped = mTable.map(Renamed.class, new Renamer());

        assertFalse(mapped.hasPrimaryKey());

        Renamed row = mapped.newRow();
        row.identifier(1);
        row.string("hello");
        row.number(123);
        mapped.store(null, row);

        row = mapped.newRow();
        row.identifier(2);
        row.string("world");
        row.number(456);
        mapped.store(null, row);

        try (var scanner = mapped.newScanner(null)) {
            row = scanner.row();
            assertEquals("{identifier=1, number=123, string=hello}", row.toString());
            row = scanner.step();
            assertEquals("{identifier=2, number=456, string=world}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mTable.newScanner(null)) {
            var sourceRow = scanner.row();
            assertEquals("{id=1, num=123, str=hello}", sourceRow.toString());
            sourceRow = scanner.step();
            assertEquals("{id=2, num=456, str=world}", sourceRow.toString());
            assertNull(scanner.step());
        }

        Query<Renamed> q = mapped.query("string > ? && number != ?");
        assertEquals(Renamed.class, q.rowType());
        assertEquals(2, q.argumentCount());
        var plan = (QueryPlan.Mapper) q.scannerPlan(null);
        var sub1 = (QueryPlan.Filter) plan.source;
        assertEquals("str > ?3 && num != ?4", sub1.expression);

        try (var scanner = mapped.newScanner(null, "string > ? && number != ?", "hello", 0)) {
            row = scanner.row();
            assertEquals("{identifier=2, number=456, string=world}", row.toString());
            assertNull(scanner.step());
        }

        try (var scanner = mapped.newScanner(null, "{identifier, number}")) {
            row = scanner.row();
            assertEquals("{identifier=1, number=123}", row.toString());
            row = scanner.step(row);
            assertEquals("{identifier=2, number=456}", row.toString());
            assertNull(scanner.step());
        }

        String query = "{-string, identifier} string >= ?";

        var plan2 = (QueryPlan.Sort) mapped.query(query).scannerPlan(null);
        assertEquals("[-string]", Arrays.toString(plan2.sortColumns));
        assertEquals(mTable.query("str >= ?2").scannerPlan(null),
                     ((QueryPlan.Mapper) plan2.source).source);

        try (var scanner = mapped.newScanner(null, query, "a")) {
            row = scanner.row();
            assertEquals("{identifier=2, string=world}", row.toString());
            row = scanner.step(row);
            assertEquals("{identifier=1, string=hello}", row.toString());
            assertNull(scanner.step());
        }

        query = "{-identifier} string >= ?";

        var plan3 = mapped.query(query).scannerPlan(null);
        assertEquals("""
- map: org.cojen.tupl.table.MappedTest$Renamed
  operation: Rename
  - filter: str >= ?2
    - reverse full scan over primary key: org.cojen.tupl.table.MappedTest$TestRow
      key columns: +id
                     """, plan3.toString());

        try (var scanner = mapped.newScanner(null, query, "a")) {
            row = scanner.row();
            assertEquals("{identifier=2}", row.toString());
            row = scanner.step(row);
            assertEquals("{identifier=1}", row.toString());
            assertNull(scanner.step());
        }
    }

    public static interface Renamed {
        long identifier();
        void identifier(long id);

        String string();
        void string(String str);

        int number();
        void number(int num);
    }

    public static class Renamer implements Mapper<TestRow, Renamed> {
        @Override
        public Renamed map(TestRow source, Renamed target) {
            assertEquals("{}", target.toString());
            target.identifier(source.id());
            target.string(source.str());
            target.number(source.num());
            return target;
        }

        @Override
        public boolean performsFiltering() {
            return false;
        }

        @Override
        public void checkStore(Table<TestRow> table, TestRow row) throws ViewConstraintException {
        }

        @Override
        public void checkUpdate(Table<TestRow> table, TestRow row) throws ViewConstraintException {
        }

        @Override
        public void checkDelete(Table<TestRow> table, TestRow row) throws ViewConstraintException {
        }

        @Untransformed
        public static long identifier_to_id(long id) {
            return id;
        }

        public static String string_to_str(String str) {
            return str;
        }

        public static int number_to_num(int num) {
            return num;
        }

        @Override
        public QueryPlan.Mapper plan(QueryPlan.Mapper plan) {
            return plan.withOperation("Rename");
        }
    }

    @Test
    public void sortMany() throws Exception {
        sortMany(5000);
    }

    @Test
    public void sortManyMore() throws Exception {
        teardown();
        setup(TestUtils.newTempDatabase(getClass(), 20_000_000));
        sortMany(1_500_000);
    }

    private void sortMany(int amount) throws Exception {
        Table<Renamed> mapped = mTable.map(Renamed.class, new Renamer());

        var plan = (QueryPlan.Sort) mapped.query("{+number, *}").scannerPlan(null);
        assertEquals("[+number]", Arrays.toString(plan.sortColumns));

        var rnd = new Random(amount);

        long checksum = 0;

        for (int i = 1; i <= amount; i++) {
            Renamed row = mapped.newRow();
            row.identifier(i);
            row.string("hello-" + i);
            int num = rnd.nextInt();
            row.number(num);
            checksum += num;
            mapped.store(null, row);
        }

        int num = 0;
        int last = Integer.MIN_VALUE;
        long actualChecksum = 0;

        try (var scanner = mapped.newScanner(null, "{+number, *}")) {
            for (var row = scanner.row(); row != null; row = scanner.step(row)) {
                num++;
                assertTrue(row.number() >= last);
                actualChecksum += row.number();
                last = row.number();
            }
        }

        assertEquals(amount, num);
        assertEquals(checksum, actualChecksum);
    }

    @Test
    public void brokenMapping() throws Exception {
        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str("hello");
            row.num(123);
            mTable.store(null, row);
        }

        Table<Renamed> mapped = mTable.map(Renamed.class, (source, target) -> {
            return target;
        });

        try {
            mapped.newScanner(null, "identifier == ?", 0);
            fail();
        } catch (UnsetColumnException e) {
        }
    }

    @Test
    public void conversionMapping() throws Exception {
        Table<TestRow2> mapped = mTable.map(TestRow2.class);

        assertFalse(mapped.hasPrimaryKey());

        {
            TestRow row = mTable.newRow();

            row.id(1);
            row.str("hello");
            row.num(123);
            mTable.store(null, row);

            row.id(5_000_000_000L);
            row.str("456");
            row.num(456);
            mTable.store(null, row);
        }

        TestRow2 row2 = mapped.newRow();
        row2.id(1);
        mapped.load(null, row2);
        assertEquals(1, row2.id());
        assertEquals(0, row2.str());
        assertTrue(123.0 == row2.num());
        assertNull(row2.x());

        try (var s = mapped.newScanner(null)) {
            row2 = s.row();
            assertEquals(1, row2.id());
            assertEquals(0, row2.str());
            assertTrue(123.0 == row2.num());
            
            row2 = s.step();
            assertEquals(Integer.MAX_VALUE, row2.id());
            assertEquals(456, row2.str());
            assertTrue(456.0 == row2.num());

            assertNull(s.step());
        }

        row2.id(Integer.MAX_VALUE);
        assertFalse(mapped.tryLoad(null, row2));

        row2.id(2);
        row2.str(567);

        row2.num(67.8);

        try {
            mapped.store(null, row2);
            fail();
        } catch (ConversionException e) {
            assertTrue(e.getMessage().contains("column=num"));
            assertTrue(e.getMessage().contains("67.8"));
        }

        row2.num(null);

        try {
            mapped.store(null, row2);
            fail();
        } catch (ConversionException e) {
            assertTrue(e.getMessage().contains("column=num"));
            assertTrue(e.getMessage().contains("null"));
        }

        row2.num(678.0);
        mapped.store(null, row2);

        TestRow row = mTable.newRow();
        row.id(2);
        mTable.load(null, row);
        assertEquals(2, row.id());
        assertEquals("567", row.str());
        assertEquals(678, row.num());

        row2.id(3);
        row2.str(33);
        row2.num(333.0);
        row2.x(3333);

        try {
            mapped.store(null, row2);
        } catch (ConversionException e) {
            assertTrue(e.getMessage().contains("column=x"));
        }

        row2.x(0);

        try {
            mapped.store(null, row2);
        } catch (ConversionException e) {
            assertTrue(e.getMessage().contains("column=x"));
        }

        row2.x(null);

        mapped.store(null, row2);

        row.id(3);
        mTable.load(null, row);
        assertEquals("33", row.str());
        assertEquals(333, row.num());

        row2.x(3333);
        assertTrue(mapped.tryDelete(null, row2));
        assertFalse(mTable.tryLoad(null, row));

        try (var s = mapped.query("{id, str, num=3.0}").newScanner(null)) {
            row2 = s.row();
            assertEquals(1, row2.id());
            assertEquals(0, row2.str());
            assertTrue(3.0 == row2.num());
            
            row2 = s.step();
            assertEquals(2, row2.id());
            assertEquals(567, row2.str());
            assertTrue(3.0 == row2.num());

            row2 = s.step();
            assertEquals(Integer.MAX_VALUE, row2.id());
            assertEquals(456, row2.str());
            assertTrue(3.0 == row2.num());

            assertNull(s.step());
        }
    }

    public interface TestRow2 {
        int id();
        void id(int id);

        int str();
        void str(int str);

        @Nullable
        Double num();
        void num(Double num);

        @Nullable
        Integer x();
        void x(Integer x);
    }

    @Test
    public void conversionMappingDroppedSourceColumn() throws Exception {
        Table<TestRow3> mapped = mTable.map(TestRow3.class);

        assertFalse(mapped.hasPrimaryKey());

        TestRow row = mTable.newRow();
        row.id(1);
        row.str("hello");
        row.num(123);
        mTable.store(null, row);

        TestRow3 row3 = mapped.newRow();
        row3.id(1);
        mapped.load(null, row3);
        assertEquals(1, row3.id());
        assertEquals("hello", row3.str());

        row3.str("world");
        mapped.update(null, row3);

        mTable.load(null, row);
        assertEquals("world", row.str());

        // Cannot insert a new row because the "num" column was dropped.
        row3.id(2);
        row3.str("x");
        try {
            mapped.tryInsert(null, row3);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        row3.id(1);
        row3.str(null);
        try {
            mapped.update(null, row3);
            fail();
        } catch (ConversionException e) {
            assertTrue(e.getMessage().contains("column=str"));
            assertTrue(e.getMessage().contains("Cannot assign null to non-nullable type"));
        }
    }

    public interface TestRow3 {
        long id();
        void id(long id);

        @Nullable
        String str();
        void str(String str);
    }

    @Test
    public void pkTransformed1() throws Exception {
        // Target primary key column is transformed.
        Table<Mapped1> mapped = mTable.map(Mapped1.class, new Mapper1());
        assertFalse(mapped.hasPrimaryKey());
    }

    @Test
    public void pkTransformed1_1() throws Exception {
        // Target primary key column has a potential lossy conversion.
        Table<Mapped1> mapped = mTable.map(Mapped1.class, new Mapper1_1());
        assertFalse(mapped.hasPrimaryKey());
    }

    public static class Mapper1 implements Mapper<TestRow, Mapped1> {
        @Override
        public Mapped1 map(TestRow source, Mapped1 target) {
            target.id((int) source.id());
            target.str(source.str());
            target.num(source.num());
            return target;
        }

        public static long id_to_id(int id) {
            return id;
        }

        @Untransformed
        public static String str_to_str(String str) {
            return str;
        }

        @Untransformed
        public static int num_to_num(int num) {
            return num;
        }
    }

    public static class Mapper1_1 extends Mapper1 {
        @Untransformed
        public static long id_to_id(int id) {
            return id;
        }
    }

    @PrimaryKey("id")
    public interface Mapped1 {
        int id();
        void id(int id);

        String str();
        void str(String str);

        int num();
        void num(int num);
    }
}
