/*
 *  Copyright (C) 2021 Cojen.org
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

import java.util.HashMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowCrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowCrudTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    protected Database mDb;
    protected Table<TestRow> mTable;

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String str1();
        void str1(String str);

        @Nullable
        String str2();
        void str2(String str);

        int num1();
        void num1(int num);
    }

    @Test
    public void basic() throws Exception {
        assertEquals(TestRow.class, mTable.rowType());
        assertTrue(mTable.isEmpty());
        assertSame(mTable, mDb.openTable(TestRow.class));

        TestRow row = mTable.newRow();
        assertTrue(row.toString().endsWith("{}"));
        mTable.unsetRow(row);
        assertTrue(row.toString().endsWith("{}"));

        assertFalse(mTable.isSet(row, "id"));

        mTable.forEach(row, (r, n, v) -> {
            fail();
        });

        try {
            row.id();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.str1();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.str2();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.num1();
            fail();
        } catch (UnsetColumnException e) {
        }

        row.str1("hello");
        assertEquals("hello", row.str1());
        assertTrue(mTable.isSet(row, "str1"));

        var found = new HashMap<String, Object>();

        mTable.forEach(row, (r, n, v) -> {
            assertSame(row, r);
            found.put(n, v);
        });

        assertEquals(1, found.size());
        assertEquals("hello", found.get("str1"));
        found.clear();

        try {
            mTable.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        try {
            mTable.exists(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        assertEquals("hello", row.str1());

        row.id(1);
        assertFalse(mTable.tryLoad(null, row));

        assertTrue(row.toString().endsWith("{*id=1}"));

        try {
            row.str1();
            fail();
        } catch (UnsetColumnException e) {
        }

        assertEquals(1, row.id());

        try {
            mTable.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Some required columns are unset: num1, str1, str2", e.getMessage());
        }

        assertFalse(mTable.tryDelete(null, row));
        assertTrue(row.toString().endsWith("{*id=1}"));

        row.str1("hello");
        row.str2(null);
        row.num1(100);
        assertTrue(row.toString().endsWith("{*id=1, *num1=100, *str1=hello, *str2=null}"));

        mTable.forEach(row, (r, n, v) -> {
            assertSame(row, r);
            found.put(n, v);
        });

        assertEquals(4, found.size());
        assertEquals(1L, found.get("id"));
        assertEquals("hello", found.get("str1"));
        assertEquals(null, found.get("str2"));
        assertEquals(100, found.get("num1"));
        found.clear();

        mTable.insert(null, row);
        assertTrue(row.toString().endsWith("{id=1, num1=100, str1=hello, str2=null}"));
        assertFalse(mTable.isEmpty());
        assertTrue(mTable.exists(null, row));
        assertTrue(mTable.tryLoad(null, row));
        assertTrue(row.toString().endsWith("{id=1, num1=100, str1=hello, str2=null}"));

        TestRow row2 = mTable.newRow();
        row2.id(1);
        assertTrue(mTable.tryLoad(null, row2));
        assertEquals(row, row2);
        assertEquals(row.hashCode(), row2.hashCode());
        assertEquals(row.toString(), row2.toString());
        try {
            mTable.insert(null, row2);
            fail();
        } catch (UniqueConstraintException e) {
        }

        row2.str2("world");
        mTable.update(null, row2);
        assertTrue(row2.toString().endsWith("{id=1, num1=100, str1=hello, str2=world}"));

        row.str1("howdy");
        mTable.update(null, row);
        assertTrue(row.toString().endsWith("{id=1, num1=100, str1=howdy, str2=null}"));
        row.str1("hi");
        mTable.merge(null, row);
        assertTrue(row.toString().endsWith("{id=1, num1=100, str1=hi, str2=world}"));

        row2.num1(-555);
        mTable.update(null, row2);
        assertTrue(row2.toString().endsWith("{id=1, num1=-555, str1=hello, str2=world}"));

        mTable.unsetRow(row2);
        row2.id(1);
        row2.num1(999);
        mTable.update(null, row2);
        assertTrue(row2.toString().endsWith("{id=1, num1=999}"));

        row2.str2("everyone");
        mTable.merge(null, row2);
        assertTrue(row2.toString().endsWith("{id=1, num1=999, str1=hi, str2=everyone}"));

        mTable.replace(null, row);
        mTable.load(null, row2);
        assertTrue(row2.toString().endsWith("{id=1, num1=100, str1=hi, str2=world}"));

        assertTrue(mTable.tryDelete(null, row2));
        assertFalse(mTable.tryDelete(null, row));
        assertTrue(mTable.isEmpty());

        assertTrue(row.toString().endsWith("{id=1, num1=100, str1=hi, str2=world}"));
        assertTrue(row2.toString().endsWith("{id=1, num1=100, str1=hi, str2=world}"));

        Transaction txn = mTable.newTransaction(null);
        mTable.insert(txn, row);
        assertTrue(mTable.exists(txn, row2));
        assertFalse(mTable.isEmpty());
        txn.reset(); // rollback

        assertFalse(mTable.exists(null, row2));
        assertTrue(mTable.isEmpty());

        mTable.store(null, row);
        row2.str1("hello");
        TestRow row3 = mTable.exchange(null, row2);
        assertEquals(row, row3);
        mTable.delete(null, row3);
        assertNull(mTable.exchange(null, row2));
    }

    @Test
    public void loadSideEffect() throws Exception {
        // When a load operation returns false, the state of all non-key fields must be unset,
        // but all the fields must remain unchanged.

        TestRow row = mTable.newRow();
        row.id(10);
        row.str1("hello");
        row.str2("world");
        row.num1(123);
        TestRow copy = mTable.cloneRow(row);
        assertEquals(row, copy);
        TestRow copy2 = mTable.newRow();
        mTable.copyRow(row, copy2);
        assertEquals(copy, copy2);
        assertFalse(mTable.tryLoad(null, row));
        assertTrue(row.toString().contains("{*id=10}"));

        assertNotEquals(row.toString(), copy.toString());
        assertEquals(0, mTable.comparator("+id+str1+str2+num1").compare(row, copy));
    }

    @Test
    public void basicUpdaterDeleteAutoCommit() throws Exception {
        basicUpdaterDelete(null);
    }

    @Test
    public void basicUpdaterDeleteUpgradable() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        basicUpdaterDelete(txn);
    }

    @Test
    public void basicUpdaterDeleteRepeatable() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.REPEATABLE_READ);
        basicUpdaterDelete(txn);
    }

    @Test
    public void basicUpdaterDeleteCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        basicUpdaterDelete(txn);
    }

    @Test
    public void basicUpdaterDeleteUncommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_UNCOMMITTED);
        basicUpdaterDelete(txn);
    }

    private void basicUpdaterDelete(Transaction txn) throws Exception {
        for (int i=1; i<=10; i++) {
            TestRow row = mTable.newRow();
            row.id(i);
            row.num1(1000 + i);
            row.str1("s1-" + i);
            row.str2("s2-" + i);
            mTable.store(null, row);
        }

        Updater<TestRow> updater = mTable.newUpdater
            (txn, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "s2-9");

        for (TestRow row = updater.row(); row != null; ) {
            if (row.num1() == 1004) {
                row = updater.step();
            } else if (row.id() == 3) {
                row = updater.delete();
            } else {
                row = updater.delete(row);
            }
        }

        assertEquals(7, mDb.findIndex(TestRow.class.getName()).count(null, null));

        {
            // Can load the row that was skipped because an exclusive lock wasn't acquired.
            TestRow row = mTable.newRow();
            row.id(4);
            assertTrue(mTable.exists(null, row));
        }

        if (txn == null) {
            // Auto-commit transaction releases the exclusive lock.
            TestRow row = mTable.newRow();
            row.id(3);
            assertFalse(mTable.exists(null, row));
        } else {
            // Regular transaction holds exclusive locks for rows that were deleted.
            TestRow row = mTable.newRow();
            row.id(3);
            try {
                mTable.exists(null, row);
                fail();
            } catch (LockTimeoutException e) {
            }

            // No lock is held for a row which was filtered out.
            Transaction txn2 = mDb.newTransaction();
            row.id(1);
            assertTrue(mTable.tryDelete(txn2, row));
            txn2.reset(); // rollback

            // A lock might be held for a row which was manually filtered out by calling step.
            row.id(4);
            if (!txn.lockMode().isRepeatable()) {
                assertTrue(mTable.tryDelete(txn2, row));
            } else {
                try {
                    mTable.delete(txn2, row);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }
            txn2.reset(); // rollback

            txn.commit();
        }

        Scanner<TestRow> scanner = mTable.newScanner
            (null, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "str2 9");

        int count = 0;
        for (TestRow row = scanner.row(); row != null; row = scanner.step()) {
            count++;
            assertTrue(row.toString().endsWith("{id=4, num1=1004, str1=s1-4, str2=s2-4}"));
        }
        assertEquals(1, count);
    }

    @Test
    public void basicUpdaterUpdateAutoCommit() throws Exception {
        basicUpdaterUpdate(null);
    }

    @Test
    public void basicUpdaterUpdateUpgradable() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        basicUpdaterUpdate(txn);
    }

    @Test
    public void basicUpdaterUpdateRepeatable() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.REPEATABLE_READ);
        basicUpdaterUpdate(txn);
    }

    @Test
    public void basicUpdaterUpdateCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        basicUpdaterUpdate(txn);
    }

    @Test
    public void basicUpdaterUpdateUncommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_UNCOMMITTED);
        basicUpdaterUpdate(txn);
    }

    private void basicUpdaterUpdate(Transaction txn) throws Exception {
        for (int i=1; i<=10; i++) {
            TestRow row = mTable.newRow();
            row.id(i);
            row.num1(1000 + i);
            row.str1("s1-" + i);
            row.str2("s2-" + i);
            mTable.store(null, row);
        }

        Updater<TestRow> updater = mTable.newUpdater
            (txn, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "s2-9");

        for (TestRow row = updater.row(); row != null; ) {
            if (row.num1() == 1004) {
                row = updater.step();
            } else {
                row.str2(row.str2() + "x");
                if (row.id() == 3) {
                    row.id(1003);
                    row = updater.update();
                } else {
                    if (row.id() == 5) {
                        try {
                            row.id(6);
                            row = updater.update();
                            fail();
                        } catch (UniqueConstraintException e) {
                            row.id(5);
                        }
                    }
                    row = updater.update(row);
                }
            }
        }

        assertEquals(10, mDb.findIndex(TestRow.class.getName()).count(null, null));

        {
            // Can load the row that was skipped because an exclusive lock wasn't acquired.
            TestRow row = mTable.newRow();
            row.id(4);
            assertTrue(mTable.exists(null, row));
        }

        final long checkId = 1003;

        if (txn == null) {
            // Auto-commit transaction releases the exclusive lock.
            TestRow row = mTable.newRow();
            row.id(checkId);
            mTable.load(null, row);
            assertEquals("s2-3x", row.str2());
        } else {
            // Regular transaction holds exclusive locks for rows that were updated.
            TestRow row = mTable.newRow();
            row.id(checkId);
            try {
                mTable.exists(null, row);
                fail();
            } catch (LockTimeoutException e) {
            }

            // No lock is held for a row which was filtered out.
            Transaction txn2 = mDb.newTransaction();
            row.id(1);
            assertTrue(mTable.tryDelete(txn2, row));
            txn2.reset(); // rollback

            // A lock might be held for a row which was manually filtered out by calling step.
            row.id(4);
            if (!txn.lockMode().isRepeatable()) {
                assertTrue(mTable.tryDelete(txn2, row));
            } else {
                try {
                    mTable.delete(txn2, row);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }
            txn2.reset(); // rollback

            txn.commit();
        }

        Scanner<TestRow> scanner = mTable.newScanner
            (null, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "s2-9x");

        int count = 0;
        for (TestRow row = scanner.row(); row != null; row = scanner.step()) {
            count++;

            String rowStr = row.toString();

            switch (row.num1()) {
                default -> fail();
                case 1003 -> assertTrue(rowStr.endsWith("{id=1003, num1=1003, str1=s1-3, str2=s2-3x}"));
                case 1004 -> assertTrue(rowStr.endsWith("{id=4, num1=1004, str1=s1-4, str2=s2-4}"));
                case 1005 -> assertTrue(rowStr.endsWith("{id=5, num1=1005, str1=s1-5, str2=s2-5x}"));
                case 1009 -> assertTrue(rowStr.endsWith("{id=9, num1=1009, str1=s1-9, str2=s2-9x}"));
            }
        }

        assertEquals(4, count);
    }

    @Test
    public void updateFiltered() throws Exception {
        // Test updating a row's scanner position when a predicate is checked.

        for (int i=1; i<=5; i++) {
            TestRow row = mTable.newRow();
            row.id(i);
            row.num1(1000 + i);
            row.str1("s1-" + i);
            row.str2("s2-" + i);
            mTable.store(null, row);
        }

        Updater<TestRow> updater = mTable.newUpdater(null, "id == ? || id == ?", 2, 4);

        for (TestRow row = updater.row(); row != null; ) {
            if (row.id() == 2) {
                row.id(30);
            } else if (row.id() == 4) {
                row.id(-40);
            } else {
                fail();
            }
            row = updater.update();
        }

        mTable.newStream(null).forEach(row -> {
            long id = row.id();
            assertTrue(id == 1 || id == 3 || id == 5 || id == 30 || id == -40);
        });
    }

    @Test
    public void updateEntry() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<Entry> table = ix.asTable(Entry.class);

        Entry e = table.newRow();
        try {
            table.update(null, e);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("Primary key isn't fully specified"));
        }

        e.key("hello".getBytes());
        try {
            table.update(null, e);
            fail();
        } catch (NoSuchRowException ex) {
        }

        e.value("world".getBytes());
        try {
            table.update(null, e);
            fail();
        } catch (NoSuchRowException ex) {
        }

        try {
            table.merge(null, e);
            fail();
        } catch (NoSuchRowException ex) {
        }

        table.insert(null, e);

        e.value("world!".getBytes());
        table.update(null, e);

        assertArrayEquals("world!".getBytes(), ix.load(null, e.key()));

        table.unsetRow(e);
        e.key("hello".getBytes());

        table.merge(null, e);
        assertArrayEquals("world!".getBytes(), e.value());

        e.value("world!!!".getBytes());
        table.merge(null, e);
        assertArrayEquals("world!!!".getBytes(), e.value());

        assertArrayEquals("world!!!".getBytes(), ix.load(null, e.key()));
    }
}
