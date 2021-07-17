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

package org.cojen.tupl.rows;

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
        mView = mDb.openRowView(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private Database mDb;
    private RowView<TestRow> mView;

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
        assertEquals(TestRow.class, mView.rowType());
        assertTrue(mView.isEmpty());
        assertTrue(mView == mDb.openRowView(TestRow.class));

        TestRow row = mView.newRow();
        assertTrue(row.toString().endsWith("TestRow{}"));
        mView.reset(row);
        assertTrue(row.toString().endsWith("TestRow{}"));

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

        try {
            mView.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        try {
            mView.exists(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        assertEquals("hello", row.str1());

        row.id(1);
        assertFalse(mView.load(null, row));

        assertTrue(row.toString().endsWith("TestRow{id=1}"));

        try {
            row.str1();
            fail();
        } catch (UnsetColumnException e) {
        }

        assertEquals(1, row.id());

        try {
            mView.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Some required columns are unset: num1, str1, str2", e.getMessage());
        }

        assertFalse(mView.delete(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1}"));

        row.str1("hello");
        row.str2(null);
        row.num1(100);
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=null}"));

        assertTrue(mView.insert(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=null}"));
        assertFalse(mView.isEmpty());
        assertTrue(mView.exists(null, row));
        assertTrue(mView.load(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=null}"));

        TestRow row2 = mView.newRow();
        row2.id(1);
        assertTrue(mView.load(null, row2));
        assertEquals(row, row2);
        assertEquals(row.hashCode(), row2.hashCode());
        assertEquals(row.toString(), row2.toString());
        assertFalse(mView.insert(null, row2));

        row2.str2("world");
        assertTrue(mView.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=world}"));

        row.str1("howdy");
        assertTrue(mView.update(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=howdy, str2=null}"));
        row.str1("hi");
        assertTrue(mView.merge(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        row2.num1(-555);
        assertTrue(mView.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=-555, str1=hello, str2=world}"));

        mView.reset(row2);
        row2.id(1);
        row2.num1(999);
        assertTrue(mView.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=999}"));

        row2.str2("everyone");
        assertTrue(mView.merge(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=999, str1=hi, str2=everyone}"));

        assertTrue(mView.replace(null, row));
        mView.load(null, row2);
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        assertTrue(mView.delete(null, row2));
        assertFalse(mView.delete(null, row));
        assertTrue(mView.isEmpty());

        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        Transaction txn = mView.newTransaction(null);
        assertTrue(mView.insert(txn, row));
        assertTrue(mView.exists(txn, row2));
        assertFalse(mView.isEmpty());
        txn.reset(); // rollback

        assertFalse(mView.exists(null, row2));
        assertTrue(mView.isEmpty());

        mView.store(null, row);
        row2.str1("hello");
        TestRow row3 = mView.exchange(null, row2);
        assertEquals(row, row3);
        mView.delete(null, row3);
        assertNull(mView.exchange(null, row2));
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
            TestRow row = mView.newRow();
            row.id(i);
            row.num1(1000 + i);
            row.str1("s1-" + i);
            row.str2("s2-" + i);
            mView.store(null, row);
        }

        RowUpdater<TestRow> updater = mView.newRowUpdater
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
            TestRow row = mView.newRow();
            row.id(4);
            assertTrue(mView.exists(null, row));
        }

        if (txn == null) {
            // Auto-commit transaction releases the exlusive lock.
            TestRow row = mView.newRow();
            row.id(3);
            assertFalse(mView.exists(null, row));
        } else {
            // Regular transaction holds exlusive locks for rows that were deleted.
            TestRow row = mView.newRow();
            row.id(3);
            try {
                mView.exists(null, row);
                fail();
            } catch (LockTimeoutException e) {
            }

            // No lock is held for a row which was filtered out.
            Transaction txn2 = mDb.newTransaction();
            row.id(1);
            assertTrue(mView.delete(txn2, row));
            txn2.reset(); // rollback

            // A lock might be held for a row which was manually filtered out by calling step.
            row.id(4);
            if (!txn.lockMode().isRepeatable()) {
                assertTrue(mView.delete(txn2, row));
            } else {
                try {
                    mView.delete(txn2, row);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }
            txn2.reset(); // rollback

            txn.commit();
        }

        RowScanner<TestRow> scanner = mView.newRowScanner
            (null, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "str2 9");

        int count = 0;
        for (TestRow row = scanner.row(); row != null; row = scanner.step()) {
            count++;
            assertTrue(row.toString().endsWith("TestRow{id=4, num1=1004, str1=s1-4, str2=s2-4}"));
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
            TestRow row = mView.newRow();
            row.id(i);
            row.num1(1000 + i);
            row.str1("s1-" + i);
            row.str2("s2-" + i);
            mView.store(null, row);
        }

        RowUpdater<TestRow> updater = mView.newRowUpdater
            (txn, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "s2-9");

        for (TestRow row = updater.row(); row != null; ) {
            if (row.num1() == 1004 || row.id() == 1003) {
                row = updater.step();
            } else {
                row.str2(row.str2() + "x");
                if (false && row.id() == 3) { // FIXME: Support snapshot.
                    // Changing the primary key is allowed, but it can cause the row to be
                    // observed again, depending on the scan order.
                    row.id(1003);
                    row = updater.update();
                } else {
                    row = updater.update(row);
                }
            }
        }

        assertEquals(10, mDb.findIndex(TestRow.class.getName()).count(null, null));

        {
            // Can load the row that was skipped because an exclusive lock wasn't acquired.
            TestRow row = mView.newRow();
            row.id(4);
            assertTrue(mView.exists(null, row));
        }

        final long checkId = 3; // FIXME: 1003

        if (txn == null) {
            // Auto-commit transaction releases the exlusive lock.
            TestRow row = mView.newRow();
            row.id(checkId);
            mView.load(null, row);
            assertEquals("s2-3x", row.str2());
        } else {
            // Regular transaction holds exlusive locks for rows that were updated.
            TestRow row = mView.newRow();
            row.id(checkId);
            try {
                mView.exists(null, row);
                fail();
            } catch (LockTimeoutException e) {
            }

            // No lock is held for a row which was filtered out.
            Transaction txn2 = mDb.newTransaction();
            row.id(1);
            assertTrue(mView.delete(txn2, row));
            txn2.reset(); // rollback

            // A lock might be held for a row which was manually filtered out by calling step.
            row.id(4);
            if (!txn.lockMode().isRepeatable()) {
                assertTrue(mView.delete(txn2, row));
            } else {
                try {
                    mView.delete(txn2, row);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }
            txn2.reset(); // rollback

            txn.commit();
        }

        RowScanner<TestRow> scanner = mView.newRowScanner
            (null, "num1 > ? && num1 < ? || str2 == ?", 1002, 1006, "s2-9x");

        int count = 0;
        for (TestRow row = scanner.row(); row != null; row = scanner.step()) {
            count++;

            String rowStr = row.toString();

            switch (row.num1()) {
            default: fail(); break;

            case 1003:
                //assertTrue(rowStr.endsWith("TestRow{id=1003, num1=1003, str1=s1-3, str2=s2-3x}"));
                assertTrue(rowStr.endsWith("TestRow{id=3, num1=1003, str1=s1-3, str2=s2-3x}"));
                break;

            case 1004:
                assertTrue(rowStr.endsWith("TestRow{id=4, num1=1004, str1=s1-4, str2=s2-4}"));
                break;

            case 1005:
                assertTrue(rowStr.endsWith("TestRow{id=5, num1=1005, str1=s1-5, str2=s2-5x}"));
                break;

            case 1009:
                assertTrue(rowStr.endsWith("TestRow{id=9, num1=1009, str1=s1-9, str2=s2-9x}"));
                break;
            }
        }

        assertEquals(4, count);
    }
}
