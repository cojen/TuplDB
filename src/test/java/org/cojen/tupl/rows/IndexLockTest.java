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

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class IndexLockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IndexLockTest.class.getName());
    }

    private Database mDatabase;

    @Before
    public void begin() throws Exception {
        mDatabase = Database.open(new DatabaseConfig()
                                  .directPageAccess(false)
                                  .lockTimeout(100, TimeUnit.MILLISECONDS));
    }
    
    @After
    public void teardown() throws Exception {
        mDatabase.close();
    }

    @Test
    public void scanStall() throws Exception {
        scanStall(TestRow.class, false);
    }

    @Test
    public void scanStall2() throws Exception {
        // Run same tests as scanStall, except against a table that has a secondary index.
        scanStall(TestRow2.class, false);
    }

    @Test
    public void updaterStall() throws Exception {
        // Run same tests as scanStall, except with an updater.
        scanStall(TestRow.class, true);
    }

    @Test
    public void updaterStall2() throws Exception {
        // Run same tests as updaterStall, except against a table that has a secondary index.
        scanStall(TestRow2.class, true);
    }

    private <R extends TestRow> void scanStall(Class<R> type, boolean updater) throws Exception {
        // A RowScanner cannot start when an open transaction has inserted a row into the range
        // that will be scanned.

        var table = mDatabase.openTable(type);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        R row = table.newRow();
        row.id(5);
        row.name("name-5");
        table.insert(txn1, row);

        Transaction scanTxn = mDatabase.newTransaction();
        try {
            newRowScanner(table, updater, scanTxn, "id >= ? && id <= ?", 3, 7);
            fail();
        } catch (LockTimeoutException e) {
            // Predicate lock.
        }

        // Attempt to exclude the row being inserted by a non-key filter, but this won't work.
        // Checks against existing locks only consider the key. Checking the value is tricky,
        // since it requires examing the transaction's undo log. Even if the scan could start
        // right away, it would still stall once it hit the row with the lock held. Filtering
        // operations cannot skip lock acquisition.
        try {
            newRowScanner(table, updater, scanTxn,
                          "id >= ? && id <= ? && name != ?", 3, 7, "name-5");
            fail();
        } catch (LockTimeoutException e) {
            // Predicate lock.
        }

        // Exclude the row being inserted by key, and then the scan can begin.
        var scanner = newRowScanner(table, updater, scanTxn,
                                    "id >= ? && id <= ? && id != ?", 3, 7, 5);
        assertEquals(3, scanner.row().id());
        try {
            scanner.step();
            fail();
        } catch (LockTimeoutException e) {
            // Row lock.
        }

        scanner.close();
        scanTxn.reset();

        // Null transaction doesn't check the predicate lock.
        scanner = newRowScanner(table, updater, null, "id >= ? && id <= ?", 3, 7);
        assertEquals(3, scanner.row().id());
        scanner.close();

        if (!updater && type == TestRow2.class) {
            // Scans over a secondary index are also blocked.

            var nameIx = table.viewSecondaryIndex("name");

            try {
                nameIx.newRowScanner(scanTxn, "name >= ? && name <= ?", "name-3", "name-7");
                fail();
            } catch (LockTimeoutException e) {
                // Predicate lock.
            }

            // Exclude the row being inserted, and then the scan can begin.
            scanner = nameIx.newRowScanner(scanTxn, "name >= ? && name <= ? && name != ?",
                                           "name-3", "name-7", "name-5");
            assertEquals(3, scanner.row().id());
            try {
                scanner.step();
                fail();
            } catch (LockTimeoutException e) {
                // Row lock.
            }

            scanner.close();
            scanTxn.reset();

            // Also works when checking by id, because id is part of the secondary key.
            scanner = nameIx.newRowScanner(scanTxn, "name >= ? && name <= ? && id != ?",
                                           "name-3", "name-7", 5);
            assertEquals(3, scanner.row().id());
            try {
                scanner.step();
                fail();
            } catch (LockTimeoutException e) {
                // Row lock.
            }

            scanner.close();
            scanTxn.reset();
        }

        // Blocked until txn1 finishes.
        Waiter w2 = start(() -> {
            Transaction scanTxn2 = mDatabase.newTransaction();
            scanTxn2.lockTimeout(2, TimeUnit.SECONDS);
            var scanner2 = newRowScanner(table, updater, scanTxn2, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner2.row().id());
            scanner2.step();
            assertEquals(5, scanner2.row().id());
            scanner2.close();
            scanTxn2.reset();
        });

        Waiter w3 = null;

        if (!updater && type == TestRow2.class) {
            // Scans over a secondary index are blocked until txn1 finishes.

            var nameIx = table.viewSecondaryIndex("name");

            w3 = start(() -> {
                Transaction scanTxn2 = mDatabase.newTransaction();
                scanTxn2.lockTimeout(2, TimeUnit.SECONDS);
                var scanner2 = nameIx.newRowScanner
                    (scanTxn2, "name >= ? && name <= ?", "name-3", "name-7");
                assertEquals(3, scanner2.row().id());
                scanner2.step();
                assertEquals(5, scanner2.row().id());
                scanner2.close();
                scanTxn2.reset();
            });
        }

        txn1.commit();

        w2.await();

        if (w3 != null) {
            w3.await();
        }
    }

    @Test
    public void blockedByScanner() throws Exception {
        blockedByScanner(TestRow.class, false);
    }

    @Test
    public void blockedByScanner2() throws Exception {
        // Run same tests as blockedByScanner, except against a table that has a secondary index.
        blockedByScanner(TestRow2.class, false);
    }

    @Test
    public void blockedByUpdater() throws Exception {
        // Run same tests as blockedByScanner, except with an updater.
        blockedByScanner(TestRow.class, true);
    }

    @Test
    public void blockedByUpdater2() throws Exception {
        // Run same tests as blockedByUpdater, except against a table that has a secondary index.
        blockedByScanner(TestRow2.class, true);
    }

    private <R extends TestRow> void blockedByScanner(Class<R> type, boolean updater)
        throws Exception
    {
        // An insert or store operation must wait for a dependent scan to finish.

        var table = mDatabase.openTable(type);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        var scanner = newRowScanner(table, updater, txn1, "id >= ? && id <= ? && id != ?", 3, 7, 6);

        Transaction insertTxn = mDatabase.newTransaction();
        R row = table.newRow();
        row.id(5);
        row.name("name-5");

        try {
            table.insert(insertTxn, row);
            fail();
        } catch (LockTimeoutException e) {
            // Predicate lock.
        }

        try {
            table.store(insertTxn, row);
            fail();
        } catch (LockTimeoutException e) {
            // Predicate lock.
        }

        // Inserting against a row not part of the scan isn't blocked.
        row.id(6);
        row.name("name-6");
        table.store(insertTxn, row);
        insertTxn.commit();

        if (!updater) {
            // Scanners don't block each other, except via row locks.
            Transaction txn2 = mDatabase.newTransaction();
            txn2.lockMode(LockMode.READ_COMMITTED);
            var scanner2 = newRowScanner(table, updater, txn2, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner2.row().id());
            scanner2.step();
            assertEquals(6, scanner2.row().id());
            scanner2.close();
            txn2.reset();
        }

        if (!updater) {
            // RowUpdater is blocked when updating out of order.
            Transaction txn2 = mDatabase.newTransaction();
            var updater2 = table.newRowUpdater(txn2, "id >= ?", 5);
            assertEquals(6, updater2.row().id());
            updater2.row().id(5);

            try {
                updater2.update();
                fail();
            } catch (LockTimeoutException e) {
                // Predicate lock.
            }

            updater2.close();
            txn2.commit();
        }

        // Blocked until txn1 finishes.
        Waiter w2 = start(() -> {
            var insertTxn2 = mDatabase.newTransaction();
            insertTxn2.lockTimeout(2, TimeUnit.SECONDS);
            R row2 = table.newRow();
            row2.id(5);
            row2.name("name-5");
            table.insert(insertTxn2, row2);
            insertTxn2.commit();
        });

        txn1.commit();

        w2.await();

        scanner.close();
        scanner = newRowScanner(table, updater, null, "id >= ? && id <= ?", 3, 7);
        assertEquals(3, scanner.row().id());
        scanner.step();
        assertEquals(5, scanner.row().id());
        scanner.close();
    }

    private static <R extends TestRow> void fill(Table<R> table, int start, int end)
        throws Exception
    {
        for (int i = start; i <= end; i++) {
            R row = table.newRow();
            row.id(i);
            row.name("name-" + i);
            table.store(null, row);
        }
    }

    private static <R extends TestRow> RowScanner<R> newRowScanner
        (Table<R> table, boolean updater, Transaction txn, String filter, Object... args)
        throws Exception
    {
        return updater ? table.newRowUpdater(txn, filter, args)
            : table.newRowScanner(txn, filter, args);
    }

    static interface Task {
        void run() throws Exception;
    }

    static class Waiter extends Thread {
        final Task mTask;

        volatile Throwable mFailed;

        Waiter(Task task) {
            mTask = task;
        }

        void await() throws Exception {
            join();
            Throwable failed = mFailed;
            if (failed != null) {
                Utils.addLocalTrace(failed);
                Utils.rethrow(failed);
            }
        }

        @Override
        public void run() {
            try {
                mTask.run();
            } catch (Throwable e) {
                mFailed = e;
            }
        }
    }

    static Waiter start(Task task) {
        return TestUtils.startAndWaitUntilBlocked(new Waiter(task));
    }

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String str);
    }

    @PrimaryKey("id")
    @SecondaryIndex("name")
    public interface TestRow2 extends TestRow {
    }
}
