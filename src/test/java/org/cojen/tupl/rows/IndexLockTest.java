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

import static org.cojen.tupl.TestUtils.*;

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
        if (mDatabase != null) {
            mDatabase.close();
            mDatabase = null;
        }
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
            predicateLockTimeout(e);
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
            predicateLockTimeout(e);
        }

        // Exclude the row being inserted by key, and then the scan can begin.
        var scanner = newRowScanner(table, updater, scanTxn,
                                    "id >= ? && id <= ? && id != ?", 3, 7, 5);
        assertEquals(3, scanner.row().id());
        try {
            scanner.step();
            fail();
        } catch (LockTimeoutException e) {
            rowLockTimeout(e);
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
                predicateLockTimeout(e);
            }

            // Exclude the row being inserted, and then the scan can begin.
            scanner = nameIx.newRowScanner(scanTxn, "name >= ? && name <= ? && name != ?",
                                           "name-3", "name-7", "name-5");
            assertEquals(3, scanner.row().id());
            try {
                scanner.step();
                fail();
            } catch (LockTimeoutException e) {
                rowLockTimeout(e);
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
                rowLockTimeout(e);
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
    public void rowLockStall() throws Exception {
        // A RowScanner which returns at most one row doesn't install a predicate lock, but it
        // can still stall on a row lock held by another transaction.

        var table = mDatabase.openTable(TestRow.class);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        TestRow row = table.newRow();
        row.id(5);
        row.name("name-5");
        table.insert(txn1, row);

        Transaction scanTxn = mDatabase.newTransaction();
        try {
            table.newRowScanner(scanTxn, "id == ?", 5);
            fail();
        } catch (LockTimeoutException e) {
            rowLockTimeout(e);
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
            predicateLockTimeout(e);
        }

        try {
            table.store(insertTxn, row);
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
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
                predicateLockTimeout(e);
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

    @Test
    public void deadlock() throws Exception {
        // Force a deadlock by moving a row. This is caused by deleting the old row before
        // inserting a replacement.

        var table = mDatabase.openTable(TestRow.class);

        fill(table, 0, 3);

        // The predicate lock will guard the whole table.
        Transaction txn1 = mDatabase.newTransaction();
        // Be nice and don't retain row locks, but this isn't enough.
        txn1.lockMode(LockMode.READ_COMMITTED);
        var scanner = table.newRowScanner(txn1);

        // Move a row by deleting before inserting. Deletes don't acquire a predicate lock.
        Transaction txn2 = mDatabase.newTransaction();
        var row = table.newRow();
        row.id(1);
        table.load(txn2, row);
        table.delete(txn2, row);

        assertEquals(0, scanner.row().id());

        // The scanner will be blocked on the row being deleted.
        txn1.lockTimeout(2, TimeUnit.SECONDS);
        Waiter w1 = start(() -> {
            scanner.step();
        });

        // Finish the move by inserting, which must acquire the predicate lock.
        row.id(100);
        try {
            table.insert(txn2, row);
            fail();
        } catch (DeadlockException e) {
            predicateLockTimeout(e);
        }

        txn2.exit();

        w1.await();
        scanner.close();
        txn1.exit();
    }

    @Test
    public void noDeadlock() throws Exception {
        noDeadlock(LockMode.READ_COMMITTED);
    }

    @Test
    public void noDeadlock2() throws Exception {
        // Same as noDeadlock, but with a stronger scan lock mode.
        noDeadlock(LockMode.REPEATABLE_READ);
    }

    @Test
    public void noDeadlock3() throws Exception {
        // The name of this test is a lie. Deadlock is expected when the scan uses the
        // strongest locking mode.
        try {
            noDeadlock(LockMode.UPGRADABLE_READ);
            fail();
        } catch (DeadlockException e) {
            rowLockTimeout(e);
        }
    }

    private void noDeadlock(LockMode scanLockMode) throws Exception {
        // Attempt a row move without deadlock by inserting before deleting. Note that if the
        // scanner uses the UPGRADABLE_READ lock mode, a deadlock is caused by the load
        // operation, which also needs UPGRADABLE_READ to finish the delete.

        var table = mDatabase.openTable(TestRow.class);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        txn1.lockMode(scanLockMode);
        var scanner = table.newRowScanner(txn1);

        Transaction txn2 = mDatabase.newTransaction();
        var row = table.newRow();
        row.id(1);
        table.load(txn2, row); // needs UPGRADABLE_READ lock mode
        row.id(100);

        // Begin the move by inserting, which must acquire the predicate lock.
        txn2.lockTimeout(2, TimeUnit.SECONDS);
        Waiter w1 = start(() -> {
            table.insert(txn2, row);
        });

        for (int i=0; i<=3; i++) {
            assertEquals(i, scanner.row().id());
            scanner.step();
        }

        assertNull(scanner.row());

        if (scanLockMode == LockMode.REPEATABLE_READ) {
            // Must forcibly release the row locks.
            scanner.close();
            txn1.exit();
        }

        // The scanner has finished, and so the move operation is unblocked.
        w1.await();

        row.id(1);
        table.delete(txn2, row);
        txn2.commit();

        scanner.close();
        txn1.exit();

        // Verify the row move.

        scanner = table.newRowScanner(null);

        long[] expect = {0, 2, 3, 100};

        for (int i=0; i<=3; i++) {
            long id = scanner.row().id();
            assertEquals(expect[i], id);
            scanner.step();
        }

        assertNull(scanner.row());
    }

    @Test
    public void replicaScanStall() throws Exception {
        // A replica RowScanner cannot start when an open transaction has inserted a row into
        // the range that will be scanned.

        teardown(); // discard pre-built Database instance

        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().directPageAccess(false).replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = newTempDatabase(getClass(), config);

        var leaderTable = leaderDb.openTable(TestRow.class);

        fill(leaderTable, 0, 3);

        Transaction txn1 = leaderDb.newTransaction();
        TestRow row = leaderTable.newRow();
        row.id(5);
        row.name("name-5");
        leaderTable.insert(txn1, row);
        txn1.flush();

        fence(leaderRepl, replicaRepl);

        var replicaTable = replicaDb.openTable(TestRow.class);

        // Scan is blocked.

        Transaction scanTxn = replicaDb.newTransaction();
        try {
            replicaTable.newRowScanner(scanTxn, "id >= ? && id <= ?", 3, 7);
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        // Scan can start when txn1 finishes.

        scanTxn.reset();
        scanTxn.lockTimeout(2, TimeUnit.SECONDS);

        Waiter w1 = start(() -> {
            var scanner = replicaTable.newRowScanner(scanTxn, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner.row().id());
            scanner.step();
            assertEquals(5, scanner.row().id());
            scanner.close();
            scanTxn.reset();
        });

        txn1.commit();
        fence(leaderRepl, replicaRepl);

        w1.await();

        leaderDb.close();
        replicaDb.close();
    }

    @Test
    public void replicaBlockedByScanner() throws Exception {
        // An incoming replica insert or store operation must wait for a dependent scan to
        // finish. Unfortunately, this stalls replication, but at least it's safe.

        teardown(); // discard pre-built Database instance

        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().directPageAccess(false).replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = newTempDatabase(getClass(), config);

        var leaderTable = leaderDb.openTable(TestRow.class);

        fill(leaderTable, 0, 3);

        fence(leaderRepl, replicaRepl);

        var replicaTable = replicaDb.openTable(TestRow.class);

        Transaction txn1 = replicaDb.newTransaction();
        txn1.lockMode(LockMode.READ_COMMITTED);
        var scanner = replicaTable.newRowScanner(txn1, "id >= ? && id <= ?", 3, 7);

        // This store is blocked on the replica side.
        TestRow row = leaderTable.newRow();
        row.id(5);
        row.name("name-5");
        leaderTable.store(null, row);

        // Even the fence operation is blocked now.

        Waiter w1 = start(() -> {
            long start = System.nanoTime();
            fence(leaderRepl, replicaRepl);
            long end = System.nanoTime();
            assertTrue((end - start) >= 1_000_000_000L);
        });

        sleep(1000);

        assertFalse(replicaTable.load(null, row));

        assertEquals(3, scanner.row().id());

        // Stepping to the end closes the scanner, which releases the predicate lock.
        assertNull(scanner.step());

        w1.await();

        assertTrue(replicaTable.load(null, row));
        assertEquals("name-5", row.name());

        leaderDb.close();
        replicaDb.close();
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

    /**
     * Writes a fence to the leader and waits for the replica to catch up.
     */
    private static void fence(SocketReplicator leaderRepl, SocketReplicator replicaRepl)
        throws Exception
    {
        byte[] message = ("fence:" + System.nanoTime()).getBytes();
        leaderRepl.writeControl(message);
        replicaRepl.waitForControl(message);
    }

    private static <R extends TestRow> RowScanner<R> newRowScanner
        (Table<R> table, boolean updater, Transaction txn, String filter, Object... args)
        throws Exception
    {
        return updater ? table.newRowUpdater(txn, filter, args)
            : table.newRowScanner(txn, filter, args);
    }

    private static void predicateLockTimeout(LockTimeoutException e) throws LockTimeoutException {
        for (StackTraceElement elem : e.getStackTrace()) {
            if (elem.getClassName().contains("RowPredicateLock")) {
                return;
            }
        }
        throw e;
    }

    private static void rowLockTimeout(LockTimeoutException e) throws LockTimeoutException {
        for (StackTraceElement elem : e.getStackTrace()) {
            if (elem.getClassName().contains("BTreeCursor")) {
                return;
            }
        }
        throw e;
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
