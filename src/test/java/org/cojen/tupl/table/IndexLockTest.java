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

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.Utils;

import org.cojen.tupl.diag.DeadlockInfo;

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
                                  .lockTimeout(100, TimeUnit.MILLISECONDS));
    }
    
    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());

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
        // A Scanner cannot start when an open transaction has inserted a row into the range
        // that will be scanned.

        var table = (StoredTable<R>) mDatabase.openTable(type);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        R row = table.newRow();
        row.id(5);
        row.name("name-5");
        table.insert(txn1, row);

        Transaction scanTxn = mDatabase.newTransaction();
        try {
            newScanner(table, updater, scanTxn, "id >= ? && id <= ?", 3, 7);
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
            newScanner(table, updater, scanTxn,
                       "id >= ? && id <= ? && name != ?", 3, 7, "name-5");
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        // Exclude the row being inserted by key, and then the scan can begin.
        var scanner = newScanner(table, updater, scanTxn,
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

        try {
            // Need an explicit transaction to add a predicate lock. READ_COMMITTED is the
            // lowest level that does this.
            Transaction txn = mDatabase.newTransaction();
            try {
                txn.lockMode(LockMode.READ_COMMITTED);
                newScanner(table, updater, txn, "id >= ? && id <= ?", 3, 7);
            } finally {
                txn.exit();
            }
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        if (!updater && type == TestRow2.class) {
            // Scans over a secondary index are also blocked.

            var nameIx = table.viewSecondaryIndex("name").viewUnjoined();

            try {
                nameIx.newScanner(scanTxn, "name >= ? && name <= ?", "name-3", "name-7");
                fail();
            } catch (LockTimeoutException e) {
                predicateLockTimeout(e);
            }

            // Exclude the row being inserted, and then the scan can begin.
            scanner = nameIx.newScanner(scanTxn, "name >= ? && name <= ? && name != ?",
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
            scanner = nameIx.newScanner(scanTxn, "name >= ? && name <= ? && id != ?",
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
            var scanner2 = newScanner(table, updater, scanTxn2, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner2.row().id());
            scanner2.step();
            assertEquals(5, scanner2.row().id());
            scanner2.close();
            scanTxn2.reset();
        });

        Waiter w3 = null;

        if (!updater && type == TestRow2.class) {
            // Scans over a secondary index are blocked until txn1 finishes.

            var nameIx = table.viewSecondaryIndex("name").viewUnjoined();

            w3 = start(() -> {
                Transaction scanTxn2 = mDatabase.newTransaction();
                scanTxn2.lockTimeout(2, TimeUnit.SECONDS);
                var scanner2 = nameIx.newScanner
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
        // A Scanner which returns at most one row doesn't need to install a predicate lock,
        // but it can still stall on a row lock held by another transaction.

        var table = mDatabase.openTable(TestRow.class);

        fill(table, 0, 3);

        Transaction txn1 = mDatabase.newTransaction();
        TestRow row = table.newRow();
        row.id(5);
        row.name("name-5");
        table.insert(txn1, row);

        Transaction scanTxn = mDatabase.newTransaction();
        try {
            table.newScanner(scanTxn, "id == ?", 5);
            fail();
        } catch (LockTimeoutException e) {
            // Can be caused by a row lock timeout or a predicate lock timeout.
        }
    }

    @Test
    public void rowLockStall2() throws Exception {
        // A Scanner which returns at most one row doesn't need to install a predicate lock,
        // but it should still acquire a lock even when the row doesn't exist.

        var table = mDatabase.openTable(TestRow.class);

        Transaction txn1 = mDatabase.newTransaction();
        table.newScanner(txn1, "id == ?", 5);

        Transaction txn2 = mDatabase.newTransaction();
        TestRow row = table.newRow();
        row.id(5);
        row.name("name-5");

        try {
            table.insert(txn2, row);
            fail();
        } catch (LockTimeoutException e) {
            // Can be caused by a row lock timeout or a predicate lock timeout.
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
        var scanner = newScanner(table, updater, txn1, "id >= ? && id <= ? && id != ?", 3, 7, 6);

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
            var scanner2 = table.newScanner(txn2, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner2.row().id());
            scanner2.step();
            assertEquals(6, scanner2.row().id());
            scanner2.close();
            txn2.reset();

            // Null transaction doesn't add a predicate lock. This scan doesn't require it
            // because no concurrent inserts are being performed.
            scanner2 = table.newScanner(null, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner2.row().id());
            scanner2.step();
            assertEquals(6, scanner2.row().id());
            scanner2.close();
        }

        if (!updater) {
            // Updater is blocked when updating out of order.
            Transaction txn2 = mDatabase.newTransaction();
            var updater2 = table.newUpdater(txn2, "id >= ?", 5);
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
        // Null transaction doesn't add a predicate lock. This scan doesn't require it
        // because no concurrent inserts are being performed.
        scanner = newScanner(table, updater, null, "id >= ? && id <= ?", 3, 7);
        assertEquals(3, scanner.row().id());
        scanner.step();
        assertEquals(5, scanner.row().id());
        scanner.close();
    }

    @Test
    public void deadlockAll() throws Exception {
        deadlock(false);
    }

    @Test
    public void deadlockWithFilter() throws Exception {
        deadlock(true);
    }

    private void deadlock(boolean withFilter) throws Exception {
        try {
            doDeadlock(withFilter);
        } catch (LockTimeoutException e) {
            // DeadlockException is sometimes missed when under load. Try again.
            teardown();
            begin();
            doDeadlock(withFilter);
        }
    }

    private void doDeadlock(boolean withFilter) throws Exception {
        // Force a deadlock by moving a row. This is caused by deleting the old row before
        // inserting a replacement.

        var table = mDatabase.openTable(TestRow.class);

        fill(table, 0, 3);

        // The predicate lock will guard the whole table.
        Transaction txn1 = mDatabase.newTransaction();
        // Be nice and don't retain row locks, but this isn't enough.
        txn1.lockMode(LockMode.READ_COMMITTED);

        Scanner<TestRow> scanner;
        if (!withFilter) {
            scanner = table.newScanner(txn1);
        } else {
            // This shouldn't actually filter anything out.
            scanner = table.newScanner(txn1, "id >= ? && name != ?", -123, "xxx");
        }

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

            findRow: {
                for (DeadlockInfo info : e.deadlockSet()) {
                    Object r = info.row();
                    if (r != null) {
                        assertEquals(1, ((TestRow) r).id());
                        break findRow;
                    }
                }
                fail();
            }

            findFilter: if (withFilter) {
                for (DeadlockInfo info : e.deadlockSet()) {
                    if (info.toString().contains("id >= -123 && name != \"xxx\"")) {
                        break findFilter;
                    }
                }
                fail();
            }
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
        var scanner = table.newScanner(txn1);

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

        if (scanLockMode == LockMode.READ_COMMITTED) {
            // Must forcibly release the predicate lock.
            txn1.exit();
        } else if (scanLockMode == LockMode.REPEATABLE_READ) {
            // Must forcibly release the row locks and the predicate lock.
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

        // Null transaction doesn't add a predicate lock. This scan doesn't require it
        // because no concurrent inserts are being performed.
        scanner = table.newScanner(null);

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
        replicaScanStall(TestRow.class);
    }

    @Test
    public void replicaScanStall2() throws Exception {
        replicaScanStall(TestRow2.class);
    }

    private <R extends TestRow> void replicaScanStall(Class<R> type) throws Exception {
        // A replica Scanner cannot start when an open transaction has inserted a row into
        // the range that will be scanned.

        teardown(); // discard pre-built Database instance

        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = newTempDatabase(getClass(), config);

        var leaderTable = leaderDb.openTable(type);

        fill(leaderTable, 0, 3);

        Transaction txn1 = leaderDb.newTransaction();
        var row = leaderTable.newRow();
        row.id(5);
        row.name("name-5");
        leaderTable.insert(txn1, row);
        txn1.flush();

        fence(leaderRepl, replicaRepl);

        var replicaTable = replicaDb.openTable(type);

        // Scan is blocked.

        Transaction scanTxn = replicaDb.newTransaction();
        try {
            replicaTable.newScanner(scanTxn, "id >= ? && id <= ?", 3, 7);
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        // Scan can start when txn1 finishes.

        scanTxn.reset();
        scanTxn.lockTimeout(2, TimeUnit.SECONDS);

        Waiter w1 = start(() -> {
            var scanner = replicaTable.newScanner(scanTxn, "id >= ? && id <= ?", 3, 7);
            assertEquals(3, scanner.row().id());
            scanner.step();
            assertEquals(5, scanner.row().id());
            scanner.close();
            scanTxn.reset();
        });

        txn1.commit();
        fence(leaderRepl, replicaRepl);

        w1.await();

        // Block replica scan when leader uses a Updater.

        var updater = leaderTable.newUpdater(txn1, "id == ?", 5);
        updater.row().id(4);
        updater.update();
        txn1.flush();

        fence(leaderRepl, replicaRepl);

        scanTxn.reset();
        scanTxn.lockTimeout(100, TimeUnit.MILLISECONDS);

        try {
            replicaTable.newScanner(scanTxn, "id == ?", 5);
            fail();
        } catch (LockTimeoutException e) {
            rowLockTimeout(e);
        }

        try {
            replicaTable.newScanner(scanTxn, "id >= ? && id <= ?", 5, 5);
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        // Scan can start when txn1 finishes.

        scanTxn.reset();
        scanTxn.lockTimeout(2, TimeUnit.SECONDS);

        Waiter w2 = start(() -> {
            var scanner = replicaTable.newScanner(scanTxn, "id == ?", 5);
            assertNull(scanner.row());
            scanner.close();
            scanner = replicaTable.newScanner(scanTxn, "id == ?", 4);
            assertEquals(4, scanner.row().id());
            assertEquals("name-5", scanner.row().name());
            scanner.close();
            scanTxn.reset();
        });

        txn1.commit();
        fence(leaderRepl, replicaRepl);

        w2.await();

        // Block replica scan when leader uses a Updater, this time when key doesn't change.

        updater = leaderTable.newUpdater(txn1, "id == ?", 4);
        updater.row().name("newname");
        updater.update();
        txn1.flush();

        fence(leaderRepl, replicaRepl);

        scanTxn.reset();
        scanTxn.lockTimeout(100, TimeUnit.MILLISECONDS);

        try {
            replicaTable.newScanner(scanTxn, "id == ?", 4);
            fail();
        } catch (LockTimeoutException e) {
            // No predicate lock is used when the key doesn't change.
            rowLockTimeout(e);
        }

        // Scan can start when txn1 finishes.

        scanTxn.reset();
        scanTxn.lockTimeout(2, TimeUnit.SECONDS);

        Waiter w3 = start(() -> {
            var scanner = replicaTable.newScanner(scanTxn, "id == ?", 4);
            assertEquals(4, scanner.row().id());
            assertEquals("newname", scanner.row().name());
            scanner.close();
            scanTxn.reset();
        });

        txn1.commit();
        fence(leaderRepl, replicaRepl);

        w3.await();

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

        var config = new DatabaseConfig().replicate(leaderRepl);
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

        var scanner = replicaTable.newScanner(txn1, "id >= ? && id <= ?", 3, 7);

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

        assertFalse(replicaTable.tryLoad(null, row));

        assertEquals(3, scanner.row().id());

        // Exiting the transaction scope releases the predicate lock. The scanner is still
        // active and can continue without predicate lock protection.
        txn1.exit();

        w1.await();

        assertTrue(replicaTable.tryLoad(null, row));
        assertEquals("name-5", row.name());

        assertEquals("name-5", scanner.step().name());

        leaderDb.close();
        replicaDb.close();
    }

    @Test
    public void replicaNotBlockedByScanner() throws Exception {
        // Without a transaction, a scanner shouldn't block replication.

        teardown(); // discard pre-built Database instance

        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = newTempDatabase(getClass(), config);

        var leaderTable = leaderDb.openTable(TestRow.class);

        fill(leaderTable, 0, 3);

        fence(leaderRepl, replicaRepl);

        var replicaTable = replicaDb.openTable(TestRow.class);

        var scanner = replicaTable.newScanner(null, "id >= ? && id <= ?", 3, 7);

        // This store shouldn't be blocked on the replica side.
        TestRow row = leaderTable.newRow();
        row.id(5);
        row.name("name-5");
        leaderTable.store(null, row);

        fence(leaderRepl, replicaRepl);

        assertTrue(replicaTable.tryLoad(null, row));
        assertEquals("name-5", row.name());

        assertEquals(3, scanner.row().id());
        assertEquals("name-5", scanner.step().name());

        leaderDb.close();
        replicaDb.close();
    }

    @Test
    public void coveringIndex() throws Exception {
        // Test that a predicate lock guards updates to a covering index, when only the value
        // portion changes. The "path" column is the covering portion.

        // In the current index trigger implementation, new index entries are created with a
        // "store" operation, and this always acquires the predicate lock. If a later
        // optimization is implemented in which the predicate lock is acquired only when an
        // entry is actually inserted, then this test should fail unless special attention is
        // given to covering indexes.

        var table = (StoredTable<TestRow4>) mDatabase.openTable(TestRow4.class);

        for (int i=1; i<=3; i++) {
            TestRow4 row = table.newRow();
            row.id(-i);
            row.name("name-" + i);
            row.path("path-" + i);
            table.store(null, row);
        }

        Table<TestRow4> ix = table.viewSecondaryIndex("name", "id", "path").viewUnjoined();
        Transaction txn = mDatabase.newTransaction();
        Scanner<TestRow4> scanner = ix.newScanner(txn);

        TestRow4 row = table.newRow();
        row.id(-2);
        row.path("newpath");
        try {
            table.merge(null, row);
            fail();
        } catch (LockTimeoutException e) {
            predicateLockTimeout(e);
        }

        txn.reset();
        
        table.merge(null, row);

        TestRow4 row2 = scanner.step();
        assertEquals(row, row2);

        scanner.close();
    }
    
    @Test
    public void noDeadlockWithScanner() throws Exception {
        tryDeadlockWithScanner(LockMode.READ_COMMITTED, false);
    }

    @Test
    public void noDeadlockWithScanner2() throws Exception {
        tryDeadlockWithScanner(LockMode.REPEATABLE_READ, false);
    }

    @Test
    public void deadlockWithScanner() throws Exception {
        tryDeadlockWithScanner(LockMode.UPGRADABLE_READ, true);
    }

    private void tryDeadlockWithScanner(LockMode mode, boolean deadlock) throws Exception {
        // Verifies that a writer doesn't deadlock with a scanner against a secondary index
        // when the writer leads the scanner. The writer should acquire an upgradable row lock
        // before updating secondaries, and then acquire an exclusive row lock at the end.
        // This technique doesn't prevent deadlocks when using UPGRADABLE_READ, however.

        var table = (StoredTable<TestRow2>) mDatabase.openTable(TestRow2.class);
        Table<TestRow2> ix = table.viewSecondaryIndex("name");

        fill(table, 1, 9);

        // Lock a row shared.
        Transaction txn0 = mDatabase.newTransaction();
        {
            txn0.lockMode(LockMode.REPEATABLE_READ);
            TestRow2 row = table.newRow();
            row.id(5);
            table.delete(txn0, row);
        }

        // Cannot store a row because it's blocked by txn0.
        Waiter w1 = start(() -> {
            Transaction txn = mDatabase.newTransaction();
            txn.lockTimeout(2, TimeUnit.SECONDS);
            TestRow2 row = table.newRow();
            row.id(5);
            row.name("newname");
            table.store(txn, row);
            txn.commit();
        }, "org.cojen.tupl.core.Lock", "tryLockUpgradable");

        // Cannot read a row because it's blocked by w1.
        String waitAt = mode == LockMode.UPGRADABLE_READ ? "tryLockUpgradable" : "tryLockShared";
        Waiter w2 = start(() -> {
            Transaction txn = mDatabase.newTransaction();
            txn.lockMode(mode);
            txn.lockTimeout(3, TimeUnit.SECONDS);
            var scanner = ix.newScanner(txn, "name >= ?", "name-5");
            scanner.step();
            scanner.close();
            txn.reset();
        }, "org.cojen.tupl.core.Lock", waitAt);

        txn0.reset();

        if (!deadlock) {
            w1.await();
            w2.await();
        } else {
            LockTimeoutException e1 = null;
            try {
                w1.await();
            } catch (LockTimeoutException e) {
                e1 = e;
            }

            LockTimeoutException e2 = null;
            try {
                w2.await();
            } catch (LockTimeoutException e) {
                e2 = e;
            }

            assertTrue(e1 instanceof DeadlockException || e2 instanceof DeadlockException);
        }
    }

    @Test
    public void filterLockRelease() throws Exception {
        // Test that secondary and primary row locks are released when row is filtered out.

        var table = (StoredTable<TestRow3>) mDatabase.openTable(TestRow3.class);
        Table<TestRow3> nameIx = table.viewSecondaryIndex("name");

        for (int i=1; i<=3; i++) {
            TestRow3 row = table.newRow();
            row.id(i);
            row.name("name-" + i);
            row.path("path-" + i);
            table.store(null, row);
        }

        Transaction txn = mDatabase.newTransaction();
        Scanner<TestRow3> scanner = nameIx.newScanner(txn, "path != ?", "path-2");
        assertEquals(1, scanner.row().id());
        scanner.step();
        assertEquals(3, scanner.row().id());
        scanner.step();
        assertNull(scanner.row());

        // All locks for id/name 1 and 3 should still be held.
        Transaction txn2 = mDatabase.newTransaction();
        TestRow3 row = table.newRow();
        row.id(1);
        try {
            table.load(txn2, row);
            fail();
        } catch (LockTimeoutException e) {
        }
        row.id(3);
        try {
            table.load(txn2, row);
            fail();
        } catch (LockTimeoutException e) {
        }
        try {
            nameIx.newScanner(txn2, "name == ?", "name-1");
            fail();
        } catch (LockTimeoutException e) {
        }
        try {
            nameIx.newScanner(txn2, "name == ?", "name-3");
            fail();
        } catch (LockTimeoutException e) {
        }

        // Locks for id/name 2 should have been released.
        row.id(2);
        table.load(txn2, row);
        assertEquals("name-2", row.name());
        Scanner<TestRow3> scanner2 = nameIx.newScanner(txn2, "name == ?", "name-2");
        assertEquals(2, scanner2.row().id());

        txn2.reset();
        txn.reset();

        // All locks should be available now.
        table.newStream(txn).toList();
        nameIx.newStream(txn).toList();
    }

    @Test
    public void filterLockRetainPrimary() throws Exception {
        filterLockRetain(false);
    }

    @Test
    public void filterLockRetainSecondary() throws Exception {
        filterLockRetain(true);
    }

    private void filterLockRetain(boolean lockSecondary) throws Exception {
        // Similar test as filterLockRelease, except the locks were already held before the
        // scanner started. They should be retained even when filtered out.

        var table = (StoredTable<TestRow3>) mDatabase.openTable(TestRow3.class);
        Table<TestRow3> nameIx = table.viewSecondaryIndex("name");

        for (int i=1; i<=3; i++) {
            TestRow3 row = table.newRow();
            row.id(i);
            row.name("name-" + i);
            row.path("path-" + i);
            table.store(null, row);
        }

        Transaction txn = mDatabase.newTransaction();

        // Acquire the row lock beforehand.
        if (lockSecondary) {
            nameIx.newScanner(txn, "name == ?", "name-2").close();
        } else {
            TestRow3 row = table.newRow();
            row.id(2);
            table.load(txn, row);
        }

        Scanner<TestRow3> scanner = nameIx.newScanner(txn, "path != ?", "path-2");
        assertEquals(1, scanner.row().id());
        scanner.step();
        assertEquals(3, scanner.row().id());
        scanner.step();
        assertNull(scanner.row());

        // All locks should still be held.
        Transaction txn2 = mDatabase.newTransaction();
        for (int i=1; i<=3; i++) {
            TestRow3 row = table.newRow();
            row.id(i);
            try {
                table.load(txn2, row);
                fail();
            } catch (LockTimeoutException e) {
            }
            try {
                nameIx.newScanner(txn2, "name == ?", "name-" + i);
                fail();
            } catch (LockTimeoutException e) {
            }
        }

        txn2.reset();
        txn.reset();

        // All locks should be available now.
        table.newStream(txn).toList();
        nameIx.newStream(txn).toList();
    }

    @Test
    public void joinNullTxn() throws Exception {
        joinNullTxn(false);
    }

    @Test
    public void joinNullTxnUpdater() throws Exception {
        joinNullTxn(true);
    }

    private void joinNullTxn(boolean withUpdater) throws Exception {
        // Test that a scanner over a secondary index with a null transaction acquires the
        // secondary lock first, and then acquires the primary lock while the secondary lock is
        // still held.

        teardown();
        mDatabase = Database.open(new DatabaseConfig()
                                  .lockTimeout(5, TimeUnit.SECONDS));

        Index tableSource = mDatabase.openIndex("test");
        var table = (StoredTable<TestRow2>) tableSource.asTable(TestRow2.class);
        var nameIx = (StoredTable<TestRow2>) table.viewSecondaryIndex("name");

        Index nameIxSource = nameIx.mSource;

        fill(table, 1, 3);

        byte[] pk2;
        try (Cursor c = tableSource.newCursor(null)) {
            c.first();
            c.next();
            pk2 = c.key();
        }

        byte[] sk2;
        try (Cursor c = nameIxSource.newCursor(null)) {
            c.first();
            c.next();
            sk2 = c.key();
        }

        // Lock the secondary key.
        Transaction txn1 = mDatabase.newTransaction();
        nameIxSource.lockExclusive(txn1, sk2);

        // Should be blocked on the secondary key lock until txn1 is reset.
        Waiter w1;
        if (!withUpdater) {
            w1 = start(() -> {
                var scanner = nameIx.newScanner(null, "name == ?", "name-2");
                fail("Obtained scanner instance: " + scanner + ", " + scanner.row());
            }, "org.cojen.tupl.core.Lock", "tryLockShared");
        } else {
            w1 = start(() -> {
                var updater = nameIx.newUpdater(null, "name == ?", "name-2");
                fail("Obtained updater instance: " + updater + ", " + updater.row());
            }, "org.cojen.tupl.core.Lock", "tryLockUpgradable");
        }

        assertEquals(Thread.State.TIMED_WAITING, w1.getState());

        // Lock the primary key.
        Transaction txn2 = mDatabase.newTransaction();
        tableSource.lockExclusive(txn2, pk2);

        // Release the secondary key lock, allowing w1 to proceed.
        txn1.reset();

        // Wait for w1 to acquire the secondary key lock.
        while (true) {
            Transaction txn3 = mDatabase.newTransaction();
            try {
                if (nameIxSource.tryLockExclusive(txn3, sk2, 0) == LockResult.TIMED_OUT_LOCK) {
                    break;
                }
                Thread.yield();
            } finally {
                txn3.reset();
            }
        }

        // With the primary key lock held, force a deadlock by trying to acquire the secondary
        // key lock. This confirms the expected scanner lock acquisition order.
        LockTimeoutException e1 = null;
        try {
            nameIxSource.lockExclusive(txn2, sk2);
        } catch (LockTimeoutException e) {
            e1 = e;
        }

        LockTimeoutException e2 = null;
        try {
            w1.await();
        } catch (LockTimeoutException e) {
            e2 = e;
        }

        if (!(e1 instanceof DeadlockException || e2 instanceof DeadlockException)) {
            fail("no deadlock: " + e1 + ", " + e2);
        }
    }

    @Test
    public void secondaryPredicate() throws Exception {
        // Test that a scan over a secondary index is protected by a predicate lock.

        var table = mDatabase.openTable(TestRow2.class);
        fill(table, 1, 4);

        Transaction txn1 = mDatabase.newTransaction();
        txn1.lockMode(LockMode.READ_COMMITTED);

        var scanner1 = table.newScanner(txn1, "name >= ? && name <= ?", "name-2", "name-4");
        assertEquals("name-2", scanner1.row().name());
        scanner1.step();
        assertEquals("name-3", scanner1.row().name());

        var row = table.newRow();
        row.id(2);
        row.name("xname");
        // Can update the row because no row lock is held.
        table.update(null, row);

        row.name("name-4");
        try {
            table.update(null, row);
            fail();
        } catch (LockTimeoutException e) {
            // Cannot move the row into the scan range.
        }

        scanner1.step();
        assertEquals("name-4", scanner1.row().name());
        assertNull(scanner1.step());
        txn1.reset();

        // Update should work now.
        table.update(null, row);
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

    private static <R extends TestRow> Scanner<R> newScanner
        (Table<R> table, boolean updater, Transaction txn, String filter, Object... args)
        throws Exception
    {
        return updater ? table.newUpdater(txn, filter, args)
            : table.newScanner(txn, filter, args);
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
            if (elem.getClassName().contains("BTree")) {
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
            check();
        }

        void await(long timeout) throws Exception {
            join(timeout);
            check();
        }

        void check() throws Exception {
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

    static Waiter start(Task task) throws Exception {
        var w = new Waiter(task);
        try {
            return TestUtils.startAndWaitUntilBlocked(w);
        } catch (Throwable e) {
            w.check();
            throw e;
        }
    }

    static Waiter start(Task task, Class where, String method) throws Exception {
        var w = new Waiter(task);
        try {
            return TestUtils.startAndWaitUntilBlocked(w, where, method);
        } catch (Throwable e) {
            w.check();
            throw e;
        }
    }

    static Waiter start(Task task, String where, String method) throws Exception {
        var w = new Waiter(task);
        try {
            return TestUtils.startAndWaitUntilBlocked(w, where, method);
        } catch (Throwable e) {
            w.check();
            throw e;
        }
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

    @PrimaryKey("id")
    @SecondaryIndex("name")
    public interface TestRow3 extends TestRow {
        String path();
        void path(String path);
    }

    @PrimaryKey("id")
    @SecondaryIndex({"name", "id", "path"})
    public interface TestRow4 extends TestRow3 {
        String path();
        void path(String path);
    }
}
