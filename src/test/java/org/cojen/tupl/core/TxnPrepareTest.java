/*
 *  Copyright (C) 2018 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.Map;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.ext.PrepareHandler;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests for the Transaction.prepare method.
 *
 * @author Brian S O'Neill
 */
public class TxnPrepareTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TxnPrepareTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    protected DatabaseConfig newConfig(PrepareHandler recovery) {
        var config = new DatabaseConfig()
            .lockTimeout(5000, TimeUnit.MILLISECONDS)
            .checkpointRate(-1, null);

        if (recovery != null) {
            config.prepareHandlers(Map.of("TestHandler", recovery));
        }

        return config;
    }

    protected Database newTempDatabase(DatabaseConfig config) throws Exception {
        return TestUtils.newTempDatabase(getClass(), config);
    }

    protected boolean isPrepareCommit() {
        return false;
    }

    protected void prepare(PrepareHandler handler, Transaction txn, byte[] message)
        throws IOException
    {
        if (isPrepareCommit()) {
            handler.prepareCommit(txn, message);
        } else {
            handler.prepare(txn, message);
        }
    }

    @Test
    public void noHandler() throws Exception {
        Database db = newTempDatabase(newConfig(null));
        try {
            db.prepareWriter("TestHandler");
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void noRedo() throws Exception {
        var recovery = new NonHandler();
        Database db = newTempDatabase(newConfig(recovery));
        PrepareHandler handler = db.prepareWriter("TestHandler");

        try {
            prepare(handler, Transaction.BOGUS, null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        Transaction txn = db.newTransaction();
        txn.durabilityMode(DurabilityMode.NO_REDO);
        try {
            prepare(handler, txn, null);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    static class NonHandler implements PrepareHandler {
        @Override
        public void prepare(Transaction txn, byte[] message) {}
        @Override
        public void prepareCommit(Transaction txn, byte[] message) {}
    }

    @Test
    public void topLevelOnly() throws Exception {
        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareWriter("TestHandler");

        Transaction txn = db.newTransaction();
        txn.enter();
        try {
            prepare(handler, txn, null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("nested") > 0);
        }
    }

    @Test
    public void noUnlock() throws Exception {
        // Test that the special prepare entry is still locked.

        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareWriter("TestHandler");
        Transaction txn = db.newTransaction();

        prepare(handler, txn, null);

        try {
            txn.unlock();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("No locks held", e.getMessage());
        }

        var key = new byte[8];
        Utils.encodeLongBE(key, 0, txn.id());
        assertEquals(LockResult.OWNED_EXCLUSIVE,
                     txn.tryLockExclusive(BTree.PREPARED_TXNS_ID, key, 0));

        assertEquals(LockResult.ACQUIRED, txn.lockUpgradable(0, "hello".getBytes()));
        assertEquals(LockResult.UPGRADED, txn.lockExclusive(0, "hello".getBytes()));

        try {
            prepare(handler, txn, null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("already prepared") > 0);
        }

        txn.reset();
    }

    @Test
    public void simpleCommit() throws Exception {
        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareWriter("TestHandler");
        Index ix = db.openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        Transaction txn = db.newTransaction();

        ix.store(txn, key, value);
        assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(ix.id(), key));

        assertEquals(LockResult.ACQUIRED, ix.lockShared(txn, k2));
        assertEquals(LockResult.OWNED_SHARED, txn.lockCheck(ix.id(), k2));

        ix.load(txn, k3);
        assertEquals(LockResult.OWNED_UPGRADABLE, txn.lockCheck(ix.id(), k3));

        prepare(handler, txn, null);

        if (isPrepareCommit()) {
            assertEquals(LockResult.UNOWNED, txn.lockCheck(ix.id(), key));
            fastAssertArrayEquals(value, ix.load(null, key));
        } else {
            assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(ix.id(), key));
        }

        assertEquals(LockResult.UNOWNED, txn.lockCheck(ix.id(), k2));
        assertEquals(LockResult.UNOWNED, txn.lockCheck(ix.id(), k3));

        txn.commit();

        fastAssertArrayEquals(value, ix.load(null, key));
    }

    // Test transaction recovery from just the redo log...

    @Test
    public void basicRedoRecoveryNoAction() throws Exception {
        basicRecovery("redo", "none");
    }

    @Test
    public void basicRedoRecoveryReset() throws Exception {
        basicRecovery("redo", "reset");
    }

    @Test
    public void basicRedoRecoveryModifyReset() throws Exception {
        basicRecovery("redo", "modify-reset");
    }

    @Test
    public void basicRedoRecoveryCommit() throws Exception {
        basicRecovery("redo", "commit");
    }

    @Test
    public void basicRedoRecoveryModifyCommit() throws Exception {
        basicRecovery("redo", "modify-commit");
    }

    @Test
    public void basicRedoRecoverySticky() throws Exception {
        basicRecovery("redo", "sticky");
    }

    // Test transaction recovery from just the undo log...

    @Test
    public void basicUndoRecoveryNoAction() throws Exception {
        basicRecovery("undo", "none");
    }

    @Test
    public void basicUndoRecoveryReset() throws Exception {
        basicRecovery("undo", "reset");
    }

    @Test
    public void basicUndoRecoveryModifyReset() throws Exception {
        basicRecovery("undo", "modify-reset");
    }

    @Test
    public void basicUndoRecoveryCommit() throws Exception {
        basicRecovery("undo", "commit");
    }

    @Test
    public void basicUndoRecoveryModifyCommit() throws Exception {
        basicRecovery("undo", "modify-commit");
    }

    @Test
    public void basicUndoRecoverySticky() throws Exception {
        basicRecovery("undo", "sticky");
    }

    // Test transaction recovery from the redo and undo logs...

    @Test
    public void basicRedoUndoRecoveryNoAction() throws Exception {
        basicRecovery("redo-undo", "none");
    }

    @Test
    public void basicRedoUndoRecoveryReset() throws Exception {
        basicRecovery("redo-undo", "reset");
    }

    @Test
    public void basicRedoUndoRecoveryModifyReset() throws Exception {
        basicRecovery("redo-undo", "modify-reset");
    }

    @Test
    public void basicRedoUndoRecoveryCommit() throws Exception {
        basicRecovery("redo-undo", "commit");
    }

    @Test
    public void basicRedoUndoRecoveryModifyCommit() throws Exception {
        basicRecovery("redo-undo", "modify-commit");
    }

    @Test
    public void basicRedoUndoRecoverySticky() throws Exception {
        basicRecovery("redo-undo", "sticky");
    }

    private void basicRecovery(String recoveryType, String recoveryAction) throws Exception {
        byte[] key1 = "key-1".getBytes();
        byte[] key2 = "key-2".getBytes();

        class Recovered {
            final long mTxnId;
            final Transaction mTxn;
            final byte[] mMessage;

            Recovered(Transaction txn, byte[] message) {
                // Capture the transaction id before the transaction is reset.
                mTxnId = txn.id();
                mTxn = txn;
                mMessage = message;
            }
        }

        var recoveredQueue = new LinkedBlockingQueue<Recovered>();

        var recovery = new PrepareHandler() {
            private Database db;
            volatile boolean prepareCommit;
            private boolean finished;

            @Override
            public void init(Database db) {
                this.db = db;
            }

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                recoveredQueue.add(new Recovered(txn, message));

                switch (recoveryAction) {
                default:
                    // Leak the transaction and keep the locks.
                    break;

                case "modify-reset":
                    db.findIndex("test1").store(txn, key1, "modified-1".getBytes());
                    db.findIndex("test2").store(txn, key2, "modified-2".getBytes());
                    // Fallthrough to the next case to reset.

                case "reset":
                    txn.reset();
                    break;

                case "modify-commit":
                    db.findIndex("test1").store(txn, key1, "modified-1".getBytes());
                    db.findIndex("test2").store(txn, key2, "modified-2".getBytes());
                    // Fallthrough to the next case to commit.

                case "commit":
                    txn.commit();
                    break;
                }

                synchronized (this) {
                    finished = true;
                    notifyAll();
                }
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepareCommit = true;
                prepare(txn, message);
            }

            synchronized void reset() {
                prepareCommit = false;
                finished = false;
            }

            synchronized void waitUntilFinished() throws Exception {
                while (!finished) {
                    wait();
                }
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareWriter("TestHandler");

        long txnId;
        {
            Index ix1 = db.openIndex("test1");
            Index ix2 = db.openIndex("test2");

            ix1.store(null, key1, "v1".getBytes());
            ix2.store(null, key2, "v2".getBytes());

            Transaction txn = db.newTransaction();

            ix1.store(txn, key1, "value-1".getBytes());

            if ("redo-undo".equals(recoveryType)) {
                db.checkpoint();
                // Suppress later assertion.
                recoveryType = "redo";
            }

            ix2.store(txn, key2, "value-2".getBytes());

            if ("undo".equals(recoveryType)) {
                db.checkpoint();
            } else if (!"redo".equals(recoveryType)) {
                fail("Unknown recovery type: " + recoveryType);
            }

            txnId = txn.id();
            prepare(handler, txn, null);
        }

        for (int i=0; i<3; i++) {
            recovery.reset();

            db = reopenTempDatabase(getClass(), db, config);

            Recovered recovered = recoveredQueue.take();
            assertEquals(txnId, recovered.mTxnId);
            assertTrue(recoveredQueue.isEmpty());

            assertEquals(isPrepareCommit(), recovery.prepareCommit);

            recovery.waitUntilFinished();

            Index ix1 = db.openIndex("test1");
            Index ix2 = db.openIndex("test2");

            switch (recoveryAction) {
            default:
                fail("Unknown recovery action: " + recoveryAction);
                break;

            case "none": case "sticky":
                // Locks are retained, unless using prepareCommit.
                if (isPrepareCommit()) {
                    fastAssertArrayEquals("value-1".getBytes(), ix1.load(null, key1));
                    fastAssertArrayEquals("value-2".getBytes(), ix2.load(null, key2));
                } else {
                    try {
                        ix1.load(null, key1);
                        fail();
                    } catch (LockTimeoutException e) {
                        // Expected.
                    }
                    try {
                        ix2.load(null, key2);
                        fail();
                    } catch (LockTimeoutException e) {
                        // Expected.
                    }
                }

                if ("sticky".equals(recoveryAction)) {
                    break;
                }

                recovered.mTxn.reset();

                // Fallthrough to the next case and verify rollback.

            case "reset": case "modify-reset":
                // Everything was rolled back, unless using prepareCommit.
                if (isPrepareCommit()) {
                    fastAssertArrayEquals("value-1".getBytes(), ix1.load(null, key1));
                    fastAssertArrayEquals("value-2".getBytes(), ix2.load(null, key2));
                } else {
                    fastAssertArrayEquals("v1".getBytes(), ix1.load(null, key1));
                    fastAssertArrayEquals("v2".getBytes(), ix2.load(null, key2));
                }
                break;

            case "modify-commit":
                // Everything was modified and committed.
                fastAssertArrayEquals("modified-1".getBytes(), ix1.load(null, key1));
                fastAssertArrayEquals("modified-2".getBytes(), ix2.load(null, key2));
                break;

            case "commit":
                // Everything was committed.
                fastAssertArrayEquals("value-1".getBytes(), ix1.load(null, key1));
                fastAssertArrayEquals("value-2".getBytes(), ix2.load(null, key2));
                break;
            }

            if (!"sticky".equals(recoveryAction)) {
                break;
            }

            // Transaction should stick around each time the database is reopened.
        }
    }

    @Test
    public void basicMix() throws Exception {
        // Test that unprepared transactions don't get passed to the recover handler, testing
        // also with multiple recovered transactions.

        var recovered = new LinkedBlockingQueue<Transaction>();

        var recovery = new PrepareHandler() {
            volatile boolean prepareCommit;

            @Override
            public void prepare(Transaction txn, byte[] message) {
                recovered.add(txn);
            }
            @Override
            public void prepareCommit(Transaction txn, byte[] message) {
                prepareCommit = true;
                prepare(txn, message);
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareWriter("TestHandler");
        Index ix = db.openIndex("test");

        // Should rollback and not be passed to the handler.
        Transaction txn1 = db.newTransaction();
        ix.store(txn1, "key-1".getBytes(), "value-1".getBytes());

        // Should be passed to the handler.
        Transaction txn2 = db.newTransaction();
        ix.store(txn2, "key-2".getBytes(), "value-2".getBytes());
        prepare(handler, txn2, null);

        // Should be passed to the handler.
        Transaction txn3 = db.newTransaction();
        ix.store(txn3, "key-3".getBytes(), "value-3".getBytes());
        prepare(handler, txn3, null);

        // Should rollback and not be passed to the handler.
        Transaction txn4 = db.newTransaction();
        ix.store(txn4, "key-4".getBytes(), "value-4".getBytes());

        // Should commit and not be passed to the handler.
        Transaction txn5 = db.newTransaction();
        ix.store(txn5, "key-5".getBytes(), "value-5".getBytes());
        prepare(handler, txn5, null);
        txn5.commit();

        // Should rollback and not be passed to the handler.
        Transaction txn6 = db.newTransaction();
        ix.store(txn6, "key-6".getBytes(), "value-6".getBytes());
        prepare(handler, txn6, null);
        txn6.exit();

        db = reopenTempDatabase(getClass(), db, config);
        handler = db.prepareWriter("TestHandler");
        ix = db.openIndex("test");

        Transaction t1 = recovered.take();
        Transaction t2 = recovered.take();
        assertTrue(recovered.isEmpty());

        assertEquals(isPrepareCommit(), recovery.prepareCommit);

        // Transactions can be recovered in any order.
        if (t1.id() == txn2.id()) {
            assertEquals(t2.id(), txn3.id());
        } else {
            assertEquals(t1.id(), txn3.id());
            assertEquals(t2.id(), txn2.id());
        }

        // Rollback of txn1, txn4, and txn6 (unless prepareCommit)
        assertNull(ix.load(null, "key-1".getBytes()));
        assertNull(ix.load(null, "key-4".getBytes()));
        if (isPrepareCommit()) {
            fastAssertArrayEquals("value-6".getBytes(), ix.load(null, "key-6".getBytes()));
        } else {
            assertNull(ix.load(null, "key-6".getBytes()));
        }

        // Commit of txn5.
        fastAssertArrayEquals("value-5".getBytes(), ix.load(null, "key-5".getBytes()));

        // Recovered transactions are still locked (unless prepareCommit)
        if (isPrepareCommit()) {
            fastAssertArrayEquals("value-2".getBytes(), ix.load(null, "key-2".getBytes()));
            fastAssertArrayEquals("value-3".getBytes(), ix.load(null, "key-3".getBytes()));
        } else {
            try {
                ix.load(null, "key-2".getBytes());
                fail();
            } catch (LockTimeoutException e) {
            }
            try {
                ix.load(null, "key-3".getBytes());
                fail();
            } catch (LockTimeoutException e) {
            }
        }

        t1.reset();
        t2.reset();

        // Explicit locks should be recovered (exclusive only).
        Transaction txn7 = db.newTransaction();
        ix.lockUpgradable(txn7, "key-7".getBytes());
        ix.lockUpgradable(txn7, "key-8".getBytes());
        ix.lockExclusive(txn7, "key-9".getBytes());
        ix.lockExclusive(txn7, "key-8".getBytes());
        prepare(handler, txn7, null);
        db.checkpoint();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");

        Transaction t7 = recovered.take();
        assertEquals(t7.id(), txn7.id());

        assertEquals(LockResult.UNOWNED, t7.lockCheck(ix.id(), "key-7".getBytes()));
        if (isPrepareCommit()) {
            assertEquals(LockResult.UNOWNED, t7.lockCheck(ix.id(), "key-8".getBytes()));
            assertEquals(LockResult.UNOWNED, t7.lockCheck(ix.id(), "key-9".getBytes()));
        } else {
            assertEquals(LockResult.OWNED_EXCLUSIVE, t7.lockCheck(ix.id(), "key-8".getBytes()));
            assertEquals(LockResult.OWNED_EXCLUSIVE, t7.lockCheck(ix.id(), "key-9".getBytes()));
        }

        Transaction txn8 = db.newTransaction();
        assertEquals(LockResult.ACQUIRED,
                     txn8.tryLockExclusive(ix.id(), "key-7".getBytes(), 0));
        if (isPrepareCommit()) {
            assertEquals(LockResult.ACQUIRED,
                         txn8.tryLockShared(ix.id(), "key-8".getBytes(), 0));
            assertEquals(LockResult.ACQUIRED,
                         txn8.tryLockExclusive(ix.id(), "key-9".getBytes(), 0));
        } else {
            assertEquals(LockResult.TIMED_OUT_LOCK,
                         txn8.tryLockShared(ix.id(), "key-8".getBytes(), 0));
            assertEquals(LockResult.TIMED_OUT_LOCK,
                         txn8.tryLockExclusive(ix.id(), "key-9".getBytes(), 0));
        }
        txn8.reset();

        t7.reset();
    }

    @Test
    public void reopenNoHandler() throws Exception {
        // When database is reopened without a recovery handler, the recovered transactions
        // aren't lost.

        var recovery = new PrepareHandler() {
            volatile byte[] message;
            volatile boolean prepareCommit;

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                this.message = message;
                txn.commit();
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepareCommit = true;
                prepare(txn, message);
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareWriter("TestHandler");
        Index ix = db.openIndex("test");

        Transaction txn = db.newTransaction();
        byte[] key = "hello".getBytes();
        ix.store(txn, key, "world".getBytes());
        prepare(handler, txn, "message".getBytes());

        // Reopen without the handler.
        config.prepareHandlers(null);

        // Install a listener which is notified that recovery fails.
        var listener = new EventListener() {
            private boolean notified;

            @Override
            public void notify(EventType type, String message, Object... args) {
                if (type == EventType.RECOVERY_HANDLER_UNCAUGHT) {
                    synchronized (this) {
                        notified = true;
                        notifyAll();
                    }
                }
            }

            synchronized void waitForNotify() throws InterruptedException {
                while (!notified) {
                    wait();
                }
            }
        };

        config.eventListener(listener);

        db = reopenTempDatabase(getClass(), db, config);

        // Still locked (unless prepareCommit)
        ix = db.openIndex("test");
        txn = db.newTransaction();
        if (isPrepareCommit()) {
            assertEquals(LockResult.ACQUIRED, ix.tryLockShared(txn, key, 0));
        } else {
            assertEquals(LockResult.TIMED_OUT_LOCK, ix.tryLockShared(txn, key, 0));
        }
        txn.reset();

        listener.waitForNotify();

        // Reopen with the handler installed.
        config.prepareHandlers(Map.of("TestHandler", recovery));
        config.eventListener(null);
        recovery.message = null;
        recovery.prepareCommit = false;
        db = reopenTempDatabase(getClass(), db, config);

        // Verify that the handler has committed the recovered transaction.
        ix = db.openIndex("test");
        fastAssertArrayEquals("world".getBytes(), ix.load(null, key));

        byte[] message = recovery.message;
        if (message == null && isPrepareCommit()) {
            // Wait for it since no lock was held.
            for (int i=0; i<10; i++) {
                Thread.sleep(1000);
                message = recovery.message;
                if (message != null) {
                    break;
                }
            }
        }

        fastAssertArrayEquals("message".getBytes(), recovery.message);

        assertEquals(isPrepareCommit(), recovery.prepareCommit);
    }

    @Test
    public void postPrepareRollback() throws Exception {
        // Verify that changes after prepare are rolled back by recovery.

        var recoveredQueue = new LinkedBlockingQueue<Transaction>();

        var recovery = new PrepareHandler() {
            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                recoveredQueue.add(txn);
            }

            @Override
            public void prepareCommit(Transaction txn, byte[] message) throws IOException {
                prepare(txn, message);
            }
        };

        var keys = new byte[2][];
        var values = new byte[keys.length][];

        for (int i=0; i<keys.length; i++) {
            keys[i] = ("key-" + i).getBytes();
            values[i] = ("value-" + i).getBytes();
        }

        DatabaseConfig config = newConfig(recovery).lockTimeout(1, TimeUnit.SECONDS);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareWriter("TestHandler");

        {
            Index ix = db.openIndex("test");

            Transaction txn = db.newTransaction();
            ix.store(txn, keys[0], values[0]);
            prepare(handler, txn, null);
            ix.store(txn, keys[1], values[1]);

            db.checkpoint();
        }

        Transaction txn = null;
        Index ix = null;

        // Recover twice to verify that prepare undo operation isn't lost.
        for (int q=0; q<2; q++) {
            db = reopenTempDatabase(getClass(), db, config);
            ix = db.openIndex("test");

            txn = recoveredQueue.take();
            assertTrue(recoveredQueue.isEmpty());

            // All locks should still be held except for the last key (unless prepareCommit)
            if (isPrepareCommit()) {
                for (int i=0; i<keys.length; i++) {
                    ix.load(null, keys[i]);
                }
            } else {
                for (int i=0; i<keys.length; i++) {
                    try {
                        ix.load(null, keys[i]);
                        if (i < keys.length - 1) {
                            fail();
                        }
                    } catch (LockTimeoutException e) {
                    }
                }
            }

            // The recovered transaction should own the locks.
            for (int i=0; i<keys.length; i++) {
                byte[] value = ix.load(txn, keys[i]);
                if (i < keys.length - 1) {
                    fastAssertArrayEquals(values[i], value);
                } else {
                    // This one got rolled back.
                    assertNull(value);
                }
            }

            db.checkpoint();
        }

        txn.reset();

        if (isPrepareCommit()) { 
            fastAssertArrayEquals(values[0], ix.load(null, keys[0]));
            assertNull(ix.load(null, keys[1]));
        } else {
            for (int i=0; i<keys.length; i++) {
                assertNull(ix.load(null, keys[i]));
            }
        }
    }
}
