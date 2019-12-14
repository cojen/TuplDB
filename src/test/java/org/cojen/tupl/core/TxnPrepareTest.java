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

import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.*;

import static org.cojen.tupl.core.TestUtils.*;

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
            .directPageAccess(false)
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

    @Test
    public void noHandler() throws Exception {
        Database db = newTempDatabase(newConfig(null));
        try {
            db.prepareHandler("TestHandler");
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void noRedo() throws Exception {
        var recovery = new NonHandler();
        Database db = newTempDatabase(newConfig(recovery));
        PrepareHandler handler = db.prepareHandler("TestHandler");

        try {
            handler.prepare(Transaction.BOGUS, null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        Transaction txn = db.newTransaction();
        txn.durabilityMode(DurabilityMode.NO_REDO);
        try {
            handler.prepare(txn, null);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    static class NonHandler implements PrepareHandler {
        @Override
        public void prepare(Transaction txn, byte[] message) {}
    }

    @Test
    public void topLevelOnly() throws Exception {
        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareHandler("TestHandler");

        Transaction txn = db.newTransaction();
        txn.enter();
        try {
            handler.prepare(txn, null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("nested") > 0);
        }
    }

    @Test
    public void noUnlock() throws Exception {
        // Test that the special prepare entry is still locked.

        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareHandler("TestHandler");
        Transaction txn = db.newTransaction();

        handler.prepare(txn, null);

        try {
            txn.unlock();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("No locks held", e.getMessage());
        }

        var key = new byte[8];
        Utils.encodeLongBE(key, 0, txn.getId());
        assertEquals(LockResult.OWNED_EXCLUSIVE,
                     txn.tryLockExclusive(Tree.PREPARED_TXNS_ID, key, 0));

        assertEquals(LockResult.ACQUIRED, txn.lockUpgradable(0, "hello".getBytes()));
        assertEquals(LockResult.UPGRADED, txn.lockExclusive(0, "hello".getBytes()));

        try {
            handler.prepare(txn, null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("already prepared") > 0);
        }

        txn.reset();
    }

    @Test
    public void simpleCommit() throws Exception {
        Database db = newTempDatabase(newConfig(new NonHandler()));
        PrepareHandler handler = db.prepareHandler("TestHandler");
        Index ix = db.openIndex("test");

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        Transaction txn = db.newTransaction();

        ix.store(txn, key, value);
        assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(ix.getId(), key));

        assertEquals(LockResult.ACQUIRED, ix.lockShared(txn, k2));
        assertEquals(LockResult.OWNED_SHARED, txn.lockCheck(ix.getId(), k2));

        ix.load(txn, k3);
        assertEquals(LockResult.OWNED_UPGRADABLE, txn.lockCheck(ix.getId(), k3));

        handler.prepare(txn, null);

        assertEquals(LockResult.OWNED_EXCLUSIVE, txn.lockCheck(ix.getId(), key));
        assertEquals(LockResult.UNOWNED, txn.lockCheck(ix.getId(), k2));
        assertEquals(LockResult.UNOWNED, txn.lockCheck(ix.getId(), k3));

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
                mTxnId = txn.getId();
                mTxn = txn;
                mMessage = message;
            }
        }

        var recoveredQueue = new LinkedBlockingQueue<Recovered>();

        var recovery = new PrepareHandler() {
            private Database db;

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
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareHandler("TestHandler");

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

            txnId = txn.getId();
            handler.prepare(txn, null);
        }

        for (int i=0; i<3; i++) {
            db = reopenTempDatabase(getClass(), db, config);

            Recovered recovered = recoveredQueue.take();
            assertEquals(txnId, recovered.mTxnId);
            assertTrue(recoveredQueue.isEmpty());

            Index ix1 = db.openIndex("test1");
            Index ix2 = db.openIndex("test2");

            switch (recoveryAction) {
            default:
                fail("Unknown recovery action: " + recoveryAction);
                break;

            case "none": case "sticky":
                // Locks are retained.
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

                if ("sticky".equals(recoveryAction)) {
                    break;
                }

                recovered.mTxn.reset();

                // Fallthrough to the next case and verify rollback.

            case "reset": case "modify-reset":
                // Everything was rolled back.
                fastAssertArrayEquals("v1".getBytes(), ix1.load(null, key1));
                fastAssertArrayEquals("v2".getBytes(), ix2.load(null, key2));
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
            @Override
            public void prepare(Transaction txn, byte[] message) {
                recovered.add(txn);
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareHandler("TestHandler");
        Index ix = db.openIndex("test");

        // Should rollback and not be passed to the handler.
        Transaction txn1 = db.newTransaction();
        ix.store(txn1, "key-1".getBytes(), "value-1".getBytes());

        // Should be passed to the handler.
        Transaction txn2 = db.newTransaction();
        ix.store(txn2, "key-2".getBytes(), "value-2".getBytes());
        handler.prepare(txn2, null);

        // Should be passed to the handler.
        Transaction txn3 = db.newTransaction();
        ix.store(txn3, "key-3".getBytes(), "value-3".getBytes());
        handler.prepare(txn3, null);

        // Should rollback and not be passed to the handler.
        Transaction txn4 = db.newTransaction();
        ix.store(txn4, "key-4".getBytes(), "value-4".getBytes());

        // Should commit and not be passed to the handler.
        Transaction txn5 = db.newTransaction();
        ix.store(txn5, "key-5".getBytes(), "value-5".getBytes());
        handler.prepare(txn5, null);
        txn5.commit();

        // Should rollback and not be passed to the handler.
        Transaction txn6 = db.newTransaction();
        ix.store(txn6, "key-6".getBytes(), "value-6".getBytes());
        handler.prepare(txn6, null);
        txn6.exit();

        db = reopenTempDatabase(getClass(), db, config);
        handler = db.prepareHandler("TestHandler");
        ix = db.openIndex("test");

        Transaction t1 = recovered.take();
        Transaction t2 = recovered.take();
        assertTrue(recovered.isEmpty());

        // Transactions can be recovered in any order.
        if (t1.getId() == txn2.getId()) {
            assertEquals(t2.getId(), txn3.getId());
        } else {
            assertEquals(t1.getId(), txn3.getId());
            assertEquals(t2.getId(), txn2.getId());
        }

        // Rollback of txn1, txn4, and txn6.
        assertNull(ix.load(null, "key-1".getBytes()));
        assertNull(ix.load(null, "key-4".getBytes()));
        assertNull(ix.load(null, "key-6".getBytes()));

        // Commit of txn5.
        fastAssertArrayEquals("value-5".getBytes(), ix.load(null, "key-5".getBytes()));

        // Recovered transactions are still locked.
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

        t1.reset();
        t2.reset();

        // Explicit locks should be recovered (exclusive only).
        Transaction txn7 = db.newTransaction();
        ix.lockUpgradable(txn7, "key-7".getBytes());
        ix.lockUpgradable(txn7, "key-8".getBytes());
        ix.lockExclusive(txn7, "key-9".getBytes());
        ix.lockExclusive(txn7, "key-8".getBytes());
        handler.prepare(txn7, null);
        db.checkpoint();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");

        Transaction t7 = recovered.take();
        assertEquals(t7.getId(), txn7.getId());

        assertEquals(LockResult.UNOWNED, t7.lockCheck(ix.getId(), "key-7".getBytes()));
        assertEquals(LockResult.OWNED_EXCLUSIVE, t7.lockCheck(ix.getId(), "key-8".getBytes()));
        assertEquals(LockResult.OWNED_EXCLUSIVE, t7.lockCheck(ix.getId(), "key-9".getBytes()));

        Transaction txn8 = db.newTransaction();
        assertEquals(LockResult.ACQUIRED,
                     txn8.tryLockExclusive(ix.getId(), "key-7".getBytes(), 0));
        assertEquals(LockResult.TIMED_OUT_LOCK,
                     txn8.tryLockShared(ix.getId(), "key-8".getBytes(), 0));
        assertEquals(LockResult.TIMED_OUT_LOCK,
                     txn8.tryLockExclusive(ix.getId(), "key-9".getBytes(), 0));
        txn8.reset();

        t7.reset();
    }

    @Test
    public void reopenNoHandler() throws Exception {
        // When database is reopened without a recovery handler, the recovered transactions
        // aren't lost.

        var recovery = new PrepareHandler() {
            volatile byte[] message;

            @Override
            public void prepare(Transaction txn, byte[] message) throws IOException {
                this.message = message;
                txn.commit();
            }
        };

        DatabaseConfig config = newConfig(recovery);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareHandler("TestHandler");
        Index ix = db.openIndex("test");

        Transaction txn = db.newTransaction();
        byte[] key = "hello".getBytes();
        ix.store(txn, key, "world".getBytes());
        handler.prepare(txn, "message".getBytes());

        // Reopen without the handler.
        config.prepareHandlers(null);
        db = reopenTempDatabase(getClass(), db, config);

        // Still locked.
        ix = db.openIndex("test");
        txn = db.newTransaction();
        assertEquals(LockResult.TIMED_OUT_LOCK, ix.tryLockShared(txn, key, 0));
        txn.reset();

        // Reopen with the handler installed.
        config.prepareHandlers(Map.of("TestHandler", recovery));
        recovery.message = null;
        db = reopenTempDatabase(getClass(), db, config);

        // Verify that the handler has committed the recovered transaction.
        ix = db.openIndex("test");
        fastAssertArrayEquals("world".getBytes(), ix.load(null, key));

        fastAssertArrayEquals("message".getBytes(), recovery.message);
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
        };

        var keys = new byte[2][];
        var values = new byte[keys.length][];

        for (int i=0; i<keys.length; i++) {
            keys[i] = ("key-" + i).getBytes();
            values[i] = ("value-" + i).getBytes();
        }

        DatabaseConfig config = newConfig(recovery).lockTimeout(1, TimeUnit.SECONDS);
        Database db = newTempDatabase(config);
        PrepareHandler handler = db.prepareHandler("TestHandler");

        {
            Index ix = db.openIndex("test");

            Transaction txn = db.newTransaction();
            ix.store(txn, keys[0], values[0]);
            handler.prepare(txn, null);
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

            // All locks should still be held except for the last key.
            for (int i=0; i<keys.length; i++) {
                try {
                    ix.load(null, keys[i]);
                    if (i < keys.length - 1) {
                        fail();
                    }
                } catch (LockTimeoutException e) {
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

        for (int i=0; i<keys.length; i++) {
            assertNull(ix.load(null, keys[i]));
        }
    }
}
