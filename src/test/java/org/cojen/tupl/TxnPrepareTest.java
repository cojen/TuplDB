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

package org.cojen.tupl;

import java.io.IOException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.ext.RecoveryHandler;

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

    protected DatabaseConfig newConfig(RecoveryHandler handler) {
        return new DatabaseConfig()
            .recoveryHandler(handler)
            .directPageAccess(false)
            .lockTimeout(1000, TimeUnit.MILLISECONDS)
            .checkpointRate(-1, null);
    }

    protected Database newTempDatabase(DatabaseConfig config) throws Exception {
        return TestUtils.newTempDatabase(getClass(), config);
    }

    @Test
    public void noHandler() throws Exception {
        Database db = newTempDatabase(newConfig(null));
        Transaction txn = db.newTransaction();
        try {
            txn.prepare();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void noRedo() throws Exception {
        try {
            Transaction.BOGUS.prepare();
            fail();
        } catch (IllegalStateException e) {
        }

        RecoveryHandler handler = new RecoveryHandler() {
            public void init(Database db) {}
            public void recover(Transaction txn) {}
        };

        Database db = newTempDatabase(newConfig(handler));

        Transaction txn = db.newTransaction();
        txn.durabilityMode(DurabilityMode.NO_REDO);
        try {
            txn.prepare();
            fail();
        } catch (IllegalStateException e) {
        }
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

        BlockingQueue<Transaction> recovered = new LinkedBlockingQueue<>();

        RecoveryHandler handler = new RecoveryHandler() {
            private Database db;

            @Override
            public void init(Database db) {
                this.db = db;
            }

            @Override
            public void recover(Transaction txn) throws IOException {
                recovered.add(txn);

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

        DatabaseConfig config = newConfig(handler);
        Database db = newTempDatabase(config);

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
            txn.prepare();
        }

        for (int i=0; i<3; i++) {
            db = reopenTempDatabase(getClass(), db, config);

            Transaction txn = recovered.take();
            assertEquals(txnId, txn.getId());
            assertTrue(recovered.isEmpty());

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

                txn.reset();

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
}
