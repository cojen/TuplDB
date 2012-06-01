/*
 *  Copyright 2011 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.Lock;

/**
 * Defines a logical unit of work. Transaction instances can only be safely
 * used by one thread at a time, and they must be {@link #reset reset} when no
 * longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion,
 * multiple threads interacting with a Transaction instance may cause database
 * corruption.
 *
 * <p>Transactions also contain various methods for directly controlling locks,
 * although their use is not required. Methods which operate upon transactions
 * acquire and release locks automatically. Direct control over locks is
 * provided for advanced use cases. One such use is record filtering:
 *
 * <pre>
 * Transaction txn = ...
 * Cursor c = index.newCursor(txn);
 * for (LockResult result = c.first(); c.key() != null; result = c.next()) {
 *     if (shouldDiscard(c.value()) && result == LockResult.ACQUIRED) {
 *         // Unlock record which doesn't belong in the transaction.
 *         txn.unlock();
 *         continue;
 *     }
 *     ...
 * }
 * </pre>
 *
 * <p>Note: Transaction instances are never fully closed after they are reset
 * or have fully exited. Any operation which acts upon a reset transaction can
 * resurrect it.
 *
 * @author Brian S O'Neill
 * @see Database#newTransaction Database.newTransaction
 */
public final class Transaction extends Locker {
    /**
     * Transaction instance which isn't a transaction at all. It always
     * operates in an {@link LockMode#UNSAFE unsafe} lock mode and a {@link
     * DurabilityMode#NO_LOG no-log} durability mode. For safe auto-commit
     * transactions, pass null for the transaction argument.
     */
    public static final Transaction BOGUS = new Transaction();

    private final Database mDatabase;
    final DurabilityMode mDurabilityMode;

    private LockMode mLockMode;
    private long mLockTimeoutNanos;
    private long mTxnId;
    private boolean mHasRedo;
    private long mSavepoint;

    private UndoLog mUndoLog;

    // Is an exception if transaction is borked, BOGUS if bogus.
    private Object mBorked;

    // TODO: Define autoCommit(boolean) method.

    // FIXME: Add abort method which can be called by any thread. Transaction
    // is borked as a result, and the field needs to be volatile to signal the
    // abort action. Rollback is performed by the original thread, to avoid any
    // other race conditions. If transaction is waiting for a lock, it's
    // possible to wake up all threads in the wait queue, but which thread is
    // the correct one? A bogus signal can be injected into all of them, and
    // all threads need to check the lock state again anyhow. If the
    // mWaitingFor field was cleared, this indicates that transaction was
    // aborted. Define a new LockResult code: ABORTED

    Transaction(Database db,
                DurabilityMode durabilityMode,
                LockMode lockMode,
                long timeoutNanos)
    {
        super(db.mLockManager);
        mDatabase = db;
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
        mLockTimeoutNanos = timeoutNanos;
    }

    // Constructor for BOGUS transaction.
    private Transaction() {
        super();
        mDatabase = null;
        mDurabilityMode = DurabilityMode.NO_LOG;
        mLockMode = LockMode.UNSAFE;
        mBorked = this;
    }

    /**
     * Sets the lock mode for the current scope. Transactions begin in {@link
     * LockMode#UPGRADABLE_READ UPGRADABLE_READ} mode, and newly entered scopes
     * begin at the outer scope's current mode. Exiting a scope reverts the
     * lock mode.
     *
     * @param mode new lock mode
     * @throws IllegalArgumentException if mode is null
     */
    public final void lockMode(LockMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Lock mode is null");
        } else {
            mLockMode = mode;
        }
    }

    /**
     * Returns the current lock mode.
     */
    public final LockMode lockMode() {
        return mLockMode;
    }

    /**
     * Sets the lock timeout for the current scope. A negative timeout is
     * infinite.
     *
     * @param unit required unit if timeout is more than zero
     */
    public final void lockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
    }

    /**
     * Returns the current lock timeout, in the given unit.
     */
    public final long lockTimeout(TimeUnit unit) {
        return unit.convert(mLockTimeoutNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the fixed durability mode of this transaction.
     */
    public final DurabilityMode durabilityMode() {
        return mDurabilityMode;
    }

    /**
     * Checks the validity of the transaction.
     *
     * @throws DatabaseException if transaction is bogus or was invalidated by
     * an earlier exception
     */
    public final void check() throws DatabaseException {
        Object borked = mBorked;
        if (borked != null) {
            if (borked == BOGUS) {
                throw new DatabaseException("Transaction is bogus");
            } else {
                throw new DatabaseException("Invalid transaction, caused by: " + borked);
            }
        }
    }

    /**
     * Commits all modifications made within the current transaction scope. The
     * current scope is still valid after this method is called, unless an
     * exception is thrown. Call exit or reset to fully release transaction
     * resources.
     */
    public final void commit() throws IOException {
        check();

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                UndoLog undo = mUndoLog;
                if (undo == null) {
                    if (mHasRedo) {
                        RedoLog redo = mDatabase.mRedoLog;
                        if (redo.txnCommitFull(mTxnId, mDurabilityMode)) {
                            redo.txnCommitSync();
                        }
                        mHasRedo = false;
                    }
                    super.scopeUnlockAll();
                } else {
                    // Holding the shared commit lock ensures that the redo log
                    // doesn't disappear before the undo log. Lingering undo
                    // logs with no corresponding redo log are treated as
                    // aborted. Recovery would erroneously rollback committed
                    // transactions.
                    final Lock sharedCommitLock = mDatabase.sharedCommitLock();
                    sharedCommitLock.lock();
                    boolean sync;
                    try {
                        if (sync = mHasRedo) {
                            sync = mDatabase.mRedoLog.txnCommitFull(mTxnId, mDurabilityMode);
                            mHasRedo = false;
                        }
                        // Indicates that undo log should be truncated instead
                        // of rolled back during recovery. Commit lock can now
                        // be released safely. See UndoLog.rollbackRemaining.
                        undo.pushCommit();
                    } finally {
                        sharedCommitLock.unlock();
                    }

                    if (sync) {
                        // Durably sync the redo log after releasing the commit
                        // lock, preventing additional blocking.
                        mDatabase.mRedoLog.txnCommitSync();
                    }

                    // Calling this deletes any tombstones too.
                    super.scopeUnlockAll();

                    // Truncate obsolete log entries after releasing
                    // locks. Active transaction id is cleared as a
                    // side-effect. Recovery might need to re-delete
                    // tombstones, which is only possible with a complete undo
                    // log. Truncate operation can be interrupted by a
                    // checkpoint, allowing a partial undo log to be seen by
                    // the recovery. It will not attempt to delete tombstones.
                    undo.truncate(true);

                    // FIXME: If has trash, empty it now.
                }
            } else {
                if (mHasRedo) {
                    mDatabase.mRedoLog.txnCommitScope(mTxnId, parentScope.mTxnId);
                    mHasRedo = false;
                }

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    // Active transaction id is cleared as a side-effect.
                    mSavepoint = undo.savepoint();
                }
            }

            // Next transaction id is assigned on demand.
            mTxnId = 0;
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Enters a nested transaction scope.
     */
    public final void enter() throws IOException {
        check();

        ParentScope parentScope = super.scopeEnter();
        parentScope.mLockMode = mLockMode;
        parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
        parentScope.mTxnId = mTxnId;
        parentScope.mHasRedo = mHasRedo;

        UndoLog undo = mUndoLog;
        if (undo != null) {
            parentScope.mSavepoint = mSavepoint;
            // Active transaction id is cleared as a side-effect.
            mSavepoint = undo.savepoint();
        }

        // Next transaction id is assigned on demand.
        mTxnId = 0;
        mHasRedo = false;
    }

    /**
     * Exits the current transaction scope, rolling back all uncommitted
     * modifications made within. The transaction is still valid after this
     * method is called, unless an exception is thrown.
     */
    public final void exit() throws IOException {
        check();

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                if (mHasRedo) {
                    mDatabase.mRedoLog.txnRollback(mTxnId, 0);
                    mHasRedo = false;
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    // Active transaction id is cleared as a side-effect.
                    undo.rollback(mSavepoint);
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mTxnId = 0;
                mSavepoint = 0;
                if (undo != null) {
                    undo.unregister();
                    mUndoLog = null;
                }
            } else {
                if (mHasRedo) {
                    mDatabase.mRedoLog.txnRollback(mTxnId, parentScope.mTxnId);
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    // Active transaction id is cleared as a side-effect.
                    undo.rollback(mSavepoint);
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mLockMode = parentScope.mLockMode;
                mLockTimeoutNanos = parentScope.mLockTimeoutNanos;
                mTxnId = parentScope.mTxnId;
                mHasRedo = parentScope.mHasRedo;
                mSavepoint = parentScope.mSavepoint;
            }
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Exits all transaction scopes, rolling back all uncommitted
     * modifications.
     */
    public final void reset() throws IOException {
        check();

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                if (mHasRedo) {
                    mDatabase.mRedoLog.txnRollback(mTxnId, 0);
                    mHasRedo = false;
                }
            } else {
                long txnId = mTxnId;
                boolean hasRedo = mHasRedo;
                do {
                    long parentTxnId = parentScope.mTxnId;
                    if (hasRedo) {
                        mDatabase.mRedoLog.txnRollback(txnId, parentTxnId);
                    }
                    txnId = parentTxnId;
                    hasRedo = parentScope.mHasRedo;
                    parentScope = parentScope.mParentScope;
                } while (parentScope != null);
                if (hasRedo) {
                    mDatabase.mRedoLog.txnRollback(txnId, 0);
                }
                mHasRedo = false;
            }

            UndoLog undo = mUndoLog;
            if (undo != null) {
                // Active transaction id is cleared as a side-effect.
                undo.rollback(0);
            }

            // Exit and release all locks.
            super.scopeExitAll();

            mTxnId = 0;
            mSavepoint = 0;
            if (undo != null) {
                undo.unregister();
                mUndoLog = null;
            }
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder(getClass().getName());

        if (this == BOGUS) {
            return b.append('.').append("BOGUS").toString();
        }

        b.append('@').append(Integer.toHexString(hashCode()));

        b.append(" {");
        b.append("id").append(": ").append(mTxnId);
        b.append(", ");
        b.append("durabilityMode").append(": ").append(mDurabilityMode);
        b.append(", ");
        b.append("lockMode").append(": ").append(mLockMode);
        b.append(", ");
        b.append("lockTimeout").append(": ");
        TimeUnit unit = LockTimeoutException.inferUnit(TimeUnit.NANOSECONDS, mLockTimeoutNanos);
        LockTimeoutException.appendTimeout(b, lockTimeout(unit), unit);

        Object borked = mBorked;
        if (borked != null) {
            b.append(", ");
            b.append("invalid").append(": ").append(borked);
        }

        return b.append('}').toString();
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return super.lockShared(indexId, key, mLockTimeoutNanos);
    }

    final LockResult lockShared(long indexId, byte[] key, int hash) throws LockFailureException {
        return super.lockShared(indexId, key, hash, mLockTimeoutNanos);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return super.lockUpgradable(indexId, key, mLockTimeoutNanos);
    }

    final LockResult lockUpgradable(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        return super.lockUpgradable(indexId, key, hash, mLockTimeoutNanos);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned owned},
     * transaction already owns exclusive lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED
     * UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return super.lockExclusive(indexId, key, mLockTimeoutNanos);
    }

    final LockResult lockExclusive(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        return super.lockExclusive(indexId, key, hash, mLockTimeoutNanos);
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoStore(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        try {
            long txnId = mTxnId;
            if (txnId == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null && parentScope.mTxnId == 0) {
                    assignTxnId(parentScope);
                }
                txnId = assignTxnId();
            }

            RedoLog redo = mDatabase.mRedoLog;
            if (redo != null) {
                redo.txnStore(txnId, indexId, key, value);
                mHasRedo = true;
            }
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    private long assignTxnId() throws IOException {
        long txnId;
        UndoLog undo = mUndoLog;
        if (undo == null) {
            txnId = mDatabase.nextTransactionId();
        } else if ((txnId = undo.activeTransactionId()) == 0) {
            txnId = mDatabase.nextTransactionId();
            undo.activeTransactionId(txnId);
        }
        mTxnId = txnId;
        return txnId;
    }

    private void assignTxnId(ParentScope scope) {
        ParentScope parentScope = scope.mParentScope;
        if (parentScope != null && parentScope.mTxnId == 0) {
            assignTxnId(parentScope);
        }
        scope.mTxnId = mDatabase.nextTransactionId();
    }

    final long topTxnId() throws IOException {
        ParentScope parentScope = mParentScope;
        if (parentScope == null) {
            long txnId = mTxnId;
            return txnId == 0 ? assignTxnId() : txnId;
        }
        while (true) {
            ParentScope grandparentScope = parentScope.mParentScope;
            if (grandparentScope == null) {
                break;
            }
            parentScope = grandparentScope;
        }
        long txnId = parentScope.mTxnId;
        return txnId == 0 ? (parentScope.mTxnId = mDatabase.nextTransactionId()) : txnId;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op OP_UPDATE or OP_INSERT
     * @param payload key/value entry, as encoded by leaf node
     */
    final void undoStore(long indexId, byte op, byte[] payload, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().push(indexId, op, payload, off, len);
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void undoDelete(long indexId, byte[] key) throws IOException {
        check();
        try {
            undoLog().push(indexId, UndoLog.OP_DELETE, key, 0, key.length);
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param payload Node encoded key followed by trash id
     */
    final void undoReclaimFragmented(long indexId, byte[] payload, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().push(indexId, UndoLog.OP_RECLAIM_FRAGMENTED, payload, off, len);
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    private UndoLog undoLog() throws IOException {
        UndoLog undo = mUndoLog;
        if (undo == null) {
            undo = UndoLog.newUndoLog(mDatabase, mTxnId);
            mTxnId = undo.activeTransactionId();
            mUndoLog = undo;
        } else if (undo.activeTransactionId() == 0) {
            long txnId = mTxnId;
            if (txnId == 0) {
                mTxnId = txnId = mDatabase.nextTransactionId();
            }
            undo.activeTransactionId(txnId);
        }
        return undo;
    }

    RuntimeException borked(Throwable e) {
        // Because this transaction is now borked, user cannot commit or
        // rollback. Locks cannot be released, ensuring other transactions
        // cannot see the partial changes made by this transaction. A restart
        // is required, which then performs a clean rollback.
        if (mBorked != null) {
            mBorked = e;
        }
        return Utils.rethrow(e);
    }
}
