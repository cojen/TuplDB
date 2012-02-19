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
 * Defines a logical unit of work. Transactions must be {@link #reset reset}
 * when no longer needed to free up resources. Transaction instances can only
 * be safely used by one thread at a time. Instances can be exchanged by
 * threads, as long as a happens-before relationship is established. Without
 * proper exclusion, multiple threads interacting with a Transaction instance
 * may cause database corruption.
 *
 * <p>Transactions also contain various methods for directly controlling locks,
 * although their use is not required. Methods which operate upon transactions
 * acquire and release locks automatically. Direct control over locks is
 * provided for advanced use cases. These methods are documented as such.
 *
 * @author Brian S O'Neill
 * @see Database#newTransaction
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

    LockMode mLockMode;
    long mLockTimeoutNanos;
    long mTxnId;
    boolean mHasRedo;
    long mSavepoint;

    private UndoLog mUndoLog;

    // Is an exception if transaction is borked, BOGUS if bogus.
    private Object mBorked;

    // TODO: Define autoCommit(boolean) method.

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
     * LockMode#UPGRADABLE_READ} mode, and newly entered scopes begin at the
     * outer scope's current mode. Exiting a scope reverts the lock mode.
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
            if (mHasRedo) {
                long parentTxnId = parentScope == null ? 0 : parentScope.mTxnId;
                mDatabase.mRedoLog.txnCommit(mTxnId, parentTxnId, mDurabilityMode);
                mHasRedo = false;
            }

            UndoLog undo = mUndoLog;
            if (parentScope == null) {
                super.scopeUnlockAll();
                // Safe to truncate obsolete log entries after releasing locks.
                if (undo != null) {
                    // Active transaction id is cleared as a side-effect.
                    undo.truncate();
                }
            } else {
                super.promote();
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
            if (mHasRedo) {
                long parentTxnId = parentScope == null ? 0 : parentScope.mTxnId;
                mDatabase.mRedoLog.txnRollback(mTxnId, parentTxnId);
                mHasRedo = false;
            }

            UndoLog undo = mUndoLog;
            if (undo != null) {
                // Active transaction id is cleared as a side-effect.
                undo.rollback(mSavepoint);
            }

            // Exit and release all locks obtained in this scope.
            super.scopeExit();

            if (parentScope == null) {
                mTxnId = 0;
                mSavepoint = 0;
                if (undo != null) {
                    undo.unregister();
                    mUndoLog = null;
                }
            } else {
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

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is OWNED_*, locker already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full
     * non-zero timeout
     */
    public final LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return super.lockShared(indexId, key, mLockTimeoutNanos);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is OWNED_*,
     * locker already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full
     * non-zero timeout
     */
    public final LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return super.lockUpgradable(indexId, key, mLockTimeoutNanos);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is OWNED_EXCLUSIVE, locker already
     * owns exclusive lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, UPGRADED, or OWNED_EXCLUSIVE
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full
     * non-zero timeout
     */
    public final LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return super.lockExclusive(indexId, key, mLockTimeoutNanos);
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
                UndoLog undo = mUndoLog;
                if (undo == null) {
                    txnId = mDatabase.nextTransactionId();
                } else if ((txnId = undo.activeTransactionId()) == 0) {
                    txnId = mDatabase.nextTransactionId();
                    undo.activeTransactionId(txnId);
                }
                mTxnId = txnId;
            }

            mDatabase.mRedoLog.txnStore(txnId, indexId, key, value);
            mHasRedo = true;
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    private void assignTxnId(ParentScope scope) {
        ParentScope parentScope = scope.mParentScope;
        if (parentScope != null && parentScope.mTxnId == 0) {
            assignTxnId(parentScope);
        }
        scope.mTxnId = mDatabase.nextTransactionId();
    }

    /**
     * Caller must hold commit lock.
     *
     * @param payload key/value entry, as encoded by leaf node
     */
    final void undoStore(long indexId, byte[] payload, int off, int len) throws IOException {
        check();
        try {
            undoLog().push(indexId, UndoLog.OP_STORE, payload, off, len);
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

    private RuntimeException borked(Throwable e) {
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
