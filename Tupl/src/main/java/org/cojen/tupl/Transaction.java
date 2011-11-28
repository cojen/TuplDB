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
 * Transaction instances can only be safely used by one thread at a time.
 * Transactions can be exchanged by threads, as long as a happens-before
 * relationship is established. Without proper exclusion, multiple threads
 * interacting with a Transaction instance may cause database corruption.
 *
 * @author Brian S O'Neill
 */
public class Transaction extends Locker {
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

    // Exception thrown during critcal operation.
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
     * Attempt to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is OWNED_*, locker already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     */
    public final LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return super.lockShared(indexId, key, mLockTimeoutNanos);
    }

    /**
     * Attempt to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is OWNED_*,
     * locker already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     */
    public final LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return super.lockUpgradable(indexId, key, mLockTimeoutNanos);
    }

    /**
     * Attempt to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is OWNED_EXCLUSIVE, locker already
     * owns exclusive lock, and no extra unlock should be performed.
     *
     * @param key non-null key to lock; instance is not cloned
     * @return ACQUIRED, UPGRADED, or OWNED_EXCLUSIVE
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     */
    public final LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return super.lockExclusive(indexId, key, mLockTimeoutNanos);
    }

    /**
     * Set the lock mode for the current scope. Transactions begin in {@link
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
     * Set the lock timeout for the current scope. A negative timeout is
     * infinite.
     */
    public final void lockTimeout(long timeout, TimeUnit unit) {
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
    }

    /**
     * Returns the current lock timeout, in nanoseconds.
     */
    public final long lockTimeoutNanos() {
        return mLockTimeoutNanos;
    }

    /**
     * Commit all modifications made within the current transaction scope. The
     * current scope is still valid after this method is called, unless an
     * exception is thrown. Call exit to fully release transaction resources.
     */
    public final void commit() throws IOException {
        check();

        try {
            Scope parentScope = mParentScope;
            if (mHasRedo) {
                long parentTxnId = parentScope == null ? 0 : parentScope.mTxnId;
                mDatabase.mRedoLog.txnCommit(mTxnId, parentTxnId, mDurabilityMode);
                mHasRedo = false;
            }

            UndoLog undo = mUndoLog;
            if (undo != null) {
                // Active transaction id is cleared as a side-effect.
                undo.truncate(mSavepoint);
            }

            if (parentScope == null) {
                super.scopeUnlockAll();
            } else {
                // Retain locks for modifications which aren't truly committed yet.
                super.scopeUnlockAllNonExclusive();
            }

            // Next transaction id is assigned on demand.
            mTxnId = 0;
        } catch (Throwable e) {
            throw borked(e);
        }
    }

    /**
     * Enter a nested transaction scope.
     */
    public final void enter() throws IOException {
        check();

        Scope parentScope = super.scopeEnter();
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
     * Exit the current transaction scope, rolling back all uncommitted
     * modifications made within. The transaction is still valid after this
     * method is called, unless an exception is thrown.
     */
    public final void exit() throws IOException {
        check();

        try {
            Scope parentScope = mParentScope;
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

            super.scopeExit(true);

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
     * Exit all transaction scopes, rolling back all uncommitted modifications.
     * The transaction is still valid after this method is called, unless an
     * exception is thrown.
     */
    public final void exitAll() throws IOException {
        check();
        // FIXME
        throw null;
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
                Scope parentScope = mParentScope;
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

    private void assignTxnId(Scope scope) {
        Scope parentScope = scope.mParent;
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
            undo = new UndoLog(mDatabase, mTxnId);
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

    private void check() throws DatabaseException {
        Object borked = mBorked;
        if (borked != null) {
            if (borked == BOGUS) {
                throw new DatabaseException("Transaction is bogus");
            } else {
                throw new DatabaseException("Invalid transaction, caused by: " + borked);
            }
        }
    }
}
