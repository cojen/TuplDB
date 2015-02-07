/*
 *  Copyright 2011-2013 Brian S O'Neill
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
 *     if (shouldDiscard(c.value()) &amp;&amp; result == LockResult.ACQUIRED) {
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
public class Transaction extends Locker {
    /**
     * Transaction instance which isn't a transaction at all. It always
     * operates in an {@link LockMode#UNSAFE unsafe} lock mode and a {@link
     * DurabilityMode#NO_REDO no-redo} durability mode. For safe auto-commit
     * transactions, pass null for the transaction argument.
     */
    public static final Transaction BOGUS = new Transaction();

    static final int
        HAS_SCOPE  = 1, // When set, scoped has been entered but not logged.
        HAS_COMMIT = 2, // When set, transaction has committable changes.
        HAS_TRASH  = 4; /* When set, fragmented values are in the trash and must be
                           fully deleted after committing the top-level scope. */

    final Database mDatabase;
    final RedoWriter mRedoWriter;
    final DurabilityMode mDurabilityMode;

    private LockMode mLockMode;
    long mLockTimeoutNanos;
    private int mHasState;
    private long mSavepoint;
    private long mTxnId;

    private UndoLog mUndoLog;

    // Is an exception if transaction is borked, BOGUS if bogus.
    private Object mBorked;

    Transaction(Database db, RedoWriter redo, DurabilityMode durabilityMode,
                LockMode lockMode, long timeoutNanos)
    {
        super(db.mLockManager);
        mDatabase = db;
        mRedoWriter = redo;
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
        mLockTimeoutNanos = timeoutNanos;
    }

    // Constructor for redo recovery.
    Transaction(Database db, long txnId, LockMode lockMode, long timeoutNanos) {
        this(db, null, DurabilityMode.NO_REDO, lockMode, timeoutNanos);
        mTxnId = txnId;
    }

    // Constructor for undo recovery.
    Transaction(Database db, long txnId, LockMode lockMode, long timeoutNanos, int hasState) {
        this(db, null, DurabilityMode.NO_REDO, lockMode, timeoutNanos);
        mTxnId = txnId;
        mHasState = hasState;
    }

    // Used by recovery.
    final void recoveredScope(long savepoint, int hasState) {
        ParentScope parentScope = super.scopeEnter();
        parentScope.mLockMode = mLockMode;
        parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
        parentScope.mHasState = mHasState;
        parentScope.mSavepoint = mSavepoint;
        mSavepoint = savepoint;
        mHasState = hasState;
    }

    // Used by recovery.
    final void recoveredUndoLog(UndoLog undo) {
        mDatabase.register(undo);
        mUndoLog = undo;
    }

    // Constructor for BOGUS transaction.
    private Transaction() {
        super();
        mDatabase = null;
        mRedoWriter = null;
        mDurabilityMode = DurabilityMode.NO_REDO;
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
                    int hasState = mHasState;
                    if ((hasState & HAS_COMMIT) != 0) {
                        RedoWriter redo = mRedoWriter;
                        long commitPos = redo.txnCommitFinal(mTxnId, mDurabilityMode);
                        if (commitPos != 0) {
                            redo.txnCommitSync(this, commitPos);
                        }
                        mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
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
                    long commitPos;
                    try {
                        if ((commitPos = (mHasState & HAS_COMMIT)) != 0) {
                            commitPos = mRedoWriter.txnCommitFinal(mTxnId, mDurabilityMode);
                            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
                        }
                        // Indicates that undo log should be truncated instead
                        // of rolled back during recovery. Commit lock can now
                        // be released safely. See recoveryCleanup.
                        undo.pushCommit();
                    } finally {
                        sharedCommitLock.unlock();
                    }

                    if (commitPos != 0) {
                        // Durably sync the redo log after releasing the commit lock,
                        // preventing additional blocking.
                        if (mDurabilityMode == DurabilityMode.SYNC) {
                            mRedoWriter.txnCommitSync(this, commitPos);
                        } else {
                            PendingTxn pending = transferExclusive();
                            pending.mTxnId = mTxnId;
                            pending.mCommitPos = commitPos;
                            pending.mUndoLog = undo;
                            mUndoLog = null;
                            int hasState = mHasState;
                            if ((hasState & HAS_TRASH) != 0) {
                                pending.mHasFragmentedTrash = true;
                                mHasState = hasState & ~HAS_TRASH;
                            }
                            mTxnId = 0;
                            mRedoWriter.txnCommitPending(pending);
                            return;
                        }
                    }

                    // Calling this deletes any ghosts too.
                    super.scopeUnlockAll();

                    // Truncate obsolete log entries after releasing locks.
                    // Recovery might need to re-delete ghosts, which is only
                    // possible with a complete undo log. Truncate operation
                    // can be interrupted by a checkpoint, allowing a partial
                    // undo log to be seen by the recovery. It will not attempt
                    // to delete ghosts.
                    undo.truncate(true);

                    mDatabase.unregister(undo);
                    mUndoLog = null;

                    int hasState = mHasState;
                    if ((hasState & HAS_TRASH) != 0) {
                        mDatabase.fragmentedTrash().emptyTrash(mTxnId);
                        mHasState = hasState & ~HAS_TRASH;
                    }
                }

                mTxnId = 0;
            } else {
                int hasState = mHasState;
                if ((hasState & HAS_COMMIT) != 0) {
                    mRedoWriter.txnCommit(mTxnId);
                    mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                    parentScope.mHasState |= HAS_COMMIT;
                }

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    mSavepoint = undo.scopeCommit();
                }
            }
        } catch (Throwable e) {
            throw borked(e, true);
        }
    }

    /**
     * Commits and exits all transaction scopes.
     */
    public final void commitAll() throws IOException {
        while (true) {
            commit();
            if (mParentScope == null) {
                break;
            }
            exit();
        }
    }

    /**
     * Enters a nested transaction scope.
     */
    public final void enter() throws IOException {
        check();

        try {
            ParentScope parentScope = super.scopeEnter();
            parentScope.mLockMode = mLockMode;
            parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
            parentScope.mHasState = mHasState;

            UndoLog undo = mUndoLog;
            if (undo != null) {
                parentScope.mSavepoint = mSavepoint;
                mSavepoint = undo.scopeEnter();
            }

            // Scope and commit states are set upon first actual use of this scope.
            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
        } catch (Throwable e) {
            throw borked(e, true);
        }
    }

    /**
     * Exits the current transaction scope, rolling back all uncommitted
     * modifications made within. The transaction is still valid after this
     * method is called, unless an exception is thrown.
     */
    public final void exit() throws IOException {
        if (mBorked != null) {
            return;
        }

        try {
            ParentScope parentScope = mParentScope;
            if (parentScope == null) {
                int hasState = mHasState;
                if ((hasState & HAS_SCOPE) != 0) {
                    mRedoWriter.txnRollbackFinal(mTxnId);
                }
                mHasState = 0;

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.rollback();
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mSavepoint = 0;
                if (undo != null) {
                    mDatabase.unregister(undo);
                    mUndoLog = null;
                }

                mTxnId = 0;
            } else {
                int hasState = mHasState;
                if ((mHasState & HAS_SCOPE) != 0) {
                    mRedoWriter.txnRollback(mTxnId);
                    mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.scopeRollback(mSavepoint);
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mLockMode = parentScope.mLockMode;
                mLockTimeoutNanos = parentScope.mLockTimeoutNanos;
                // Use or assignment to promote HAS_TRASH state.
                mHasState |= parentScope.mHasState;
                mSavepoint = parentScope.mSavepoint;
            }
        } catch (Throwable e) {
            throw borked(e, true);
        }
    }

    /**
     * Exits all transaction scopes, rolling back all uncommitted
     * modifications.
     */
    public final void reset() throws IOException {
        if (mBorked != null) {
            return;
        }

        try {
            int hasState = mHasState;
            ParentScope parentScope = mParentScope;
            while (parentScope != null) {
                if ((hasState & HAS_SCOPE) != 0) {
                    mRedoWriter.txnRollback(mTxnId);
                }
                hasState = parentScope.mHasState;
                parentScope = parentScope.mParentScope;
            }
            if ((hasState & HAS_SCOPE) != 0) {
                mRedoWriter.txnRollbackFinal(mTxnId);
            }
            mHasState = 0;

            UndoLog undo = mUndoLog;
            if (undo != null) {
                undo.rollback();
            }

            // Exit and release all locks.
            super.scopeExitAll();

            mSavepoint = 0;
            if (undo != null) {
                mDatabase.unregister(undo);
                mUndoLog = null;
            }

            mTxnId = 0;
        } catch (Throwable e) {
            throw borked(e, true);
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
        TimeUnit unit = Utils.inferUnit(TimeUnit.NANOSECONDS, mLockTimeoutNanos);
        Utils.appendTimeout(b, lockTimeout(unit), unit);

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
     * @param newLock Lock instance to insert, unless another already exists. The mIndexId,
     * mKey, and mHashCode fields must be set.
     */
    final LockResult lockExclusive(org.cojen.tupl.Lock lock) throws LockFailureException {
        return super.lockExclusive(lock, mLockTimeoutNanos);
    }

    /**
     * @param resetAlways when false, only resets committed transactions
     * @return true if was reset
     */
    final boolean recoveryCleanup(boolean resetAlways) throws IOException {
        UndoLog undo = mUndoLog;
        if (undo != null) {
            switch (undo.peek(true)) {
            default:
                break;

            case UndoLog.OP_COMMIT:
                // Transaction was actually committed, but redo log is gone. This can happen
                // when a checkpoint completes in the middle of the transaction commit
                // operation. Method truncates undo log as a side-effect.
                undo.deleteGhosts();
                resetAlways = true;
                break;

            case UndoLog.OP_COMMIT_TRUNCATE:
                // Like OP_COMMIT, but ghosts have already been deleted.
                undo.truncate(false);
                resetAlways = true;
                break;
            }
        }

        if (resetAlways) {
            reset();
        }

        return resetAlways;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoStore(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        try {
            RedoWriter redo = mRedoWriter;
            if (redo != null) {
                long txnId = txnId();

                int hasState = mHasState;
                if ((hasState & HAS_SCOPE) == 0) {
                    ParentScope parentScope = mParentScope;
                    if (parentScope != null) {
                        setScopeState(redo, parentScope);
                    }
                    if (value == null) {
                        redo.txnDelete(RedoOps.OP_TXN_ENTER_DELETE, txnId, indexId, key);
                    } else {
                        redo.txnStore(RedoOps.OP_TXN_ENTER_STORE, txnId, indexId, key, value);
                    }
                    hasState |= (HAS_SCOPE | HAS_COMMIT);
                } else {
                    if (value == null) {
                        redo.txnDelete(RedoOps.OP_TXN_DELETE, txnId, indexId, key);
                    } else {
                        redo.txnStore(RedoOps.OP_TXN_STORE, txnId, indexId, key, value);
                    }
                    hasState |= HAS_COMMIT;
                }
                mHasState = hasState;
            }
        } catch (Throwable e) {
            throw borked(e, false);
        }
    }

    private void setScopeState(RedoWriter redo, ParentScope scope) throws IOException {
        int hasState = scope.mHasState;
        if ((hasState & HAS_SCOPE) == 0) {
            ParentScope parentScope = scope.mParentScope;
            if (parentScope != null) {
                setScopeState(redo, parentScope);
            }

            redo.txnEnter(txnId());
            scope.mHasState = hasState | HAS_SCOPE;
        }
    }

    /**
     * Caller must hold commit lock if transaction id has not been assigned yet.
     */
    final long txnId() throws IOException {
        long txnId = mTxnId;
        if (txnId == 0) {
            RedoWriter redo = mRedoWriter;
            if (redo != null) {
                // Replicas cannot create loggable transactions.
                redo.opWriteCheck();
            }
            mTxnId = txnId = mDatabase.nextTransactionId();
        }
        return txnId;
    }

    final void setHasTrash() {
        mHasState |= HAS_TRASH;
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
            throw borked(e, false);
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
            throw borked(e, false);
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
            throw borked(e, false);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    private UndoLog undoLog() throws IOException {
        UndoLog undo = mUndoLog;
        if (undo == null) {
            undo = new UndoLog(mDatabase, txnId());

            // TODO: Optimize into one scopeEnter(n) call.
            ParentScope parentScope = mParentScope;
            while (parentScope != null) {
                undo.scopeEnter();
                parentScope = parentScope.mParentScope;
            }

            mDatabase.register(undo);
            mUndoLog = undo;
        }
        return undo;
    }

    /**
     * @param rollback Rollback should only be performed by user operations -- the public API.
     * Otherwise a latch deadlock can occur.
     */
    final RuntimeException borked(Throwable e, boolean rollback) {
        if (mBorked == null) {
            if (mDatabase.mClosed) {
                Throwable cause = mDatabase.mClosedCause;
                if (cause != null) {
                    try {
                        e.initCause(cause);
                    } catch (IllegalArgumentException | IllegalStateException e2) {
                    }
                }
                mBorked = e;
            } else if (rollback) {
                // Attempt to rollback the mess and release the locks.
                UndoLog undo = mUndoLog;
                try {
                    if (undo != null) {
                        undo.rollback();
                    }
                    super.scopeExitAll();
                    if (undo != null) {
                        mDatabase.unregister(undo);
                        mUndoLog = null;
                    }
                } catch (Throwable e2) {
                    // Undo failed. Locks cannot be released, ensuring other
                    // transactions cannot see the partial changes made by this
                    // transaction. A restart is required, which then performs
                    // a clean rollback.
                    if (mDatabase.mClosed) {
                        Throwable cause = mDatabase.mClosedCause;
                        if (cause != null) {
                            try {
                                e.initCause(cause);
                            } catch (IllegalStateException e3) {
                            }
                        }
                    }
                    try {
                        e2.initCause(e);
                        e = e2;
                    } catch (IllegalStateException e3) {
                    }
                    mBorked = e;
                }
            }
        }

        return Utils.rethrow(e);
    }
}
