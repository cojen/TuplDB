/*
 *  Copyright (C) 2011-2017 Cojen.org
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

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.InvalidTransactionException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import static org.cojen.tupl.core.RedoOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Standard transaction implementation. The name "LocalTransaction" is used to imply that the
 * transaction is local to the current machine and not remotely accessed, although no remote
 * database layer exists. This class could just as well have been named "TransactionImpl".
 *
 * @author Brian S O'Neill
 */
public final class LocalTransaction extends Locker implements CoreTransaction {
    public static final LocalTransaction BOGUS = new LocalTransaction();

    // When set, scope has been entered and logged.
    private static final int HAS_SCOPE = 1;

    // When set, transaction has committable changes.
    private static final int HAS_COMMIT = 2;

    // When set, fragmented values are possibly in the trash and must be fully deleted after
    // committing the top-level scope.
    static final int HAS_TRASH = 4;
                            
    // When set, transaction is possibly prepared for two-phase commit.
    static final int HAS_PREPARE = 8;

    // Must be set with HAS_PREPARE to indicate that prepareCommit was called.
    static final int HAS_PREPARE_COMMIT = 16;

    final LocalDatabase mDatabase;
    final TransactionContext mContext;
    RedoWriter mRedo;
    DurabilityMode mDurabilityMode;

    LockMode mLockMode;
    long mLockTimeoutNanos;
    int mHasState;
    private long mSavepoint;
    long mTxnId;

    UndoLog mUndoLog;

    private Object mAttachment;

    // Is an exception if transaction is borked, BOGUS if bogus.
    private Object mBorked;

    LocalTransaction(LocalDatabase db, RedoWriter redo, DurabilityMode durabilityMode,
                     LockMode lockMode, long timeoutNanos)
    {
        super(db.mLockManager);
        mDatabase = db;
        mContext = db.selectTransactionContext(this);
        mRedo = redo;
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
        mLockTimeoutNanos = timeoutNanos;
    }

    // Constructor for redo recovery.
    LocalTransaction(LocalDatabase db, long txnId, LockMode lockMode, long timeoutNanos) {
        this(db, null, DurabilityMode.NO_REDO, lockMode, timeoutNanos);
        mTxnId = txnId;
    }

    // Constructor for undo recovery.
    LocalTransaction(LocalDatabase db, long txnId, int hasState) {
        this(db, null, DurabilityMode.NO_REDO, LockMode.UPGRADABLE_READ, 0);
        mTxnId = txnId;
        // Blindly assume that trash must be deleted. No harm if none exists.
        mHasState = hasState | HAS_TRASH;
    }

    // Constructor for carrier transaction used to prepare or rollback prepared transactions.
    private LocalTransaction(LocalTransaction txn) {
        super(txn.mDatabase.mLockManager, txn.mHash);
        mDatabase = txn.mDatabase;
        mContext = txn.mContext; // same context means common flush buffer
        mRedo = txn.mRedo;
        mDurabilityMode = DurabilityMode.SYNC;
        mLockMode = LockMode.UNSAFE;
    }

    // Constructor for BOGUS transaction.
    private LocalTransaction() {
        super(null);
        mDatabase = null;
        mContext = null;
        mRedo = null;
        mDurabilityMode = DurabilityMode.NO_REDO;
        mLockMode = LockMode.UNSAFE;
        mBorked = this;
    }

    // Used by recovery.
    final void recoveredScope(long savepoint, int hasState) {
        ParentScope parentScope = super.scopeEnter();
        parentScope.mLockMode = LockMode.UPGRADABLE_READ;
        parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
        parentScope.mHasState = mHasState;
        parentScope.mSavepoint = mSavepoint;
        mSavepoint = savepoint;
        mHasState = hasState;
    }

    // Used by recovery.
    final void recoveredUndoLog(UndoLog undo) {
        mContext.register(undo);
        mUndoLog = undo;
    }

    @Override
    public final LocalDatabase getDatabase() {
        return mDatabase;
    }

    @Override
    public final void attach(Object obj) {
        mAttachment = obj;
    }

    @Override
    public final Object attachment() {
        return mAttachment;
    }

    @Override
    public final void lockMode(LockMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Lock mode is null");
        } else {
            bogusCheck();
            mLockMode = mode;
        }
    }

    @Override
    public final LockMode lockMode() {
        return mLockMode;
    }

    @Override
    public final void lockTimeout(long timeout, TimeUnit unit) {
        bogusCheck();
        mLockTimeoutNanos = Utils.toNanos(timeout, unit);
    }

    @Override
    public final long lockTimeout(TimeUnit unit) {
        long timeoutNanos = mLockTimeoutNanos;
        return timeoutNanos < 0 ? -1 : unit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public final void durabilityMode(DurabilityMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Durability mode is null");
        } else {
            bogusCheck();
            mDurabilityMode = mode;
        }
    }

    @Override
    public final DurabilityMode durabilityMode() {
        return mDurabilityMode;
    }

    @Override
    public final void check() throws DatabaseException {
        Object borked = mBorked;
        if (borked != null) {
            check(borked);
        }
    }

    @Override
    public final boolean isBogus() {
        return mBorked == BOGUS;
    }

    private void check(Object borked) throws DatabaseException {
        if (borked == BOGUS) {
            throw new IllegalStateException("Transaction is bogus");
        } else if (borked instanceof Throwable t) {
            throw new InvalidTransactionException(t);
        } else {
            throw new InvalidTransactionException(String.valueOf(borked));
        }
    }

    private void bogusCheck() {
        if (mBorked == BOGUS) {
            throw new IllegalStateException("Transaction is bogus");
        }
    }

    @Override
    public final void commit() throws IOException {
        Object borked = mBorked;
        if (borked != null) {
            if (borked == BOGUS) {
                return;
            }
            check(borked);
        }

        ParentScope parentScope = mParentScope;
        if (parentScope == null) {
            try {
                UndoLog undo = mUndoLog;
                if (undo == null) {
                    int hasState = mHasState;
                    if ((hasState & HAS_COMMIT) != 0) {
                        long commitPos = mContext.redoCommitFinal(this);
                        mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                        if (commitPos != 0) {
                            if (commitPos == -1) {
                                // Pending.
                                return;
                            }
                            mRedo.txnCommitSync(commitPos);
                        }
                    }
                    super.scopeUnlockAll();
                } else {
                    // Holding the shared commit lock ensures that the redo log doesn't
                    // disappear before the undo log. Lingering undo logs with no corresponding
                    // redo log are treated as aborted. Recovery would erroneously rollback
                    // committed transactions.
                    final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
                    long commitPos;
                    try {
                        if ((commitPos = (mHasState & HAS_COMMIT)) != 0) {
                            commitPos = mContext.redoCommitFinal(this);
                            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
                        }
                        // Indicates that undo log should be truncated instead
                        // of rolled back during recovery. Commit lock can now
                        // be released safely. See recoveryCleanup.
                        undo.commit();
                    } finally {
                        shared.release();
                    }

                    if (commitPos != 0) {
                        if (commitPos == -1) {
                            // Pending.
                            return;
                        }

                        try {
                            // Durably sync the redo log after releasing the commit lock,
                            // preventing additional blocking.
                            mRedo.txnCommitSync(commitPos);
                        } catch (Throwable e) {
                            commitSyncFailed(e, commitPos);
                            throw e;
                        }
                    }

                    // Calling this deletes any ghosts too.
                    super.scopeUnlockAll();

                    // Truncate obsolete log entries after releasing locks. Recovery might need
                    // to re-delete ghosts, which is only possible with a complete undo log.
                    // Truncate operation can be interrupted by a checkpoint, allowing a
                    // partial undo log to be seen by the recovery.
                    undo.truncate();

                    mContext.unregister(undo);
                    mUndoLog = null;

                    int hasState = mHasState;
                    if ((hasState & HAS_TRASH) != 0) {
                        emptyTrash(hasState);
                    }
                }

                mTxnId = 0;
            } catch (Throwable e) {
                borked(e, true, true); // rollback = true, rethrow = true
            }
        } else {
            try {
                int hasState = mHasState;
                if ((hasState & HAS_COMMIT) != 0) {
                    mContext.redoCommit(mRedo, mTxnId);
                    mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                    parentScope.mHasState |= HAS_COMMIT;
                }

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    mSavepoint = undo.scopeCommit();
                }
            } catch (Throwable e) {
                borked(e);
            }
        }
    }

    private void commitSyncFailed(Throwable e, long commitPos) {
        if (!isRecoverable(e)) {
            panic(e);
            return;
        }

        try {
            if (e instanceof UnmodifiableReplicaException) {
                mUndoLog.uncommit();
                mContext.uncommitted(mTxnId);
            } else if (mRedo instanceof ReplWriter rw) {
                PendingTxn pending = preparePending();
                rw.mReplWriter.uponCommit(commitPos, pos -> {
                    pending.commitPos(pos);
                    pending.run();
                });
            }
        } catch (Throwable e2) {
            suppress(e, e2);
            panic(e);
        }
    }

    PendingTxn preparePending() {
        return new PendingTxn(this);
    }

    private void emptyTrash(int hasState) throws IOException {
        BTree trash = mDatabase.tryFragmentedTrash();
        if (trash != null) {
            FragmentedTrash.emptyTrash(trash, mTxnId);
        }
        mHasState = hasState & ~HAS_TRASH;
    }

    /**
     * Commit combined with a store operation.
     *
     * @param undoTxn pass this if undo logging is required, BOGUS otherwise
     */
    final void storeCommit(LocalTransaction undoTxn, BTreeCursor cursor, byte[] value)
        throws IOException
    {
        if (mRedo == null) {
            cursor.storeNoRedo(this, value);
            commit();
            return;
        }

        check();

        // Implementation consists of redoStore and commit logic, without extraneous checks.

        long txnId = mTxnId;

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            if (txnId == 0) {
                txnId = doAssignTransactionId();
            }
        } catch (Throwable e) {
            shared.release();
            throw e;
        }

        int hasState = mHasState;
        byte[] key = cursor.mKey;

        ParentScope parentScope = mParentScope;
        if (parentScope == null) {
            try {
                long commitPos;
                try {
                    cursor.storeNoRedo(undoTxn, value);

                    if ((hasState & HAS_SCOPE) == 0) {
                        mContext.redoEnter(mRedo, txnId);
                        mHasState = hasState | HAS_SCOPE;
                    }

                    long cursorId = cursor.mCursorId;
                    if (cursorId == 0) {
                        long indexId = cursor.mTree.mId;
                        if (value == null) {
                            commitPos = mContext.redoDeleteCommitFinal(this, indexId, key);
                        } else {
                            commitPos = mContext.redoStoreCommitFinal(this, indexId, key, value);
                        }
                    } else {
                        if (value == null) {
                            mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                        } else {
                            mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                        }
                        commitPos = mContext.redoCommitFinal(this);
                    }
                } catch (Throwable e) {
                    shared.release();
                    throw e;
                }

                mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);

                UndoLog undo = mUndoLog;
                if (undo == null) {
                    shared.release();
                    if (commitPos != 0) {
                        if (commitPos == -1) {
                            // Pending.
                            return;
                        }
                        mRedo.txnCommitSync(commitPos);
                    }
                    super.scopeUnlockAll();
                } else {
                    undo.commit();
                    shared.release();

                    if (commitPos != 0) {
                        if (commitPos == -1) {
                            // Pending.
                            return;
                        }

                        try {
                            mRedo.txnCommitSync(commitPos);
                        } catch (Throwable e) {
                            commitSyncFailed(e, commitPos);
                            throw e;
                        }
                    }

                    super.scopeUnlockAll();

                    undo.truncate();

                    mContext.unregister(undo);
                    mUndoLog = null;

                    if ((hasState & HAS_TRASH) != 0) {
                        emptyTrash(hasState);
                    }
                }

                mTxnId = 0;
            } catch (Throwable e) {
                borked(e, true, true); // rollback = true, rethrow = true
            }
        } else {
            try {
                try {
                    // Always undo when inside a scope.
                    cursor.storeNoRedo(this, value);

                    long cursorId = cursor.mCursorId;
                    if (cursorId == 0) {
                        long indexId = cursor.mTree.mId;
                        if ((hasState & HAS_SCOPE) == 0) {
                            setScopeState(parentScope);
                            if (value == null) {
                                mContext.redoDelete
                                    (mRedo, OP_TXN_DELETE, txnId, indexId, key);
                            } else {
                                mContext.redoStore
                                    (mRedo, OP_TXN_STORE, txnId, indexId, key, value);
                            }
                        } else {
                            if (value == null) {
                                mContext.redoDelete
                                    (mRedo, OP_TXN_DELETE_COMMIT, txnId, indexId, key);
                            } else {
                                mContext.redoStore
                                    (mRedo, OP_TXN_STORE_COMMIT, txnId, indexId, key, value);
                            }
                        }
                    } else {
                        if ((hasState & HAS_SCOPE) == 0) {
                            setScopeState(parentScope);
                            if (value == null) {
                                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                            } else {
                                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                            }
                        } else {
                            if (value == null) {
                                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
                            } else {
                                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
                            }
                            mContext.redoCommit(mRedo, txnId);
                        }
                    }
                } finally {
                    shared.release();
                }

                mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                parentScope.mHasState |= HAS_COMMIT;

                super.promote();

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    mSavepoint = undo.scopeCommit();
                }
            } catch (Throwable e) {
                borked(e);
            }
        }
    }

    @Override
    public final void commitAll() throws IOException {
        while (true) {
            commit();
            if (mParentScope == null) {
                break;
            }
            exit();
        }
    }

    @Override
    public final void enter() throws IOException {
        Object borked = mBorked;
        if (borked != null) {
            if (borked == BOGUS) {
                return;
            }
            check(borked);
        }

        try {
            ParentScope parentScope = super.scopeEnter();
            parentScope.mLockMode = mLockMode;
            parentScope.mLockTimeoutNanos = mLockTimeoutNanos;
            parentScope.mHasState = mHasState;

            mLockMode = LockMode.UPGRADABLE_READ;

            UndoLog undo = mUndoLog;
            if (undo != null) {
                parentScope.mSavepoint = mSavepoint;
                mSavepoint = undo.scopeEnter();
            }

            // Scope and commit states are set upon first actual use of this scope.
            mHasState &= ~(HAS_SCOPE | HAS_COMMIT);
        } catch (Throwable e) {
            borked(e);
        }
    }

    @Override
    public final void exit() {
        if (mBorked != null) {
            super.scopeExit();
            return;
        }

        ParentScope parentScope = mParentScope;
        if (parentScope == null) {
            try {
                doRollbackAll(mHasState);
            } catch (Throwable e) {
                borked(e, true, null); // rollback = true, rethrow = maybe
            }
        } else {
            try {
                try {
                    int hasState = mHasState;
                    if ((hasState & HAS_SCOPE) != 0) {
                        mContext.redoRollback(mRedo, mTxnId);
                        mHasState = hasState & ~(HAS_SCOPE | HAS_COMMIT);
                    }
                } catch (UnmodifiableReplicaException e) {
                    // Suppress and let undo proceed.
                }

                UndoLog undo = mUndoLog;
                if (undo != null) {
                    undo.scopeRollback(mSavepoint);
                }

                // Exit and release all locks obtained in this scope.
                super.scopeExit();

                mLockMode = parentScope.mLockMode;
                mLockTimeoutNanos = parentScope.mLockTimeoutNanos;
                // Use 'or' assignment to keep HAS_TRASH state.
                mHasState |= parentScope.mHasState;
                mSavepoint = parentScope.mSavepoint;
            } catch (Throwable e) {
                borked(e, false, null); // rollback = false, rethrow = maybe
            }
        }
    }

    @Override
    public final void reset() {
        if (mBorked == null) {
            try {
                rollbackAll();
            } catch (Throwable e) {
                borked(e, true, null); // rollback = true, rethrow = maybe
            }
        } else {
            super.scopeExitAll();
        }
    }

    @Override
    public final void reset(Throwable cause) {
        if (cause == null) {
            try {
                reset();
                return;
            } catch (Throwable e) {
                cause = e;
            }
        }

        borked(cause, true, false); // rollback = true, rethrow = false
    }

    @Override
    public void rollback() throws IOException {
        if (!isNested()) {
            reset();
        } else {
            LockMode lockMode = mLockMode;
            exit();
            enter();
            mLockMode = lockMode;
        }
    }

    private void rollbackAll() throws IOException {
        int hasState = mHasState;
        ParentScope parentScope = mParentScope;
        while (parentScope != null) {
            hasState |= parentScope.mHasState;
            parentScope = parentScope.mParentScope;
        }

        doRollbackAll(hasState);
    }

    private void doRollbackAll(int hasState) throws IOException {
        if (hasState != 0) {
            if ((hasState & HAS_PREPARE) != 0 && tryPreparedRollback()) {
                return;
            }

            if ((hasState & (HAS_SCOPE | HAS_COMMIT)) != 0) {
                try {
                    mContext.redoRollbackFinal(mRedo, mTxnId);
                } catch (UnmodifiableReplicaException e) {
                    // Suppress and let undo proceed.
                }
            }

            mHasState = 0;
        }

        UndoLog undo = mUndoLog;
        if (undo != null) {
            undo.rollback();
        }

        // Exit and release all locks.
        super.scopeExitAll();

        mSavepoint = 0;
        if (undo != null) {
            mContext.unregister(undo);
            mUndoLog = null;
        }

        mTxnId = 0;
    }

    /**
     * Rollback of a prepared transaction requires consensus.
     *
     * @return false if not actually prepared
     */
    private boolean tryPreparedRollback() throws IOException {
        BTreeCursor c = checkPrepared();
        if (c == null) {
            return false;
        }

        try {
            // Perform partial rollback to the prepare state, which doesn't require consensus.
            mUndoLog.rollbackToPrepare();
            unlockToPrepare();

            // Commit a carrier transaction to safely attain consensus.
            var carrier = new LocalTransaction(this);
            try {
                CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
                try {
                    // The undo operation undeletes the prepare entry, without locking it.
                    carrier.undoLog().pushPreparedUnrollback(c.key());

                    // Replace the ghost with an empty value, to signify rollback.
                    c.store(EMPTY_BYTES);

                    // Note: Both transactions have the same context and redo.
                    mContext.redoPrepareRollback(mRedo, carrier.id(), mTxnId);
                    carrier.mHasState |= HAS_COMMIT;
                } finally {
                    shared.release();
                }

                carrier.commit();
            } catch (Throwable e) {
                carrier.reset(e);

                if (e instanceof UnmodifiableReplicaException) {
                    preparedHandoff(e);
                }

                throw e;
            }

            mHasState = 0;
            mUndoLog.rollback();

            // Exit and release all locks.
            super.scopeExitAll();

            mSavepoint = 0;
            mContext.unregister(mUndoLog);
            mUndoLog = null;
            mTxnId = 0;

            // Delete the rollback marker, now that rollback is complete.
            c.store(null);

            return true;
        } finally {
            c.reset();
        }
    }

    /**
     * Transfers the state of this transaction to a new one, which is then handed off to be
     * recovered later.
     */
    private void preparedHandoff(Throwable cause) {
        var txn = new LocalTransaction(mDatabase, mTxnId, LockMode.UPGRADABLE_READ, 0);
        // No redo log, so this state must be cleared.
        txn.mHasState = mHasState & ~(HAS_COMMIT | HAS_SCOPE);
        txn.mUndoLog = mUndoLog;
        txn.mAttachment = mAttachment;

        transferExclusive(txn);

        mRedo.stashForRecovery(txn);

        mRedo = null;
        mHasState = 0;
        mTxnId = 0;
        mUndoLog = null;
        mBorked = cause;
    }

    @Override
    public final void flush() throws IOException {
        if (mTxnId != 0) {
            mContext.flush();
        }
    }

    @Override
    public final String toString() {
        var b = new StringBuilder(Transaction.class.getName());

        if (this == BOGUS) {
            return b.append('.').append("BOGUS").toString();
        }

        b.append('@').append(Integer.toHexString(hashCode()));

        b.append('{');
        b.append("id").append(": ").append(mTxnId);
        b.append(", ");
        b.append("durabilityMode").append(": ").append(mDurabilityMode);
        b.append(", ");
        b.append("lockMode").append(": ").append(mLockMode);
        b.append(", ");
        b.append("lockTimeout").append(": ");
        TimeUnit unit = Utils.inferUnit(TimeUnit.NANOSECONDS, mLockTimeoutNanos);
        Utils.appendTimeout(b, lockTimeout(unit), unit);

        Object att = mAttachment;
        if (att != null) {
            b.append(", ");
            b.append("attachment").append(": ").append(att);
        }

        try {
            if (isPrepared()) {
                b.append(", ");
                b.append((mHasState & HAS_PREPARE_COMMIT) != 0 ? "preparedCommit" : "prepared");
            }
        } catch (IOException e) {
            // Ignore.
        }

        Object borked = mBorked;
        if (borked != null) {
            b.append(", ");
            b.append("invalid").append(": ").append(borked);
        }

        return b.append('}').toString();
    }

    final LockResult doLockShared(long indexId, byte[] key, int hash) throws LockFailureException {
        return super.doLockShared(indexId, key, hash, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        // Don't replicate shared lock acquisitions. Shared lock replication with a non-strict
        // LockUpgradeRule can cause replica deadlocks when the lock is upgraded due to
        // out-of-order transaction processing. A shared lock just prevents other transactions
        // from making modifications, but replicas cannot make modifications anyhow.
        return lockShared(indexId, key, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        // See comments in lockShared method above.
        return doLockShared(indexId, key, nanosTimeout);
    }

    @Override
    public final LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        // See comments in lockShared method above.
        return doTryLockShared(indexId, key, nanosTimeout);
    }

    @Override
    public final LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        // Don't replicate upgradable lock acquisitions. Because locks are only created by the
        // leader, replication of upgradable locks isn't necessary to prevent deadlocks. If the
        // leader was able acquire upgradable locks and make changes, the replica only needs to
        // acquire upgradable locks to ensure the deadlock-free sequence is applied.
        // Also, if upgradable locks were replicated, then unlocking them should also be
        // replicated to prevent deadlocks due to lingering locks on the replica. Unlocking
        // upgradable locks is a common filtering pattern, so don't add overhead.
        return lockUpgradable(indexId, key, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        // See comments in lockUpgradable method above.
        return doLockUpgradable(indexId, key, nanosTimeout);
    }

    @Override
    public final LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        // See comments in lockUpgradable method above.
        return doTryLockUpgradable(indexId, key, nanosTimeout);
    }

    final LockResult doLockExclusive(long indexId, byte[] key)
        throws LockFailureException
    {
        return doLockExclusive(indexId, key, mLockTimeoutNanos);
    }

    final LockResult doLockExclusive(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        return doLockExclusive(indexId, key, hash, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return lockExclusive(indexId, key, mLockTimeoutNanos);
    }

    @Override
    public final LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return logExclusiveLock(doLockExclusive(indexId, key, nanosTimeout), indexId, key);
    }

    @Override
    public final LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return logExclusiveLock(doTryLockExclusive(indexId, key, nanosTimeout), indexId, key);
    }

    private LockResult logExclusiveLock(LockResult result, long indexId, byte[] key)
        throws LockFailureException
    {
        if (!result.isAcquired()) {
            return result;
        }

        try {
            check();

            final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
            try {
                undoLog().pushLock(UndoLog.OP_LOCK_EXCLUSIVE, indexId, key);
            } catch (Throwable e) {
                borked(e);
            } finally {
                shared.release();
            }

            if (mRedo != null && mDurabilityMode != DurabilityMode.NO_REDO) {
                long txnId = mTxnId;

                if (txnId == 0) {
                    txnId = assignTransactionId();
                }

                int hasState = mHasState;
                if ((hasState & HAS_SCOPE) == 0) {
                    ParentScope parentScope = mParentScope;
                    if (parentScope != null) {
                        setScopeState(parentScope);
                    }
                    mContext.redoEnter(mRedo, txnId);
                    mHasState = hasState | HAS_SCOPE;
                }

                mContext.redoLock(mRedo, OP_TXN_LOCK_EXCLUSIVE, txnId, indexId, key);
            }
        } catch (Throwable e) {
            if (e instanceof UnmodifiableReplicaException || mDatabase.isClosed()) {
                // Keep the lock for now and fail later instead of throwing an odd
                // exception when attempting to acquire a lock. The transaction won't be
                // able to commit anyhow, and by then an exception will be thrown again.
            } else {
                var fail = new LockFailureException(rootCause(e));

                try {
                    if (result == LockResult.UPGRADED) {
                        doUnlockToUpgradable();
                    } else {
                        unlock();
                    }
                } catch (Exception e2) {
                    suppress(fail, e2);
                }

                throw fail;
            }
        }

        return result;
    }

    public final void customRedo(int handlerId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = assignTransactionId();
        }

        int hasState = mHasState;
        if ((hasState & HAS_SCOPE) == 0) {
            ParentScope parentScope = mParentScope;
            if (parentScope != null) {
                setScopeState(parentScope);
            }
            mContext.redoEnter(mRedo, txnId);
        }

        mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);

        if (indexId == 0) {
            if (key != null) {
                throw new IllegalArgumentException("Key cannot be used if indexId is zero");
            }
            mContext.redoCustom(mRedo, txnId, handlerId, message);
        } else {
            LockResult result = lockCheck(indexId, key);
            if (result != LockResult.OWNED_EXCLUSIVE) {
                throw new IllegalStateException("Lock isn't owned exclusively: " + result);
            }
            mContext.redoCustomLock(mRedo, txnId, handlerId, message, indexId, key);
        }
    }

    final void customUndo(int handlerId, byte[] message) throws IOException {
        check();

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            undoLog().pushCustom(handlerId, message);
        } catch (Throwable e) {
            borked(e);
        } finally {
            shared.release();
        }
    }

    /**
     * Called by the PrepareWriter class.
     */
    void prepare(int handlerId, byte[] message, boolean commit) throws IOException {
        check();

        long txnId = mTxnId;

        if (mRedo == null || mDurabilityMode == DurabilityMode.NO_REDO || txnId < 0) {
            // Although this could probably be made to work, it doesn't make much sense.
            throw new IllegalStateException("Cannot prepare a no-redo transaction");
        }

        if (mParentScope != null) {
            // This could probably be made to work too, but recovery would be a mess.
            throw new IllegalStateException("Cannot prepare within a nested scope");
        }

        if (txnId == 0) {
            txnId = assignTransactionId();
        }

        var prepareKey = new byte[8];
        encodeLongBE(prepareKey, 0, txnId);

        // Ensure an UndoLog instance exists or is created.
        UndoLog undo = undoLog();

        BTree preparedTxns = mDatabase.preparedTxns();

        if ((mHasState & HAS_SCOPE) == 0) {
            mContext.redoEnter(mRedo, txnId);
            mHasState |= HAS_SCOPE;
        }

        // Enter a pseudo scope to clean things up if an exception is thrown. The difference
        // being that the scope isn't written to the redo log.
        super.scopeEnter();
        long savepoint = undo.savepoint();

        try {
            // Lock in this transaction to keep it after the consensus transaction commits.
            // Note that this must be a logged operation, because most of the other undo and
            // redo operations against the prepare key won't lock it.
            if (lockExclusive(preparedTxns.mId, prepareKey).isAlreadyOwned()) {
                // The could be made to work, but it requires that the prepare lock be
                // repositioned within the list of owned locks. Otherwise, a call to
                // unlockToPrepare unlocks too much.
                throw new IllegalStateException("Transaction is already prepared");
            }

            // Commit a carrier transaction to safely attain consensus.
            var carrier = new LocalTransaction(this);
            try (var c = new BTreeCursor(preparedTxns)) {
                c.mTxn = LocalTransaction.BOGUS;
                c.autoload(false);
                c.find(prepareKey);

                CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
                try {
                    // The undo operation uninserts the prepare entry, without locking it.
                    carrier.undoLog().pushUnprepare(prepareKey);

                    // The prepare entry isn't actually stored in the prepared transactions
                    // index, except as a ghost.
                    c.storeGhost(new GhostFrame());

                    // Note: Both transactions have the same context and redo.
                    mContext.redoPrepare(mRedo, carrier.id(), txnId, handlerId, message, commit);
                    carrier.mHasState |= HAS_COMMIT;

                    // Following a checkpoint, this operation will store the only copy of the
                    // prepare entry. It also defines the partial rollback location, and so it
                    // must be the last operation pushed to the undo log.
                    undo.pushPrepared(handlerId, message, commit);
                } finally {
                    shared.release();
                }

                carrier.commit();
            } catch (Throwable e) {
                carrier.reset(e);
                throw e;
            }

            mHasState |= HAS_COMMIT | HAS_PREPARE;

            super.promote();

            // Releasing non-exclusive locks provides consistency with the recovery handler,
            // which only recovers exclusive locks. After calling prepare, an application might
            // choose to call the recovery handler immediately.
            super.unlockNonExclusive();
        } catch (Throwable e) {
            try {
                undo.scopeRollback(savepoint);
            } catch (Throwable e2) {
                suppress(e, e2);
            }

            throw e;
        } finally {
            super.scopeExit();
        }

        if (commit) {
            finishPrepareCommit(prepareKey, handlerId, message);
        }
    }

    /**
     * Called by the ReplEngine class. It's a reduced form of the full prepare method above.
     */
    void prepareRedo(int handlerId, byte[] message, boolean commit) throws IOException {
        var prepareKey = new byte[8];
        encodeLongBE(prepareKey, 0, mTxnId);

        UndoLog undo = undoLog();

        BTree preparedTxns = mDatabase.preparedTxns();

        try (var c = new BTreeCursor(preparedTxns)) {
            c.mTxn = LocalTransaction.BOGUS;
            c.autoload(false);
            c.find(prepareKey);

            CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
            try {
                c.storeGhost(new GhostFrame());
                undo.pushPrepared(handlerId, message, commit);
            } finally {
                shared.release();
            }
        }

        mHasState |= HAS_PREPARE;

        if (commit) {
            finishPrepareCommit(prepareKey, handlerId, message);
        }
    }

    private void finishPrepareCommit(byte[] prepareKey, int handlerId, byte[] message)
        throws IOException
    {
        mHasState |= HAS_PREPARE_COMMIT;
        doFinishPrepareCommit(prepareKey, handlerId, message);
    }

    private void doFinishPrepareCommit(int handlerId, byte[] message) throws IOException {
        var prepareKey = new byte[8];
        encodeLongBE(prepareKey, 0, mTxnId);
        doFinishPrepareCommit(prepareKey, handlerId, message);
    }

    private void doFinishPrepareCommit(byte[] prepareKey, int handlerId, byte[] message)
        throws IOException
    {
        // Calling this deletes any ghosts too.
        unlockAllExceptPrepare();

        // Truncate the existing undo log and replace it with a new one that just contains the
        // prepareCommit operation.

        UndoLog oldUndo = mUndoLog;
        var newUndo = new UndoLog(mDatabase, mTxnId);
        newUndo.pushLock(UndoLog.OP_LOCK_EXCLUSIVE, BTree.PREPARED_TXNS_ID, prepareKey);
        newUndo.pushPrepared(handlerId, message, true);

        CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            oldUndo.commit();
            mContext.unregister(oldUndo);
            mContext.register(newUndo);
        } finally {
            shared.release();
        }

        mUndoLog = newUndo;

        oldUndo.truncate();

        int hasState = mHasState;
        if ((hasState & HAS_TRASH) != 0) {
            emptyTrash(hasState);
        }
    }

    private boolean isPrepared() throws IOException {
        BTreeCursor c = checkPrepared();
        if (c == null) {
            return false;
        }
        c.reset();
        return true;
    }

    /**
     * @return null if not prepared; positioned cursor (not loaded) at prepare entry otherwise
     */
    private BTreeCursor checkPrepared() throws IOException {
        if ((mHasState & HAS_PREPARE) == 0) {
            return null;
        }

        BTree preparedTxns = mDatabase.tryPreparedTxns();
        if (preparedTxns != null) {
            var prepareKey = new byte[8];
            encodeLongBE(prepareKey, 0, mTxnId);

            // Double check that the transaction actually owns the prepare lock before
            // potentially performing an odd concurrent operation.
            if (lockCheck(preparedTxns.mId, prepareKey) == LockResult.OWNED_EXCLUSIVE) {
                var c = new BTreeCursor(preparedTxns);
                try {
                    c.mTxn = LocalTransaction.BOGUS;
                    c.autoload(false);
                    c.find(prepareKey);

                    // Check against the frame to account for the entry being ghosted. No need
                    // to latch the node to protect against concurrent deletes because the
                    // prepare lock should be guarding it.
                    if (c.mFrame.mNotFoundKey == null) {
                        if (c.value() == null) {
                            // Key exists, but a null value a indicates ghost.
                            return c;
                        }
                        // Transaction was rolled back, so delete the rollback marker.
                        c.store(null);
                    }

                    c.reset();
                } catch (Throwable e) {
                    c.reset();
                    throw e;
                }
            }
        }

        mHasState &= ~(HAS_PREPARE | HAS_PREPARE_COMMIT);
        return null;
    }

    /**
     * Should only be called for transactions known to be prepared.
     *
     * @return object with handler id and message
     */
    UndoLog.RTP rollbackForRecovery(RedoWriter redo, DurabilityMode durabilityMode,
                                    LockMode lockMode, long timeoutNanos)
        throws IOException
    {
        if ((mHasState & HAS_PREPARE) == 0) {
            throw new AssertionError();
        }

        mRedo = redo;
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
        mLockTimeoutNanos = timeoutNanos;
        mAttachment = null;

        // When recovered from the undo log only, the HAS_COMMIT state won't be set. Set it
        // explicitly now, in order for commit and rollback operations to actually work. Also
        // set the HAS_SCOPE state, which is always set for even the outermost scope when
        // anything is written to the transaction.
        mHasState |= HAS_COMMIT | HAS_SCOPE;

        if (redo == null || durabilityMode == DurabilityMode.NO_REDO) {
            // Oops. HAS_COMMIT also implies that a redo log exists and can be written to.
            mHasState &= ~(HAS_COMMIT | HAS_SCOPE);
        }

        UndoLog.RTP rtp = mUndoLog.rollbackToPrepare();

        unlockToPrepare();

        if (rtp.commit) {
            if ((mHasState & HAS_PREPARE_COMMIT) == 0) {
                throw new AssertionError();
            }
            doFinishPrepareCommit(rtp.handlerId, rtp.message);
        } else if ((mHasState & HAS_PREPARE_COMMIT) != 0) {
            throw new AssertionError();
        }

        return rtp;
    }

    @Override
    public final long id() {
        long txnId = mTxnId;

        if (txnId == 0 && mRedo != null) {
            txnId = assignTransactionId();
        }

        return txnId < 0 ? 0 : txnId;
    }

    /**
     * Recovery cleanup always resets committed transactions, or those with negative
     * identifiers. When the finish parameter is false, unfinished transactions aren't reset.
     *
     * @return true if was reset
     */
    final boolean recoveryCleanup(boolean finish) throws IOException {
        finish |= mTxnId < 0;

        UndoLog undo = mUndoLog;
        if (undo != null) {
            finish |= undo.recoveryCleanup();
        }

        if (finish) {
            if (isPrepared()) {
                // Can't reset; must instead hand off to a recovery handler.
                finish = false;
            } else {
                reset();
            }
        }

        return finish;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoStore(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = doAssignTransactionId();
        }

        try {
            int hasState = mHasState;

            // Set early in case an exception is thrown. Caller is permitted to write redo
            // entry after making any changes, and setting the commit state ensures that
            // undo log is not prematurely truncated when commit is called.
            mHasState = hasState | HAS_COMMIT;

            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                if (value == null) {
                    mContext.redoDelete(mRedo, OP_TXN_ENTER_DELETE, txnId, indexId, key);
                } else {
                    mContext.redoStore(mRedo, OP_TXN_ENTER_STORE, txnId, indexId, key, value);
                }
                mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);
            } else {
                if (value == null) {
                    mContext.redoDelete(mRedo, OP_TXN_DELETE, txnId, indexId, key);
                } else {
                    mContext.redoStore(mRedo, OP_TXN_STORE, txnId, indexId, key, value);
                }
            }
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     */
    final void redoCursorStore(long cursorId, byte[] key, byte[] value) throws IOException {
        check();

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = doAssignTransactionId();
        }

        try {
            int hasState = mHasState;

            // Set early in case an exception is thrown. Caller is permitted to write redo
            // entry after making any changes, and setting the commit state ensures that
            // undo log is not prematurely truncated when commit is called.
            mHasState = hasState | HAS_COMMIT;

            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                mContext.redoEnter(mRedo, txnId);
            }

            if (value == null) {
                mContext.redoCursorDelete(mRedo, cursorId, txnId, key);
            } else {
                mContext.redoCursorStore(mRedo, cursorId, txnId, key, value);
            }

            mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param value pass null for redo delete
     * @return non-zero position if caller should call txnCommitSync after releasing commit lock
     */
    final long redoStoreNoLock(long indexId, byte[] key, byte[] value) throws IOException {
        check();

        if (mRedo != null) try {
            return mContext.redoStoreNoLockAutoCommit(mRedo, indexId, key, value, mDurabilityMode);
        } catch (Throwable e) {
            borked(e);
        }

        return 0;
    }

    /**
     * Transaction id must be assigned.
     */
    private void setScopeState(ParentScope scope) throws IOException {
        int hasState = scope.mHasState;
        if ((hasState & HAS_SCOPE) == 0) {
            ParentScope parentScope = scope.mParentScope;
            if (parentScope != null) {
                setScopeState(parentScope);
            }

            mContext.redoEnter(mRedo, mTxnId);
            scope.mHasState = hasState | HAS_SCOPE;
        }
    }

    /**
     * Caller must hold commit lock if transaction id has not been assigned yet.
     */
    final long txnId() {
        long txnId = mTxnId;
        if (txnId == 0) {
            txnId = mContext.nextTransactionId();
            if (mRedo != null) {
                // Replicas set the high bit to ensure no identifier conflict with the leader.
                txnId = mRedo.adjustTransactionId(txnId);
            }
            mTxnId = txnId;
        }
        return txnId;
    }

    /**
     * Caller must hold commit lock and have verified that current transaction id is 0.
     */
    private long doAssignTransactionId() {
        long txnId = mContext.nextTransactionId();
        // Replicas set the high bit to ensure no identifier conflict with the leader.
        txnId = mRedo.adjustTransactionId(txnId);
        mTxnId = txnId;
        return txnId;
    }

    /**
     * Caller must have verified that current transaction id is 0.
     */
    private long assignTransactionId() {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            return doAssignTransactionId();
        } finally {
            shared.release();
        }
    }

    /**
     * Attempt to generate an identifier for a cursor to perform direct redo operations.
     * Caller must hold commit lock.
     */
    final boolean tryRedoCursorRegister(BTreeCursor cursor) throws IOException {
        if (mRedo == null || (mTxnId <= 0 && mRedo.adjustTransactionId(1) <= 0)) {
            return false;
        } else {
            doRedoCursorRegister(cursor);
            return true;
        }
    }

    private long doRedoCursorRegister(BTreeCursor cursor) throws IOException {
        long cursorId = mContext.nextTransactionId();
        try {
            mContext.redoCursorRegister(mRedo, cursorId, cursor.mTree.mId);
        } catch (Throwable e) {
            borked(e);
        }
        BTree cursorRegistry = mDatabase.cursorRegistry();
        cursor.mCursorId = cursorId;
        mDatabase.registerCursor(cursorRegistry, cursor);
        return cursorId;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op OP_SET_LENGTH, OP_WRITE, or OP_CLEAR
     * @param buf pass EMPTY_BYTES for OP_SET_LENGTH or OP_CLEAR
     */
    final void redoCursorValueModify(BTreeCursor cursor, int op,
                                     long pos, byte[] buf, int off, long len)
        throws IOException
    {
        check();

        if (mRedo == null) {
            return;
        }

        long txnId = mTxnId;

        if (txnId == 0) {
            txnId = doAssignTransactionId();
        }

        try {
            int hasState = mHasState;
            if ((hasState & HAS_SCOPE) == 0) {
                ParentScope parentScope = mParentScope;
                if (parentScope != null) {
                    setScopeState(parentScope);
                }
                mContext.redoEnter(mRedo, txnId);
            }

            mHasState = hasState | (HAS_SCOPE | HAS_COMMIT);

            long cursorId = cursor.mCursorId;

            if (cursorId < 0) {
                // High bit set indicates that a redo op was written which positioned the cursor.
                cursorId &= ~(1L << 63);
            } else {
                if (cursorId == 0) {
                    cursorId = doRedoCursorRegister(cursor);
                }
                mContext.redoCursorFind(mRedo, cursorId, txnId, cursor.mKey);
                cursor.mCursorId = cursorId | (1L << 63);
            }

            if (op == BTreeValue.OP_SET_LENGTH) {
                mContext.redoCursorValueSetLength(mRedo, cursorId, txnId, pos);
            } else if (op == BTreeValue.OP_WRITE) {
                mContext.redoCursorValueWrite(mRedo, cursorId, txnId, pos, buf, off, (int) len);
            } else {
                mContext.redoCursorValueClear(mRedo, cursorId, txnId, pos, len);
            }
        } catch (Throwable e) {
            borked(e);
        }
    }

    @Override
    public final void redoPredicateMode() throws IOException {
        // Note: Not critical that check() be called.

        RedoWriter redo = mRedo;
        if (redo != null) {
            long txnId = mTxnId;
            if (txnId == 0) {
                txnId = assignTransactionId();
            }
            try {
                mContext.redoPredicateMode(redo, txnId);
            } catch (Throwable e) {
                borked(e);
            }
        }
    }

    final void setHasTrash() {
        mHasState |= HAS_TRASH;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op OP_UNUPDATE or OP_UNDELETE
     * @param payloadAddr page with Node-encoded key/value entry
     */
    final void pushUndoStore(long indexId, byte op, long payloadAddr, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushNodeEncoded(indexId, op, payloadAddr, off, len);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUninsert(long indexId, byte[] key) throws IOException {
        check();
        try {
            undoLog().pushUninsert(indexId, key);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     *
     * @param payload Node-encoded key followed by trash id
     */
    final void pushUndeleteFragmented(long indexId, byte[] payload, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushNodeEncoded(indexId, UndoLog.OP_UNDELETE_FRAGMENTED, payload, off, len);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUncreate(long indexId, byte[] key) throws IOException {
        check();
        try {
            undoLog().pushUncreate(indexId, key);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnextend(long indexId, byte[] key, long length) throws IOException {
        check();
        try {
            undoLog().pushUnextend(mSavepoint, indexId, key, length);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnalloc(long indexId, byte[] key, long pos, long length) throws IOException {
        check();
        try {
            undoLog().pushUnalloc(indexId, key, pos, length);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final void pushUnwrite(long indexId, byte[] key, long pos, long addr, int off, int len)
        throws IOException
    {
        check();
        try {
            undoLog().pushUnwrite(indexId, key, pos, addr, off, len);
        } catch (Throwable e) {
            borked(e);
        }
    }

    /**
     * Caller must hold commit lock.
     */
    private UndoLog undoLog() throws IOException {
        UndoLog undo = mUndoLog;
        if (undo == null) {
            undo = new UndoLog(mDatabase, txnId());

            ParentScope parentScope = mParentScope;
            while (parentScope != null) {
                undo.doScopeEnter();
                parentScope = parentScope.mParentScope;
            }

            mContext.register(undo);
            mUndoLog = undo;
        }
        return undo;
    }

    /**
     * Called when an operation against the transaction failed with an exception. This variant
     * never attempts a rollback, and it never rethrows the exception. The "borked" state is
     * set only when the database is closed.
     *
     * <p>By convention, this variant is the typical one to call when a transaction operation
     * fails. By minimizing alteration of the transaction state, an application is permitted to
     * fully rollback later when it calls reset or exit. Otherwise, locks and undo actions
     * might be applied sooner than expected, causing confusing outcomes as the application
     * attempts to perform it's own exception handling and reporting.
     *
     * @param borked non-null exception which might be rethrown, possibly with a cause or
     * suppressed exception tacked on
     */
    final void borked(Throwable borked) {
        borked(borked, false, true); // rollback = false, rethrow = true
    }

    /**
     * Called when an operation against the transaction failed with an exception. Rollback and
     * rethrowing of the exception is attempted only when requested. As a side-effect, the
     * transaction might be assigned a "borked" state, and then the check method always throws
     * an InvalidTransactionException.
     *
     * <p>By convention, rollback is requested when a top-level commit or exit operation
     * failed. More strictly, rollback should only be performed by operations which don't hold
     * tree node latches; otherwise a latch deadlock can occur as the undo rollback attempts to
     * apply compensating actions against the tree nodes.
     *
     * <p>Rethrow should always be requested, except when exit and reset operations fail. If an
     * exit or retry operation failed and the database is closed, rethrowing the exception
     * doesn't provide much utility. The transaction will reset exactly as expected when the
     * database is reopened.
     *
     * @param borked non-null exception which might be rethrown, possibly with a cause or
     * suppressed exception tacked on
     * @param rollback pass true to attempt a rollback, unless the database is closed
     * @param rethrow pass true to always throw an exception; pass null to suppress rethrowing
     * if database is known to be closed; pass false to never throw an exception
     */
    private void borked(Throwable borked, boolean rollback, Boolean rethrow) {
        // Note: The mBorked field is set only if the database is closed or if some action in
        // this method altered the state of the transaction. Leaving the field alone in all
        // other cases permits an application to fully rollback later when reset or exit is
        // called. Any action which releases locks must only do so after it has issued a
        // rollback operation to the undo log.

        boolean closed = mDatabase != null && mDatabase.isClosed();

        if (rethrow == null) {
            rethrow = !closed;
        }

        if (mBorked == null) doBorked: {
            if (closed) {
                Utils.initCause(borked, mDatabase.closedCause());
                mBorked = borked;
            } else if (rollback) {
                // Attempt to rollback the mess and release the locks.
                try {
                    rollbackAll();
                } catch (Throwable rollbackFailed) {
                    if (mBorked != null) {
                        // Rollback already took care of borking the transaction.
                        break doBorked;
                    }

                    if (rethrow && isRecoverable(borked) && isRecoverable(rollbackFailed)) {
                        // Allow application to try again later.
                        Utils.rethrow(borked);
                    }

                    // Rollback failed. Locks cannot be released, ensuring other transactions
                    // cannot see the partial changes made by this transaction. A restart is
                    // required, which then performs a clean rollback.

                    Utils.suppress(borked, rollbackFailed);

                    // Also panic the database if not done so already.
                    panic(borked);

                    // Discard all of the locks, making it impossible for them to be released
                    // even if the application later calls reset.
                    discardAllLocks();
                }

                // Setting this field permits future operations like reset to simply release
                // any newly acquired locks, and not attempt to issue an undo log rollback.
                mBorked = borked;

                // Force application to check again if transaction is borked.
                mUndoLog = null;
            }
        }

        if (rethrow) {
            Utils.rethrow(borked);
        }
    }

    private void panic(Throwable e) {
        try {
            Utils.closeOnFailure(mDatabase, e);
        } catch (Throwable e2) {
            // Ignore.
        }
    }
}
