/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.remote;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.InvalidTransactionException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientTransaction implements Transaction {
    final ClientDatabase mDb;
    RemoteTransaction mRemote;
    DurabilityMode mDurabilityMode;
    LockMode mLockMode;
    long mLockTimeoutNanos = Long.MIN_VALUE;
    Throwable mBorked;
    int mScopeCount;

    /**
     * @param dm pass null if default was selected
     */
    ClientTransaction(ClientDatabase db, RemoteTransaction remote, DurabilityMode dm) {
        mDb = db;
        mRemote = remote;
        mDurabilityMode = dm;
    }

    @Override
    public void lockMode(LockMode mode) {
        remote().lockMode(mode);
        mLockMode = mode;
    }

    @Override
    public LockMode lockMode() {
        LockMode mode = mLockMode;
        if (mode == null) {
            mLockMode = mode = remote().lockMode();
        }
        return mode;
    }

    @Override
    public void lockTimeout(long timeout, TimeUnit unit) {
        long timeoutNanos = Utils.toNanos(timeout, unit);
        remote().lockTimeoutNanos(timeoutNanos);
        mLockTimeoutNanos = timeoutNanos;
    }

    @Override
    public long lockTimeout(TimeUnit unit) {
        long timeoutNanos = mLockTimeoutNanos;
        if (timeoutNanos == Long.MIN_VALUE) {
            mLockTimeoutNanos = timeoutNanos = remote().lockTimeoutNanos();
        }
        return timeoutNanos < 0 ? -1 : unit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void durabilityMode(DurabilityMode dm) {
        remote().durabilityMode(dm);
        mDurabilityMode = dm;
    }

    @Override
    public DurabilityMode durabilityMode() {
        DurabilityMode dm = mDurabilityMode;
        if (dm == null) {
            mDurabilityMode = dm = remote().durabilityMode();
        }
        return dm;
    }

    @Override
    public void check() throws DatabaseException {
        RemoteTransaction remote = mRemote;
        if (remote != null) {
            remote.check();
        } else {
            checkBorked();
        }
    }

    private void checkBorked() throws DatabaseException {
        Throwable borked = mBorked;
        if (borked != null) {
            throw new InvalidTransactionException(borked);
        }
    }

    @Override
    public boolean isBogus() {
        // No need to make a remote call.
        return this == mDb.mBogus;
    }

    @Override
    public void commit() throws IOException {
        if (!isBogus()) {
            RemoteTransaction remote = mRemote;
            if (remote != null) {
                remote.commit();
                if (mScopeCount == 0) {
                    remote.dispose();
                    mRemote = null;
                }
            }
        }
    }

    @Override
    public void commitAll() throws IOException {
        if (!isBogus()) {
            RemoteTransaction remote = mRemote;
            if (isBogus()) {
                remote.commitAll();
            } else if (remote != null) {
                remote.commitAllAndDispose();
                mRemote = null;
            }
        }
    }

    @Override
    public void enter() throws IOException {
        if (isBogus()) {
            throw new IllegalStateException("Transaction is bogus");
        } else {
            remote().enter();
            mScopeCount++;
        }
    }

    @Override
    public void exit() throws IOException {
        if (!isBogus()) {
            RemoteTransaction remote = mRemote;
            if (remote != null) {
                remote.exit();
                int count = mScopeCount;
                if (count > 0) {
                    mScopeCount = count - 1;
                } else {
                    remote.dispose();
                    mRemote = null;
                }
            }
        }
    }

    @Override
    public void reset() throws IOException {
        if (!isBogus()) {
            RemoteTransaction remote = mRemote;
            if (remote != null) {
                remote.resetAndDispose();
                mRemote = null;
            }
        }
    }

    @Override
    public void reset(Throwable cause) {
        if (!isBogus()) {
            try {
                RemoteTransaction remote = mRemote;
                if (remote != null) {
                    remote.resetAndDispose(cause);
                    mRemote = null;
                    mBorked = cause;
                }
            } catch (RemoteException e) {
                // Ignore.
            }
        }
    }

    @Override
    public void rollback() throws IOException {
        if (!isBogus()) {
            RemoteTransaction remote = mRemote;
            if (remote != null) {
                remote.rollback();
            }
        }
    }

    @Override
    public LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return remote().lockShared(indexId, key);
    }

    @Override
    public LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return remote().lockUpgradable(indexId, key);
    }

    @Override
    public LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return remote().lockExclusive(indexId, key);
    }

    @Override
    public boolean isNested() {
        return mScopeCount != 0;
    }

    @Override
    public int nestingLevel() {
        return mScopeCount;
    }

    @Override
    public LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return remote().tryLockShared(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return remote().lockShared(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return remote().tryLockUpgradable(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return remote().lockUpgradable(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return remote().tryLockExclusive(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return remote().lockExclusive(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockCheck(long indexId, byte[] key) {
        return remote().lockCheck(indexId, key);
    }

    @Override
    public long lastLockedIndex() {
        RemoteTransaction remote = mRemote;
        return remote == null ? 0 : remote.lastLockedIndex();
    }
    
    @Override
    public byte[] lastLockedKey() {
        RemoteTransaction remote = mRemote;
        return remote == null ? null : remote.lastLockedKey();
    }

    @Override
    public boolean wasAcquired(long indexId, byte[] key) {
        RemoteTransaction remote = mRemote;
        return remote != null && remote.wasAcquired(indexId, key);
    }

    @Override
    public void unlock() {
        remote().unlock();
    }

    @Override
    public void unlockToShared() {
        remote().unlockToShared();
    }

    @Override
    public void unlockCombine() {
        remote().unlockCombine();
    }

    @Override
    public long id() {
        return remote().id();
    }

    @Override
    public void flush() throws IOException {
        RemoteTransaction remote = mRemote;
        if (remote != null) {
            remote.flush();
        }
    }

    /**
     * Ensures that the RemoteTransaction is active, resurrecting it if necessary.
     */
    RemoteTransaction remote() {
        RemoteTransaction remote = mRemote;
        return remote != null ? remote : resurrect();
    }

    /**
     * Ensures that the transaction is active, possibly by resurrecting it and then updating
     * the remote cursor.
     */
    void activeTxn(ClientCursor c) {
        RemoteTransaction remote = mRemote;
        if (remote == null) {
            remote = resurrect();
            c.mRemote.link(remote);
        }
    }

    private RemoteTransaction resurrect() {
        try {
            checkBorked();

            RemoteTransaction remote;
            if (mDurabilityMode == null) {
                remote = mDb.newRemoteTransaction();
            } else {
                remote = mDb.newRemoteTransaction(mDurabilityMode);
            }

            try {
                if (mLockMode != null) {
                    remote().lockMode(mLockMode);
                }
                if (mLockTimeoutNanos != Long.MIN_VALUE) {
                    remote().lockTimeoutNanos(mLockTimeoutNanos);
                }
            } catch (Throwable e) {
                try {
                    remote.dispose();
                } catch (Throwable e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }

            mRemote = remote;
            return remote;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw Utils.rethrow(e);
            }
            throw new IllegalStateException(e);
        }
    }
}
