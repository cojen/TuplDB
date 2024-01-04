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

import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.NoReply;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteTransaction extends Remote, Disposable {
    @Batched
    @RemoteFailure(declared=false)
    void lockMode(LockMode mode);

    @RemoteFailure(declared=false)
    LockMode lockMode();

    @Batched
    @RemoteFailure(declared=false)
    void lockTimeout(long timeout, TimeUnit unit);

    @Batched
    @RemoteFailure(declared=false)
    void lockTimeoutNanos(long timeout);

    @RemoteFailure(declared=false)
    long lockTimeout(TimeUnit unit);

    @RemoteFailure(declared=false)
    long lockTimeoutNanos();

    @Batched
    @RemoteFailure(declared=false)
    void durabilityMode(DurabilityMode dm);

    @RemoteFailure(declared=false)
    DurabilityMode durabilityMode();

    @RemoteFailure(exception=DatabaseException.class)
    void check() throws DatabaseException;

    @RemoteFailure(declared=false)
    boolean isBogus();

    void commit() throws IOException;

    void commitAll() throws IOException;

    @Disposer
    default void commitAllAndDispose() throws IOException {
        commitAll();
    }

    @Batched
    void enter() throws IOException;

    void exit() throws IOException;

    void reset() throws IOException;

    @Disposer
    default void resetAndDispose() throws IOException {
        reset();
    }

    void reset(Throwable cause) throws RemoteException;

    @NoReply
    @Disposer
    default void resetAndDispose(Throwable cause) throws RemoteException {
        reset(cause);
    }

    void rollback() throws IOException;

    @RemoteFailure(declared=false)
    LockResult lockShared(long indexId, byte[] key) throws LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException;

    @RemoteFailure(declared=false)
    boolean isNested();

    @RemoteFailure(declared=false)
    int nestingLevel();

    @RemoteFailure(declared=false)
    LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    @RemoteFailure(declared=false)
    LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    @RemoteFailure(declared=false)
    LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException;

    @RemoteFailure(declared=false)
    LockResult lockCheck(long indexId, byte[] key);

    @RemoteFailure(declared=false)
    long lastLockedIndex();

    @RemoteFailure(declared=false)
    byte[] lastLockedKey();

    @RemoteFailure(declared=false)
    boolean wasAcquired(long indexId, byte[] key);

    @RemoteFailure(declared=false)
    void unlock();

    @RemoteFailure(declared=false)
    void unlockToShared();

    @RemoteFailure(declared=false)
    void unlockCombine();

    @RemoteFailure(declared=false)
    long id();

    void flush() throws IOException;
}
