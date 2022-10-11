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

import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.ViewConstraintException;
/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteView extends Remote {
    @RemoteFailure(declared=false)
    public byte ordering();

    @Batched
    @RemoteFailure(declared=false)
    public RemoteCursor newCursor(RemoteTransaction txn);

    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException;

    public RemoteCursor newAccessor(RemoteTransaction txn, byte[] key) throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction(byte durabilityMode);

    public boolean isEmpty() throws IOException;

    public long count(byte[] lowKey, byte[] highKey) throws IOException;

    public long count(byte[] lowKey, boolean lowInclusive,
                      byte[] highKey, boolean highInclusive)
        throws IOException;

    public byte[] load(RemoteTransaction txn, byte[] key) throws IOException;

    public boolean exists(RemoteTransaction txn, byte[] key) throws IOException;

    public void store(RemoteTransaction txn, byte[] key, byte[] value) throws IOException;

    public byte[] exchange(RemoteTransaction txn, byte[] key, byte[] value) throws IOException;

    public boolean insert(RemoteTransaction txn, byte[] key, byte[] value) throws IOException;

    public boolean replace(RemoteTransaction txn, byte[] key, byte[] value) throws IOException;

    public boolean update(RemoteTransaction txn, byte[] key, byte[] value) throws IOException;

    public boolean update(RemoteTransaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException;

    public boolean delete(RemoteTransaction txn, byte[] key) throws IOException;

    public boolean remove(RemoteTransaction txn, byte[] key, byte[] value)
        throws IOException;

    @RemoteFailure(declared=false)
    public byte touch(RemoteTransaction txn, byte[] key) throws LockFailureException;

    @RemoteFailure(declared=false)
    public byte tryLockShared(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte lockShared(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte tryLockUpgradable(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte lockUpgradable(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte tryLockExclusive(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte lockExclusive(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    @RemoteFailure(declared=false)
    public byte lockCheck(RemoteTransaction txn, byte[] key) throws ViewConstraintException;

    @RemoteFailure(declared=false)
    public boolean isUnmodifiable();

    @RemoteFailure(declared=false)
    public boolean isModifyAtomic();

    @Disposer
    public void dispose() throws RemoteException;
}
