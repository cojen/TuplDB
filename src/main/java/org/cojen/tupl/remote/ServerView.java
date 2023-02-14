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

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.View;
import org.cojen.tupl.ViewConstraintException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ServerView<V extends View> implements RemoteView {
    final V mView;

    ServerView(V view) {
        mView = view;
    }

    @Override
    public Ordering ordering() {
        return mView.ordering();
    }

    @Override
    public RemoteCursor newCursor(RemoteTransaction txn) {
        return new ServerCursor(mView.newCursor(ServerTransaction.txn(txn)));
    }

    @Override
    public RemoteCursor newAccessor(RemoteTransaction txn, byte[] key) throws IOException {
        return new ServerCursor(mView.newAccessor(ServerTransaction.txn(txn), key));
    }

    @Override
    public RemoteTransaction newTransaction(DurabilityMode dm) {
        return new ServerTransaction(mView.newTransaction(dm));
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mView.isEmpty();
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        return mView.count(lowKey, highKey);
    }

    @Override
    public long count(byte[] lowKey, boolean lowInclusive,
                      byte[] highKey, boolean highInclusive)
        throws IOException
    {
        return mView.count(lowKey, lowInclusive, highKey, highInclusive);
    }

    @Override
    public byte[] load(RemoteTransaction txn, byte[] key) throws IOException {
        return mView.load(ServerTransaction.txn(txn), key);
    }

    @Override
    public boolean exists(RemoteTransaction txn, byte[] key) throws IOException {
        return mView.exists(ServerTransaction.txn(txn), key);
    }

    @Override
    public void store(RemoteTransaction txn, byte[] key, byte[] value) throws IOException {
        mView.store(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public byte[] exchange(RemoteTransaction txn, byte[] key, byte[] value) throws IOException {
        return mView.exchange(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public boolean insert(RemoteTransaction txn, byte[] key, byte[] value) throws IOException {
        return mView.insert(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public boolean replace(RemoteTransaction txn, byte[] key, byte[] value) throws IOException {
        return mView.replace(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public boolean update(RemoteTransaction txn, byte[] key, byte[] value) throws IOException {
        return mView.update(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public boolean update(RemoteTransaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        return mView.update(ServerTransaction.txn(txn), key, oldValue, newValue);
    }

    @Override
    public boolean delete(RemoteTransaction txn, byte[] key) throws IOException {
        return mView.delete(ServerTransaction.txn(txn), key);
    }

    @Override
    public boolean remove(RemoteTransaction txn, byte[] key, byte[] value)
        throws IOException
    {
        return mView.remove(ServerTransaction.txn(txn), key, value);
    }

    @Override
    public LockResult touch(RemoteTransaction txn, byte[] key) throws LockFailureException {
        return mView.touch(ServerTransaction.txn(txn), key);
    }

    @Override
    public LockResult tryLockShared(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mView.tryLockShared(ServerTransaction.txn(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockShared(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mView.lockShared(ServerTransaction.txn(txn), key);
    }

    @Override
    public LockResult tryLockUpgradable(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mView.tryLockUpgradable(ServerTransaction.txn(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockUpgradable(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mView.lockUpgradable(ServerTransaction.txn(txn), key);
    }

    @Override
    public LockResult tryLockExclusive(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mView.tryLockExclusive(ServerTransaction.txn(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockExclusive(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mView.lockExclusive(ServerTransaction.txn(txn), key);
    }

    @Override
    public LockResult lockCheck(RemoteTransaction txn, byte[] key) throws ViewConstraintException {
        return mView.lockCheck(ServerTransaction.txn(txn), key);
    }

    @Override
    public boolean isUnmodifiable() {
        return mView.isUnmodifiable();
    }

    @Override
    public boolean isModifyAtomic() {
        return mView.isModifyAtomic();
    }

    @Override
    public void dispose() {
    }
}
