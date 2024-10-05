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

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;
import org.cojen.tupl.ViewConstraintException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientView<R extends RemoteView> implements View {
    final ClientDatabase mDb;
    final R mRemote;

    protected volatile boolean mClosed;

    ClientView(ClientDatabase db, R remote) {
        mDb = db;
        mRemote = remote;
    }

    @Override
    public Ordering ordering() {
        return mRemote.ordering();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new ClientCursor(this, mRemote.newCursor(mDb.remoteTransaction(txn)), txn);
    }

    @Override
    public Cursor newAccessor(Transaction txn, byte[] key) throws IOException {
        return new ClientCursor(this, mRemote.newAccessor(mDb.remoteTransaction(txn), key), txn);
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return new ClientTransaction(mDb, mRemote.newTransaction(dm), dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mRemote.isEmpty();
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        return mRemote.count(lowKey, highKey);
    }

    @Override
    public long count(byte[] lowKey, boolean lowInclusive,
                      byte[] highKey, boolean highInclusive)
        throws IOException
    {
        return mRemote.count(lowKey, lowInclusive, highKey, highInclusive);
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        return mRemote.load(mDb.remoteTransaction(txn), key);
    }

    @Override
    public boolean exists(Transaction txn, byte[] key) throws IOException {
        return mRemote.exists(mDb.remoteTransaction(txn), key);
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        mRemote.store(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mRemote.exchange(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mRemote.insert(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mRemote.replace(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mRemote.update(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        return mRemote.update(mDb.remoteTransaction(txn), key, oldValue, newValue);
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return mRemote.delete(mDb.remoteTransaction(txn), key);
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mRemote.remove(mDb.remoteTransaction(txn), key, value);
    }

    @Override
    public LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        return mRemote.touch(mDb.remoteTransaction(txn), key);
    }

    @Override
    public LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mRemote.tryLockShared(mDb.remoteTransaction(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException 
    {
        return mRemote.lockShared(mDb.remoteTransaction(txn), key);
    }

    @Override
    public LockResult tryLockUpgradable(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mRemote.tryLockUpgradable(mDb.remoteTransaction(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mRemote.lockUpgradable(mDb.remoteTransaction(txn), key);
    }

    @Override
    public LockResult tryLockExclusive(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return mRemote.tryLockExclusive(mDb.remoteTransaction(txn), key, nanosTimeout);
    }

    @Override
    public LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException 
    {
        return mRemote.lockExclusive(mDb.remoteTransaction(txn), key);
    }

    @Override
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException {
        return mRemote.lockCheck(mDb.remoteTransaction(txn), key);
    }

    @Override
    public boolean isUnmodifiable() {
        return mRemote.isUnmodifiable();
    }

    @Override
    public boolean isModifyAtomic() {
        return mRemote.isModifyAtomic();
    }

    public boolean isClosed() {
        return mClosed;
    }

    void checkClosed() throws ClosedIndexException {
        if (isClosed()) {
            throw new ClosedIndexException();
        }
    }
}
