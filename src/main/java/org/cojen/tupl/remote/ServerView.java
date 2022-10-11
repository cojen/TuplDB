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

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.View;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.core.Utils;

import static org.cojen.tupl.remote.RemoteUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ServerView<V extends View> implements RemoteView {
    static ServerView from(View ix) {
        return new ServerView<View>(ix);
    }

    final V mView;

    protected ServerView(V view) {
        mView = view;
    }

    @Override
    public byte ordering() {
        return RemoteUtils.toByte(mView.ordering());
    }

    @Override
    public RemoteCursor newCursor(RemoteTransaction txn) {
        return ServerCursor.from(mView.newCursor(ServerTransaction.txn(txn)));
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException {
        try {
            doScan: {
                Scanner s;
                try {
                    s = mView.newScanner(ServerTransaction.txn(txn));
                } catch (Throwable e) {
                    pipe.writeObject(e);
                    break doScan;
                }

                try {
                    pipe.write(0); // TODO: comparator type

                    while (true) {
                        byte[] key = s.key();
                        pipe.writeObject(key);
                        if (key == null) {
                            break;
                        }
                        pipe.writeObject(s.value());
                        try {
                            s.step();
                        } catch (Throwable e) {
                            pipe.writeObject(e);
                            break doScan;
                        }
                    }
                } finally {
                    Utils.closeQuietly(s);
                }
            }

            pipe.flush();
            pipe.recycle();

            return null;
        } catch (Throwable e) {
            throw Utils.fail(pipe, e);
        }
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME: newUpdater
        throw null;
    }

    @Override
    public RemoteCursor newAccessor(RemoteTransaction txn, byte[] key) throws IOException {
        return ServerCursor.from(mView.newAccessor(ServerTransaction.txn(txn), key));
    }

    @Override
    public RemoteTransaction newTransaction(byte durabilityMode) {
        return ServerTransaction.from(mView.newTransaction(toDurabilityMode(durabilityMode)));
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
    public byte touch(RemoteTransaction txn, byte[] key) throws LockFailureException {
        return toByte(mView.touch(ServerTransaction.txn(txn), key));
    }

    @Override
    public byte tryLockShared(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return toByte(mView.tryLockShared(ServerTransaction.txn(txn), key, nanosTimeout));
    }

    @Override
    public byte lockShared(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return toByte(mView.lockShared(ServerTransaction.txn(txn), key));
    }

    @Override
    public byte tryLockUpgradable(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return toByte(mView.tryLockUpgradable(ServerTransaction.txn(txn), key, nanosTimeout));
    }

    @Override
    public byte lockUpgradable(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return toByte(mView.lockUpgradable(ServerTransaction.txn(txn), key));
    }

    @Override
    public byte tryLockExclusive(RemoteTransaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return toByte(mView.tryLockExclusive(ServerTransaction.txn(txn), key, nanosTimeout));
    }

    @Override
    public byte lockExclusive(RemoteTransaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return toByte(mView.lockExclusive(ServerTransaction.txn(txn), key));
    }

    @Override
    public byte lockCheck(RemoteTransaction txn, byte[] key) throws ViewConstraintException {
        return toByte(mView.lockCheck(ServerTransaction.txn(txn), key));
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
