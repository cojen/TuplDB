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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.Utils;

import static org.cojen.tupl.remote.RemoteUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientCursor implements Cursor {
    final ClientView mView;
    RemoteCursor mRemote;

    Transaction mTxn;
    boolean mAutoload;

    ClientCursor(ClientView view, RemoteCursor remote) {
        mView = view;
        mRemote = remote;
    }

    @Override
    public Ordering ordering() {
        return toOrdering(remote().ordering());
    }

    @Override
    public Transaction link(Transaction txn) {
        Transaction old = mTxn;
        if (txn != old) {
            mRemote.link(mView.mDb.remoteTransaction(txn));
            mTxn = txn;
        }
        return old;
    }

    @Override
    public Transaction link() {
        return mTxn;
    }

    @Override
    public byte[] key() {
        return mRemote.key();
    }

    @Override
    public byte[] value() {
        return mRemote.value();
    }

    @Override
    public boolean autoload(boolean mode) {
        boolean old = mAutoload;
        if (mode != old) {
            remote().autoload(mode);
            mAutoload = mode;
        }
        return old;
    }

    @Override
    public boolean autoload() {
        return mAutoload;
    }

    @Override
    public int compareKeyTo(byte[] rkey) {
        return remote().compareKeyTo(rkey);
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return compareKeyTo(Arrays.copyOfRange(rkey, offset, offset + length));
    }

    @Override
    public boolean register() throws IOException {
        return remote().register();
    }

    @Override
    public void unregister() {
        remote().unregister();
    }

    @Override
    public LockResult first() throws IOException {
        return toLockResult(remote().first());
    }

    @Override
    public LockResult last() throws IOException {
        return toLockResult(remote().last());
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        return toLockResult(remote().skip(amount));
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return toLockResult(remote().skip(amount, limitKey, inclusive));
    }

    @Override
    public LockResult next() throws IOException {
        return toLockResult(remote().next());
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return toLockResult(remote().nextLe(limitKey));
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return toLockResult(remote().nextLt(limitKey));
    }

    @Override
    public LockResult previous() throws IOException {
        return toLockResult(remote().previous());
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return toLockResult(remote().previousGe(limitKey));
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return toLockResult(remote().previousGt(limitKey));
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return toLockResult(remote().find(key));
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return toLockResult(remote().findGe(key));
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return toLockResult(remote().findGt(key));
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return toLockResult(remote().findLe(key));
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return toLockResult(remote().findLt(key));
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return toLockResult(remote().findNearby(key));
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return toLockResult(remote().findNearbyGe(key));
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return toLockResult(remote().findNearbyGt(key));
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return toLockResult(remote().findNearbyLe(key));
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return toLockResult(remote().findNearbyLt(key));
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return toLockResult(remote().random(lowKey, highKey));
    }

    @Override
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive) throws IOException
    {
        return toLockResult(remote().random(lowKey, lowInclusive, highKey, highInclusive));
    }

    @Override
    public boolean exists() throws IOException {
        return remote().exists();
    }

    @Override
    public LockResult lock() throws IOException {
        return toLockResult(remote().lock());
    }

    @Override
    public LockResult load() throws IOException {
        return toLockResult(remote().load());
    }

    @Override
    public void store(byte[] value) throws IOException {
        remote().store(value);
    }

    @Override
    public void delete() throws IOException {
        remote().delete();
    }

    @Override
    public void commit(byte[] value) throws IOException {
        remote().commit(value);
    }

    @Override
    public Cursor copy() {
        var copy = new ClientCursor(mView, remote().copy());
        copy.mTxn = mTxn;
        copy.mAutoload = mAutoload;
        return copy;
    }

    @Override
    public void reset() {
        RemoteCursor remote = mRemote;
        if (remote != null) {
            remote.reset();
            mRemote = null;
        }
    }

    @Override
    public long valueLength() throws IOException {
        return remote().valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        remote().valueLength(length);
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        Pipe pipe = remote().valueRead(pos, len, null);
        Object result;
        doRead: try {
            pipe.flush();
            result = pipe.readObject();
            if (result != null) {
                pipe.recycle();
                break doRead;
            }
            int actual = pipe.readInt();
            if (actual > 0) {
                pipe.inputStream().readNBytes(buf, off, actual);
            }
            pipe.recycle();
            return actual;
        } catch (Throwable e) {
            throw Utils.fail(pipe, e);
        }
        throw Utils.rethrow((Throwable) result);
    }

    @Override
    public void valueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        Pipe pipe = remote().valueWrite(pos, len, null);
        Object result;
        try {
            pipe.write(buf, off, len);
            pipe.flush();
            result = pipe.readObject();
            pipe.recycle();
        } catch (Throwable e) {
            throw Utils.fail(pipe, e);
        }
        if (result instanceof Throwable) {
            Utils.rethrow((Throwable) result);
        }
    }

    @Override
    public void valueClear(long pos, long length) throws IOException {
        remote().valueClear(pos, length);
    }

    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        // FIXME: newValueInputStream
        throw null;
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        // FIXME: newValueInputStream
        throw null;
    }

    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        // FIXME: newValueOutputStream
        throw null;
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        // FIXME: newValueOutputStream
        throw null;
    }

    private RemoteCursor remote() {
        RemoteCursor remote = mRemote;
        return remote != null ? remote : resurrect();
    }

    private RemoteCursor resurrect() {
        try {
            RemoteCursor remote = mView.mRemote.newCursor(mView.mDb.remoteTransaction(mTxn));

            if (!mAutoload) {
                try {
                    remote.autoload(false);
                } catch (Throwable e) {
                    try {
                        remote.reset();
                    } catch (Throwable e2) {
                        e.addSuppressed(e2);
                    }
                    throw e;
                }
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
