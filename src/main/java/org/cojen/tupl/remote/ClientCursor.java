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
import java.util.Objects;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class ClientCursor implements Cursor {
    final ClientView mView;
    RemoteCursor mRemote;
    Transaction mTxn;
    boolean mAutoload = true;

    ClientCursor(ClientView view, RemoteCursor remote, Transaction txn) {
        mView = view;
        mRemote = remote;
        mTxn = txn;
    }

    @Override
    public Ordering ordering() {
        return remote().ordering();
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
        Object value = mRemote.value();
        if (value == null) {
            return null;
        } else if (value instanceof byte[] bytes) {
            return bytes;
        } else {
            return NOT_LOADED;
        }
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
        return remoteTxn().register();
    }

    @Override
    public void unregister() {
        remote().unregister();
    }

    @Override
    public LockResult first() throws IOException {
        return remoteTxn().first();
    }

    @Override
    public LockResult last() throws IOException {
        return remoteTxn().last();
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        return remoteTxn().skip(amount);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return remoteTxn().skip(amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        return remoteTxn().next();
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return remoteTxn().nextLe(limitKey);
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return remoteTxn().nextLt(limitKey);
    }

    @Override
    public LockResult previous() throws IOException {
        return remoteTxn().previous();
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return remoteTxn().previousGe(limitKey);
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return remoteTxn().previousGt(limitKey);
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return remoteTxn().find(key);
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return remoteTxn().findGe(key);
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return remoteTxn().findGt(key);
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return remoteTxn().findLe(key);
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return remoteTxn().findLt(key);
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return remoteTxn().findNearby(key);
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return remoteTxn().findNearbyGe(key);
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return remoteTxn().findNearbyGt(key);
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return remoteTxn().findNearbyLe(key);
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return remoteTxn().findNearbyLt(key);
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return remoteTxn().random(lowKey, highKey);
    }

    @Override
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive) throws IOException
    {
        return remoteTxn().random(lowKey, lowInclusive, highKey, highInclusive);
    }

    @Override
    public boolean exists() throws IOException {
        return remoteTxn().exists();
    }

    @Override
    public LockResult lock() throws IOException {
        return remoteTxn().lock();
    }

    @Override
    public LockResult load() throws IOException {
        return remoteTxn().load();
    }

    @Override
    public void store(byte[] value) throws IOException {
        remoteTxn().store(value);
    }

    @Override
    public void delete() throws IOException {
        remoteTxn().delete();
    }

    @Override
    public void commit(byte[] value) throws IOException {
        remote().commit(value);
    }

    @Override
    public Cursor copy() {
        var copy = new ClientCursor(mView, remote().copy(), mTxn);
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
        return remoteValue().valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        remoteValue().valueLength(length);
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        posCheck(pos);
        Objects.checkFromIndexSize(off, len, buf.length);

        Pipe pipe = remoteValue().valueRead(pos, len, null);
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
        posCheck(pos);
        Objects.checkFromIndexSize(off, len, buf.length);

        Pipe pipe = remoteValue().valueWrite(pos, len, null);
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
        posCheck(pos);
        if (length < 0) {
            throw new IllegalArgumentException();
        }
        remoteValue().valueClear(pos, length);
    }

    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        posCheck(pos);
        return new ValueInputStream(this, remoteValue().valueReadTransfer(pos, null));
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        posCheck(pos);
        return new ValueInputStream(this, remoteValue().valueReadTransfer(pos, bufferSize, null));
    }

    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        return newValueOutputStream(pos, -1);
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        posCheck(pos);
        return new ValueOutputStream(this, remoteValue().valueWriteTransfer(pos, null), bufferSize);
    }

    private static void posCheck(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Test method.
     */
    public boolean equalPositions(ClientCursor other) throws IOException {
        return remote().equalPositions(other.remote());
    }

    /**
     * Test method.
     */
    public boolean verifyExtremities(byte extremity) throws IOException {
        return remote().verifyExtremities(extremity);
    }

    /**
     * Ensures that the RemoteCursor is active, resurrecting it if necessary.
     */
    private RemoteCursor remote() {
        RemoteCursor remote = mRemote;
        return remote != null ? remote : resurrect();
    }

    /**
     * Ensures that the RemoteCursor is active and that the transaction is active too.
     */
    private RemoteCursor remoteTxn() {
        RemoteCursor remote = mRemote;
        if (remote == null) {
            remote = resurrect();
        } else {
            Transaction txn = mTxn;
            if (txn instanceof ClientTransaction ct) {
                ct.activeTxn(this);
            }
        }
        return remote;
    }

    /**
     * Throws an exception if RemoteCursor isn't active, but otherwise ensures the transaction
     * is active.
     */
    private RemoteCursor remoteValue() {
        RemoteCursor remote = mRemote;
        if (remote == null) {
            throw new IllegalStateException();
        } else {
            Transaction txn = mTxn;
            if (txn instanceof ClientTransaction ct) {
                ct.activeTxn(this);
            }
        }
        return remote;
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
