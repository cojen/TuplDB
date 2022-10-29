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

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Session;
import org.cojen.dirmi.SessionAware;

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
final class ServerCursor implements RemoteCursor, SessionAware {
    static ServerCursor from(Cursor c) {
        return new ServerCursor(c);
    }

    final Cursor mCursor;

    private ServerCursor(Cursor c) {
        mCursor = c;
    }

    @Override
    public void attached(Session<?> session) {
    }

    @Override
    public void detached(Session<?> session) {
        mCursor.reset();
    }

    @Override
    public Ordering ordering() {
        return mCursor.ordering();
    }

    @Override
    public void link(RemoteTransaction txn) {
        mCursor.link(ServerTransaction.txn(txn));
    }

    @Override
    public byte[] key() {
        return mCursor.key();
    }

    @Override
    public byte[] value() {
        return mCursor.value();
    }

    @Override
    public void autoload(boolean mode) {
        mCursor.autoload(mode);
    }

    @Override
    public int compareKeyTo(byte[] rkey) {
        return mCursor.compareKeyTo(rkey);
    }

    @Override
    public boolean register() throws IOException {
        return mCursor.register();
    }

    @Override
    public void unregister() {
        mCursor.unregister();
    }

    @Override
    public LockResult first() throws IOException {
        return mCursor.first();
    }

    @Override
    public LockResult last() throws IOException {
        return mCursor.last();
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        return mCursor.skip(amount);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return mCursor.skip(amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        return mCursor.next();
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return mCursor.nextLe(limitKey);
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return mCursor.nextLt(limitKey);
    }

    @Override
    public LockResult previous() throws IOException {
        return mCursor.previous();
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return mCursor.previousGe(limitKey);
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return mCursor.previousGe(limitKey);
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return mCursor.find(key);
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return mCursor.findGe(key);
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return mCursor.findGt(key);
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return mCursor.findLe(key);
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return mCursor.findLt(key);
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return mCursor.findNearby(key);
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return mCursor.findNearbyGe(key);
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return mCursor.findNearbyGe(key);
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return mCursor.findNearbyLe(key);
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return mCursor.findNearbyLt(key);
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return mCursor.random(lowKey, highKey);
    }

    @Override
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive) throws IOException
    {
        return mCursor.random(lowKey, lowInclusive, highKey, highInclusive);
    }

    @Override
    public boolean exists() throws IOException {
        return mCursor.exists();
    }

    @Override
    public LockResult lock() throws IOException {
        return mCursor.lock();
    }

    @Override
    public LockResult load() throws IOException {
        return mCursor.load();
    }

    @Override
    public void store(byte[] value) throws IOException {
        mCursor.store(value);
    }

    @Override
    public void delete() throws IOException {
        mCursor.delete();
    }

    @Override
    public void commit(byte[] value) throws IOException {
        mCursor.commit(value);
    }

    @Override
    public RemoteCursor copy() {
        return from(mCursor.copy());
    }

    @Override
    public void reset() {
        mCursor.reset();
    }

    @Override
    public long valueLength() throws IOException {
        return mCursor.valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        mCursor.valueLength(length);
    }

    @Override
    public Pipe valueRead(long pos, int len, Pipe pipe) throws IOException {
        try {
            doRead: {
                byte[] buf = new byte[len];
                int actual;
                try {
                    actual = mCursor.valueRead(pos, buf, 0, len);
                } catch (Throwable e) {
                    pipe.writeObject(e);
                    break doRead;
                }
                pipe.writeNull();
                pipe.writeInt(actual);
                if (actual > 0) {
                    pipe.write(buf, 0, actual);
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
    public Pipe valueWrite(long pos, int len, Pipe pipe) throws IOException {
        try {
            doWrite: {
                byte[] buf = new byte[len];
                pipe.readFully(buf);
                try {
                    mCursor.valueWrite(pos, buf, 0, len);
                } catch (Throwable e) {
                    pipe.writeObject(e);
                    break doWrite;
                }
                pipe.writeNull();
            }
            pipe.flush();
            pipe.recycle();
            return null;
        } catch (Throwable e) {
            throw Utils.fail(pipe, e);
        }
    }

    @Override
    public void valueClear(long pos, long length) throws IOException {
        mCursor.valueClear(pos, length);
    }

    @Override
    public Pipe newValueInputStream(long pos, Pipe pipe) throws IOException {
        return newValueInputStream(pos, 4096, pipe);
    }

    @Override
    public Pipe newValueInputStream(long pos, int bufferSize, Pipe pipe) throws IOException {
        // FIXME: need to read/write chunks; otherwise, pipe must be closed and not recycled
        //InputStream in = mCursor.newValueInputStream(pos, 0);
        throw null;
    }

    @Override
    public Pipe newValueOutputStream(long pos, Pipe pipe) throws IOException {
        return newValueOutputStream(pos, 4096, pipe);
    }

    @Override
    public Pipe newValueOutputStream(long pos, int bufferSize, Pipe pipe) throws IOException {
        // FIXME: need to read/write chunks; otherwise, pipe must be closed and not recycled
        throw null;
    }
}
