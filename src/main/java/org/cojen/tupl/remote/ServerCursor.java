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
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.Utils;

import static org.cojen.tupl.remote.RemoteUtils.*;

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
    public byte ordering() {
        return RemoteUtils.toByte(mCursor.ordering());
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
    public byte first() throws IOException {
        return toByte(mCursor.first());
    }

    @Override
    public byte last() throws IOException {
        return toByte(mCursor.last());
    }

    @Override
    public byte skip(long amount) throws IOException {
        return toByte(mCursor.skip(amount));
    }

    @Override
    public byte skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return toByte(mCursor.skip(amount, limitKey, inclusive));
    }

    @Override
    public byte next() throws IOException {
        return toByte(mCursor.next());
    }

    @Override
    public byte nextLe(byte[] limitKey) throws IOException {
        return toByte(mCursor.nextLe(limitKey));
    }

    @Override
    public byte nextLt(byte[] limitKey) throws IOException {
        return toByte(mCursor.nextLt(limitKey));
    }

    @Override
    public byte previous() throws IOException {
        return toByte(mCursor.previous());
    }

    @Override
    public byte previousGe(byte[] limitKey) throws IOException {
        return toByte(mCursor.previousGe(limitKey));
    }

    @Override
    public byte previousGt(byte[] limitKey) throws IOException {
        return toByte(mCursor.previousGe(limitKey));
    }

    @Override
    public byte find(byte[] key) throws IOException {
        return toByte(mCursor.find(key));
    }

    @Override
    public byte findGe(byte[] key) throws IOException {
        return toByte(mCursor.findGe(key));
    }

    @Override
    public byte findGt(byte[] key) throws IOException {
        return toByte(mCursor.findGt(key));
    }

    @Override
    public byte findLe(byte[] key) throws IOException {
        return toByte(mCursor.findLe(key));
    }

    @Override
    public byte findLt(byte[] key) throws IOException {
        return toByte(mCursor.findLt(key));
    }

    @Override
    public byte findNearby(byte[] key) throws IOException {
        return toByte(mCursor.findNearby(key));
    }

    @Override
    public byte findNearbyGe(byte[] key) throws IOException {
        return toByte(mCursor.findNearbyGe(key));
    }

    @Override
    public byte findNearbyGt(byte[] key) throws IOException {
        return toByte(mCursor.findNearbyGe(key));
    }

    @Override
    public byte findNearbyLe(byte[] key) throws IOException {
        return toByte(mCursor.findNearbyLe(key));
    }

    @Override
    public byte findNearbyLt(byte[] key) throws IOException {
        return toByte(mCursor.findNearbyLt(key));
    }

    @Override
    public byte random(byte[] lowKey, byte[] highKey) throws IOException {
        return toByte(mCursor.random(lowKey, highKey));
    }

    @Override
    public byte random(byte[] lowKey, boolean lowInclusive,
                       byte[] highKey, boolean highInclusive) throws IOException
    {
        return toByte(mCursor.random(lowKey, lowInclusive, highKey, highInclusive));
    }

    @Override
    public boolean exists() throws IOException {
        return mCursor.exists();
    }

    @Override
    public byte lock() throws IOException {
        return toByte(mCursor.lock());
    }

    @Override
    public byte load() throws IOException {
        return toByte(mCursor.load());
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
