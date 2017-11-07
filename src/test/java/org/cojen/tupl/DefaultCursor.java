/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Cursor implementation used to test the default methods of the Cursor interface.
 *
 * @author Brian S O'Neill
 */
class DefaultCursor implements Cursor {
    final Cursor mSource;

    DefaultCursor(Cursor source) {
        mSource = source;
    }

    @Override
    public long valueLength() throws IOException {
        return mSource.valueLength();
    }

    @Override
    public void setValueLength(long length) throws IOException {
        mSource.setValueLength(length);
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        return mSource.valueRead(pos, buf, off, len);
    }

    @Override
    public void valueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        mSource.valueWrite(pos, buf, off, len);
    }

    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        return mSource.newValueInputStream(pos);
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        return mSource.newValueInputStream(pos, bufferSize);
    }

    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        return mSource.newValueOutputStream(pos);
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        return mSource.newValueOutputStream(pos, bufferSize);
    }

    @Override
    public Ordering getOrdering() {
        return mSource.getOrdering();
    }

    @Override
    public Transaction link(Transaction txn) {
        return mSource.link(txn);
    }

    @Override
    public Transaction link() {
        return mSource.link();
    }

    @Override
    public byte[] key() {
        return mSource.key();
    }

    @Override
    public byte[] value() {
        return mSource.value();
    }

    @Override
    public boolean autoload(boolean mode) {
        return mSource.autoload(mode);
    }

    @Override
    public boolean autoload() {
        return mSource.autoload();
    }

    @Override
    public LockResult first() throws IOException {
        return mSource.first();
    }

    @Override
    public LockResult last() throws IOException {
        return mSource.last();
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        if (amount == 0) {
            return mSource.skip(amount);
        } else {
            return skip(amount, null, false);
        }
    }

    @Override
    public LockResult next() throws IOException {
        return mSource.next();
    }

    @Override
    public LockResult previous() throws IOException {
        return mSource.previous();
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return mSource.find(key);
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.random(lowKey, highKey);
    }

    @Override
    public LockResult load() throws IOException {
        return mSource.load();
    }

    @Override
    public void store(byte[] value) throws IOException {
        mSource.store(value);
    }

    @Override
    public Cursor copy() {
        return new DefaultCursor(mSource.copy());
    }

    @Override
    public void reset() {
        mSource.reset();
    }

    @Override
    public void close() {
        mSource.close();
    }
}
