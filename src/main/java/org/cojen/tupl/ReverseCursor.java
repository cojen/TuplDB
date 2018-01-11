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

import java.util.Comparator;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ReverseCursor implements Cursor {
    private final Cursor mSource;

    ReverseCursor(Cursor source) {
        mSource = source;
    }

    @Override
    public long valueLength() throws IOException {
        return mSource.valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        mSource.valueLength(length);
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
    public void valueClear(long pos, long length) throws IOException {
        mSource.valueClear(pos, length);
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
        return mSource.getOrdering().reverse();
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mSource.getComparator().reversed();
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
    public int compareKeyTo(byte[] rkey) {
        return -mSource.compareKeyTo(rkey);
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return -mSource.compareKeyTo(rkey, offset, length);
    }

    @Override
    public boolean register() throws IOException {
        return mSource.register();
    }

    @Override
    public void unregister() {
        mSource.unregister();
    }

    @Override
    public LockResult first() throws IOException {
        return mSource.last();
    }

    @Override
    public LockResult last() throws IOException {
        return mSource.first();
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        if (amount == Long.MIN_VALUE) {
            LockResult result = mSource.skip(Long.MAX_VALUE);
            if (mSource.key() == null) {
                return result;
            }
            // Should release the lock if just acquired, although it's unlikely
            // that an index will actually have 2^63 entries.
            return next();
        } else {
            return mSource.skip(-amount);
        }
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        if (amount == Long.MIN_VALUE) {
            LockResult result = mSource.skip(Long.MAX_VALUE, limitKey, inclusive);
            if (mSource.key() == null) {
                return result;
            }
            // Should release the lock if just acquired, although it's unlikely
            // that an index will actually have 2^63 entries.
            return next();
        } else {
            return mSource.skip(-amount, limitKey, inclusive);
        }
    }

    @Override
    public LockResult next() throws IOException {
        return mSource.previous();
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return mSource.previousGe(limitKey);
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return mSource.previousGt(limitKey);
    }

    @Override
    public LockResult previous() throws IOException {
        return mSource.next();
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return mSource.nextLe(limitKey);
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return mSource.nextLt(limitKey);
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return mSource.find(key);
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return mSource.findLe(key);
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return mSource.findLt(key);
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return mSource.findGe(key);
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return mSource.findGt(key);
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return mSource.findNearby(key);
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return mSource.findNearbyGe(key);
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return mSource.findNearbyGt(key);
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return mSource.findNearbyLe(key);
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return mSource.findNearbyLt(key);
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.random(ReverseView.appendZero(highKey), ReverseView.appendZero((lowKey)));
    }

    @Override
    public LockResult lock() throws IOException {
        return mSource.lock();
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
    public void commit(byte[] value) throws IOException {
        mSource.commit(value);
    }

    @Override
    public Cursor copy() {
        return new ReverseCursor(mSource.copy());
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
