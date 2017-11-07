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
final class TrimmedCursor implements Cursor {
    private final TrimmedView mView;
    private final Cursor mSource;
    private final int mTrim;

    private byte[] mKey;

    TrimmedCursor(TrimmedView view, Cursor source) {
        mView = view;
        mSource = source;
        mTrim = view.mTrim;
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
    public Comparator<byte[]> getComparator() {
        return mSource.getComparator();
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
        byte[] key = mKey;
        if (key == null) {
            byte[] full = mSource.key();
            if (full != null) {
                int trim = mTrim;
                int len = full.length - trim;
                key = new byte[len];
                System.arraycopy(full, trim, key, 0, len);
                mKey = key;
            }
        }
        return key;
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
        return mSource.compareKeyTo(mView.applyPrefix(rkey));
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return mSource.compareKeyTo(mView.applyPrefix(rkey, offset, length));
    }

    @Override
    public LockResult first() throws IOException {
        mKey = null;
        return mSource.first();
    }

    @Override
    public LockResult last() throws IOException {
        mKey = null;
        return mSource.last();
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        mKey = null;
        return mSource.skip(amount);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        mKey = null;
        return mSource.skip(amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        mKey = null;
        return mSource.next();
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        mKey = null;
        return mSource.nextLe(mView.applyPrefix(limitKey));
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        mKey = null;
        return mSource.nextLt(mView.applyPrefix(limitKey));
    }

    @Override
    public LockResult previous() throws IOException {
        mKey = null;
        return mSource.previous();
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        mKey = null;
        return mSource.previousGe(mView.applyPrefix(limitKey));
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        mKey = null;
        return mSource.previousGt(mView.applyPrefix(limitKey));
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        mKey = null;
        return mSource.find(mView.applyPrefix(key));
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        mKey = null;
        return mSource.findGe(mView.applyPrefix(key));
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        mKey = null;
        return mSource.findGt(mView.applyPrefix(key));
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        mKey = null;
        return mSource.findLe(mView.applyPrefix(key));
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        mKey = null;
        return mSource.findLt(mView.applyPrefix(key));
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        mKey = null;
        return mSource.findNearby(mView.applyPrefix(key));
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        mKey = null;
        return mSource.findNearbyGe(mView.applyPrefix(key));
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        mKey = null;
        return mSource.findNearbyGt(mView.applyPrefix(key));
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        mKey = null;
        return mSource.findNearbyLe(mView.applyPrefix(key));
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        mKey = null;
        return mSource.findNearbyLt(mView.applyPrefix(key));
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        mKey = null;
        if (lowKey != null) {
            lowKey = mView.applyPrefix(lowKey);
        }
        if (highKey != null) {
            highKey = mView.applyPrefix(highKey);
        }
        return mSource.random(lowKey, highKey);
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
        TrimmedCursor c = new TrimmedCursor(mView, mSource.copy());
        c.mKey = mKey;
        return c;
    }

    @Override
    public void reset() {
        mKey = null;
        mSource.reset();
    }

    @Override
    public void close() {
        reset();
    }
}
