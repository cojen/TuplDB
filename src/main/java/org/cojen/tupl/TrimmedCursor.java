/*
 *  Copyright 2013-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

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

    /*
    @Override
    public Stream newStream() {
        return new TrimmedStream(mView, mSource.newStream());
    }
    */

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
}
