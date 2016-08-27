/*
 *  Copyright 2012-2015 Cojen.org
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
final class ReverseCursor implements Cursor {
    private final Cursor mSource;

    ReverseCursor(Cursor source) {
        mSource = source;
    }

    @Override
    public Ordering getOrdering() {
        return mSource.getOrdering().reverse();
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

    /*
    @Override
    public Stream newStream() {
        return mSource.newStream();
    }
    */

    @Override
    public Cursor copy() {
        return new ReverseCursor(mSource.copy());
    }

    @Override
    public void reset() {
        mSource.reset();
    }
}
