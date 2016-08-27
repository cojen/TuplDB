/*
 *  Copyright 2016 Cojen.org
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
 * Cursor implementation used to test the default methods Cursor.
 *
 * @author Brian S O'Neill
 */
class DefaultCursor implements Cursor {
    final Cursor mSource;

    DefaultCursor(Cursor source) {
        mSource = source;
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
}
