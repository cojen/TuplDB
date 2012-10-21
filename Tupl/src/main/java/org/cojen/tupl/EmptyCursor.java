/*
 *  Copyright 2012 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class EmptyCursor implements Cursor {
    private Transaction mTxn;
    private boolean mKeyOnly;

    EmptyCursor(Transaction txn) {
        mTxn = txn;
    }

    @Override
    public Transaction link(Transaction txn) {
        Transaction old = mTxn;
        mTxn = txn;
        return old;
    }

    @Override
    public byte[] key() {
        return null;
    }

    @Override
    public byte[] value() {
        return null;
    }

    @Override
    public boolean autoload(boolean mode) {
        boolean old = mKeyOnly;
        mKeyOnly = !mode;
        return !old;
    }

    @Override
    public int compareKeyTo(byte[] rkey) {
        throw new NullPointerException();
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        throw new NullPointerException();
    }

    @Override
    public LockResult first() {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult last() {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult skip(long amount) {
        if (amount != 0) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult next() {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult nextLe(byte[] limitKey) {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult nextLt(byte[] limitKey) {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult previous() {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult previousGe(byte[] limitKey) {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult previousGt(byte[] limitKey) {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public LockResult find(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findGe(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findGt(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findLe(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findLt(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findNearby(byte[] key) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) {
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult load() {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public void store(byte[] value) {
        throw new IllegalStateException("Cursor position is undefined");
    }

    @Override
    public Cursor copy() {
        return new EmptyCursor(mTxn);
    }

    @Override
    public void reset() {
    }
}
