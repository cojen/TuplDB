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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * Returned by {@link Database#allIndexes}.
 *
 * @author Brian S O'Neill
 */
class IndexesCursor implements Cursor {
    private final Cursor mRegistryCursor;

    private byte[] mKey;

    IndexesCursor(Cursor registryCursor) {
        mRegistryCursor = registryCursor;
    }

    public void link(Transaction txn) {
        mRegistryCursor.link(txn);
    }

    public byte[] key() {
        return mKey;
    }

    public byte[] value() {
        return mRegistryCursor.value();
    }

    public void autoload(boolean mode) {
        mRegistryCursor.autoload(mode);
    }

    public int compareKeyTo(byte[] rkey) {
        byte[] lkey = mKey;
        return Utils.compareKeys(lkey, 0, lkey.length, rkey, 0, rkey.length);
    }

    public int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = mKey;
        return Utils.compareKeys(lkey, 0, lkey.length, rkey, offset, length);
    }

    public LockResult first() throws IOException {
        return assignKey(mRegistryCursor.findGe(firstKey()));
    }

    public LockResult first(long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findGe(firstKey(), maxWait, unit));
    }

    public LockResult last() throws IOException {
        return assignKey(mRegistryCursor.findLe(afterLastKey()));
    }

    public LockResult last(long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findLe(afterLastKey(), maxWait, unit));
    }

    public LockResult move(long amount) throws IOException {
        return assignKey(mRegistryCursor.move(amount));
    }

    public LockResult next() throws IOException {
        return assignKey(mRegistryCursor.next());
    }

    public LockResult next(long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.next(maxWait, unit));
    }

    public LockResult previous() throws IOException {
        return assignKey(mRegistryCursor.previous());
    }

    public LockResult previous(long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.previous(maxWait, unit));
    }

    public LockResult find(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.find(applyKeyPrefix(key)));
    }

    public LockResult findGe(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.findGe(applyKeyPrefix(key)));
    }

    public LockResult findGe(byte[] key, long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findGe(applyKeyPrefix(key), maxWait, unit));
    }

    public LockResult findGt(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.findGt(applyKeyPrefix(key)));
    }

    public LockResult findGt(byte[] key, long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findGt(applyKeyPrefix(key), maxWait, unit));
    }

    public LockResult findLe(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.findLe(applyKeyPrefix(key)));
    }

    public LockResult findLe(byte[] key, long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findLe(applyKeyPrefix(key), maxWait, unit));
    }

    public LockResult findLt(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.findLt(applyKeyPrefix(key)));
    }

    public LockResult findLt(byte[] key, long maxWait, TimeUnit unit) throws IOException {
        return assignKey(mRegistryCursor.findLt(applyKeyPrefix(key), maxWait, unit));
    }

    public LockResult findNearby(byte[] key) throws IOException {
        return assignKey(mRegistryCursor.findNearby(applyKeyPrefix(key)));
    }

    public LockResult load() throws IOException {
        return mRegistryCursor.load();
    }

    public void store(byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    public Cursor copy() {
        IndexesCursor c = new IndexesCursor(mRegistryCursor.copy());
        c.mKey = mKey;
        return c;
    }

    public void reset() {
        mKey = null;
        mRegistryCursor.reset();
    }

    private static byte[] firstKey() {
        return new byte[] {Database.KEY_TYPE_INDEX_NAME};
    }

    private static byte[] afterLastKey() {
        return new byte[] {Database.KEY_TYPE_INDEX_NAME + 1};
    }

    private static byte[] applyKeyPrefix(byte[] key) {
        byte[] full = new byte[1 + key.length];
        full[0] = Database.KEY_TYPE_INDEX_NAME;
        System.arraycopy(key, 0, full, 1, key.length);
        return full;
    }

    private LockResult assignKey(LockResult result) {
        byte[] full = mRegistryCursor.key();
        if (full == null) {
            mKey = null;
        } else if (full[0] == Database.KEY_TYPE_INDEX_NAME) {
            byte[] key = new byte[full.length - 1];
            System.arraycopy(full, 1, key, 0, key.length);
            mKey = key;
        } else {
            // Out of bounds.
            reset();
            return LockResult.UNOWNED;
        }
        return result;
    }
}
