/*
 *  Copyright 2015 Brian S O'Neill
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
final class TransformedCursor implements Cursor {
    private final Cursor mSource;
    private final Transformer mTransformer;

    private byte[] mKey;
    private byte[] mValue;

    TransformedCursor(Cursor source, Transformer transformer) {
        mSource = source;
        mTransformer = transformer;
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
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue;
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
    public int compareKeyTo(final byte[] rTKey) {
        final byte[] key = key();
        byte[] rkey = inverseTransformKey(rTKey);
        if (rkey != null) {
            return Utils.compareKeys(key, rkey);
        }

        rkey = mTransformer.inverseTransformKeyLt(rTKey);
        if (rkey != null) {
            int result = Utils.compareKeys(key, rkey);
            if (result == 0) {
                result = -1;
            }
            return result;
        }

        rkey = mTransformer.inverseTransformKeyGt(rTKey);
        if (rkey != null) {
            int result = Utils.compareKeys(key, rkey);
            if (result == 0) {
                result = 1;
            }
            return result;
        }

        throw new NullPointerException("Unsupported key");
    }

    @Override
    public int compareKeyTo(byte[] rTKey, int offset, int length) {
        if (offset == 0 && length == rTKey.length) {
            return compareKeyTo(rTKey);
        }
        byte[] copy = new byte[length];
        System.arraycopy(rTKey, offset, copy, 0, length);
        return compareKeyTo(copy);
    }

    @Override
    public LockResult first() throws IOException {
        LockResult result = transformCurrent(mSource.first());
        return result == null ? next() : result;
    }

    @Override
    public LockResult last() throws IOException {
        LockResult result = transformCurrent(mSource.last());
        return result == null ? previous() : result;
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        if (amount == 0) {
            return mSource.skip(0);
        }

        LockResult result;

        if (amount > 0) {
            do {
                result = next();
            } while (--amount > 0);
        } else {
            do {
                result = previous();
            } while (++amount > 0);
        }

        return result;
    }

    @Override
    public LockResult next() throws IOException {
        final Cursor c = mSource;
        while (true) {
            LockResult result = transformCurrent(c.next());
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult nextLe(final byte[] limitTKey) throws IOException {
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyLt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        while (true) {
            LockResult result = transformCurrent(c.nextLe(limitKey));
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult nextLt(final byte[] limitTKey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyLt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
            while (true) {
                result = transformCurrent(c.nextLe(limitKey));
                if (result != null) {
                    return result;
                }
            }
        } else {
            while (true) {
                result = transformCurrent(c.nextLt(limitKey));
                if (result != null) {
                    return result;
                }
            }
        }
    }

    @Override
    public LockResult previous() throws IOException {
        final Cursor c = mSource;
        while (true) {
            LockResult result = transformCurrent(c.previous());
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult previousGe(final byte[] limitTKey) throws IOException {
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyGt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        while (true) {
            LockResult result = transformCurrent(c.previousGe(limitKey));
            if (result != null) {
                return result;
            }
        }
    }

    @Override
    public LockResult previousGt(final byte[] limitTKey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] limitKey = inverseTransformKey(limitTKey);
        if (limitKey == null) {
            limitKey = mTransformer.inverseTransformKeyGt(limitTKey);
            if (limitKey == null) {
                reset();
                return LockResult.UNOWNED;
            }
            while (true) {
                result = transformCurrent(c.previousGe(limitKey));
                if (result != null) {
                    return result;
                }
            }
        } else {
            while (true) {
                result = transformCurrent(c.previousGt(limitKey));
                if (result != null) {
                    return result;
                }
            }
        }
    }

    @Override
    public LockResult find(final byte[] tkey) throws IOException {
        final byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            reset();
            return LockResult.UNOWNED;
        }
        return transformCurrent(mSource.find(key), key, tkey);
    }

    @Override
    public LockResult findGe(final byte[] tkey) throws IOException {
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyGt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        LockResult result = transformCurrent(c.findGe(key));
        return result == null ? next() : result;
    }

    @Override
    public LockResult findGt(final byte[] tkey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyGt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
            result = c.findGe(key);
        } else {
            result = c.findGt(key);
        }
        result = transformCurrent(result);
        return result == null ? next() : result;
    }

    @Override
    public LockResult findLe(final byte[] tkey) throws IOException {
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyLt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
        }
        final Cursor c = mSource;
        LockResult result = transformCurrent(c.findLe(key));
        return result == null ? previous() : result;
    }

    @Override
    public LockResult findLt(final byte[] tkey) throws IOException {
        final Cursor c = mSource;
        LockResult result;
        byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            key = mTransformer.inverseTransformKeyLt(tkey);
            if (key == null) {
                reset();
                return LockResult.UNOWNED;
            }
            result = c.findLe(key);
        } else {
            result = c.findLt(key);
        }
        result = transformCurrent(result);
        return result == null ? previous() : result;
    }

    @Override
    public LockResult findNearby(final byte[] tkey) throws IOException {
        final byte[] key = inverseTransformKey(tkey);
        if (key == null) {
            reset();
            return LockResult.UNOWNED;
        }
        return transformCurrent(mSource.findNearby(key), key, tkey);
    }

    @Override
    public LockResult random(final byte[] lowTKey, final byte[] highTKey) throws IOException {
        byte[] lowKey = null;
        if (lowTKey != null) {
            lowKey = mTransformer.inverseTransformKey(lowTKey);
            if (lowKey == null) {
                lowKey = mTransformer.inverseTransformKeyGt(lowTKey);
                if (lowKey == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
            }
        }

        byte[] highKey = null;
        if (highTKey != null) {
            highKey = mTransformer.inverseTransformKey(highTKey);
            if (highKey == null) {
                highKey = mTransformer.inverseTransformKeyLt(highTKey);
                if (highKey == null) {
                    reset();
                    return LockResult.UNOWNED;
                }
            }
        }

        LockResult result = transformCurrent(mSource.random(lowKey, highKey));

        if (result == null) {
            if (Utils.random().nextBoolean()) {
                result = next();
                if (mKey == null) {
                    // Reached the end, so wrap around.
                    result = first();
                }
            } else {
                result = previous();
                if (mKey == null) {
                    // Reached the end, so wrap around.
                    result = last();
                }
            }
        }

        return result;
    }

    @Override
    public LockResult load() throws IOException {
        final byte[] tkey = mKey;
        if (tkey == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        final Cursor c = mSource;
        return transformCurrent(c.load(), c.key(), tkey);
    }

    @Override
    public void store(final byte[] tvalue) throws IOException {
        final byte[] tkey = mKey;
        if (tkey == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        final Cursor c = mSource;
        final byte[] key = c.key();
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        c.store(mTransformer.inverseTransformValue(tvalue, key, tkey));
    }

    @Override
    public Stream newStream() {
        Cursor c = mSource;
        if (mKey == null && c.key() != null) {
            c = c.copy();
            c.reset();
        }
        return new TransformedStream(c.newStream(), mTransformer);
    }

    @Override
    public Cursor copy() {
        TransformedCursor copy = new TransformedCursor(mSource.copy(), mTransformer);
        copy.mKey = Utils.cloneArray(mKey);
        copy.mValue = Utils.cloneArray(mValue);
        return copy;
    }

    @Override
    public void reset() {
        mKey = null;
        mValue = null;
        mSource.reset();
    }

    private byte[] inverseTransformKey(final byte[] tkey) {
        if (tkey == null) {
            throw new NullPointerException("Key is null");
        }
        return mTransformer.inverseTransformKey(tkey);
    }

    /**
     * Method returns null if entry was filtered out and cursor must be moved. As a
     * side-effect, the mKey and mValue fields are set to null when filtered out.
     *
     * @param result must not be null
     * @return null if cursor must be moved
     */
    private LockResult transformCurrent(LockResult result) throws IOException {
        final Cursor c = mSource;

        final byte[] key = c.key();
        if (key == null) {
            mKey = null;
            mValue = null;
            return LockResult.UNOWNED;
        }

        byte[] value = c.value();

        if (value == null) {
            byte[] tkey = mTransformer.transformKey(key, value);
            if (tkey != null) {
                // Retain the position and lock when value doesn't exist.
                mKey = tkey;
                mValue = null;
                return result;
            }
        } else if (c.autoload() || !mTransformer.requireValue()) {
            byte[] tkey = mTransformer.transformKey(key, value);
            if (tkey != null) {
                byte[] tvalue = mTransformer.transformValue(value, key, tkey);
                if (tvalue != null) {
                    mKey = tkey;
                    mValue = tvalue;
                    return result;
                }
            }
        } else {
            // Disabling autoload mode makes little sense when using a value transformer,
            // because the value must be loaded anyhow.
            c.load();
            value = c.value();
            byte[] tkey = mTransformer.transformKey(key, value);
            if (tkey != null) {
                byte[] tvalue = mTransformer.transformValue(value, key, tkey);
                if (tvalue != null) {
                    mKey = tkey;
                    // Obey the interface contract.
                    mValue = Cursor.NOT_LOADED;
                    return result;
                }
            }
        }

        // This point is reached when the entry was filtered out and the cursor must move.

        mKey = null;
        mValue = null;

        if (result == LockResult.ACQUIRED) {
            // Release the lock when filtered out, but maintain the cursor position.
            c.link().unlock();
        }

        return null;
    }

    /**
     * As a side-effect, mKey is set to tkey, which must not be null.
     */
    private LockResult transformCurrent(LockResult result, final byte[] key, final byte[] tkey)
        throws IOException
    {
        final Cursor c = mSource;
        final byte[] value = c.value();

        if (value == null) {
            // Retain the position and lock when value doesn't exist.
            mKey = tkey;
            mValue = null;
            return result;
        }

        byte[] tvalue;

        if (c.autoload() || !mTransformer.requireValue()) {
            tvalue = mTransformer.transformValue(value, key, tkey);
        } else {
            // Disabling autoload mode makes little sense when using a value transformer,
            // because the value must be loaded anyhow.
            c.load();
            tvalue = mTransformer.transformValue(c.value(), key, tkey);
            if (tvalue != null) {
                // Obey the interface contract.
                tvalue = Cursor.NOT_LOADED;
            }
        }

        mKey = tkey;
        mValue = tvalue;

        if (tvalue == null && result == LockResult.ACQUIRED) {
            // Release the lock when filtered out, but maintain the cursor position.
            c.link().unlock();
            result = LockResult.UNOWNED;
        }

        return result;
    }
}
