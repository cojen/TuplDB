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
abstract class SubView implements View {
    final View mSource;

    SubView(View source) {
        mSource = source;
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        return inRange(key) ? mSource.load(txn, key) : null;
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (inRange(key)) {
            mSource.store(txn, key, value);
        } else {
            throw fail();
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (inRange(key)) {
            return mSource.exchange(txn, key, value);
        } else {
            throw fail();
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (inRange(key)) {
            return mSource.insert(txn, key, value);
        }
        if (value == null) {
            return true;
        }
        throw fail();
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        return inRange(key) ? mSource.replace(txn, key, value) : false;
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (inRange(key)) {
            return mSource.update(txn, key, oldValue, newValue);
        }
        if (oldValue == null) {
            if (newValue == null) {
                return true;
            }
            throw fail();
        }
        return false;
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return inRange(key) ? mSource.delete(txn, key) : false;
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return inRange(key) ? mSource.remove(txn, key, value) : (value == null);
    }

    @Override
    public LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.tryLockShared(txn, key, nanosTimeout);
        }
        throw fail();
    }

    @Override
    public final LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.lockShared(txn, key);
        }
        throw fail();
    }

    @Override
    public LockResult tryLockUpgradable(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.tryLockUpgradable(txn, key, nanosTimeout);
        }
        throw fail();
    }

    @Override
    public final LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.lockUpgradable(txn, key);
        }
        throw fail();
    }

    @Override
    public final LockResult tryLockExclusive(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.tryLockExclusive(txn, key, nanosTimeout);
        }
        throw fail();
    }

    @Override
    public final LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        if (inRange(key)) {
            return mSource.lockExclusive(txn, key);
        }
        throw fail();
    }

    @Override
    public final LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException {
        if (inRange(key)) {
            return mSource.lockCheck(txn, key);
        }
        throw fail();
    }

    /*
    @Override
    public Stream newStream() {
        return new SubStream(this, mSource.newStream());
    }
    */

    @Override
    public View viewTransformed(Transformer transformer) {
        return TransformedView.apply(this, transformer);
    }

    @Override
    public View viewReverse() {
        return new ReverseView(this);
    }

    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    @Override
    public boolean isUnmodifiable() {
        return mSource.isUnmodifiable();
    }

    abstract boolean inRange(byte[] key);

    static void prefixCheck(byte[] prefix, int trim) {
        if (prefix == null) {
            throw new NullPointerException("Prefix is null");
        }
        if (trim < 0 | trim > prefix.length) {
            if (trim < 0) {
                throw new IllegalArgumentException("Negative trim");
            }
            throw new IllegalArgumentException("Trim amount is longer than prefix");
        }
    }

    private static ViewConstraintException fail() {
        return new ViewConstraintException("Key is outside allowed range");
    }
}
