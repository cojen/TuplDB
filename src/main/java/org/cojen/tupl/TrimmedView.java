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
final class TrimmedView implements View {
    private final View mSource;
    private final byte[] mPrefix;
    final int mTrim;

    TrimmedView(View source, byte[] prefix, int trim) {
        mSource = source;
        mPrefix = prefix;
        mTrim = trim;
    }

    @Override
    public Ordering getOrdering() {
        return mSource.getOrdering();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new TrimmedCursor(this, mSource.newCursor(txn));
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.count(lowKey, highKey);
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        return mSource.load(txn, applyPrefix(key));
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        mSource.store(txn, applyPrefix(key), value);
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.exchange(txn, applyPrefix(key), value);
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.insert(txn, applyPrefix(key), value);
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.replace(txn, applyPrefix(key), value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        return mSource.update(txn, applyPrefix(key), oldValue, newValue);
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return mSource.delete(txn, applyPrefix(key));
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.remove(txn, applyPrefix(key), value);
    }

    @Override
    public final LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockShared(txn, applyPrefix(key));
    }

    @Override
    public final LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockUpgradable(txn, applyPrefix(key));
    }

    @Override
    public final LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockExclusive(txn, applyPrefix(key));
    }

    @Override
    public final LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException {
        return mSource.lockCheck(txn, applyPrefix(key));
    }

    /*
    @Override
    public Stream newStream() {
        return new TrimmedStream(this, mSource.newStream());
    }
    */

    @Override
    public View viewGe(byte[] key) {
        return new TrimmedView(mSource.viewGe(applyPrefix(key)), mPrefix, mTrim);
    }

    @Override
    public View viewGt(byte[] key) {
        return new TrimmedView(mSource.viewGt(applyPrefix(key)), mPrefix, mTrim);
    }

    @Override
    public View viewLe(byte[] key) {
        return new TrimmedView(mSource.viewLe(applyPrefix(key)), mPrefix, mTrim);
    }

    @Override
    public View viewLt(byte[] key) {
        return new TrimmedView(mSource.viewLt(applyPrefix(key)), mPrefix, mTrim);
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        SubView.prefixCheck(prefix, trim);
        return mSource.viewPrefix(applyPrefix(prefix), mTrim + trim);
    }

    @Override
    public View viewTransformed(Transformer transformer) {
        return TransformedView.apply(this, transformer);
    }

    @Override
    public View viewReverse() {
        return new TrimmedView(mSource.viewReverse(), mPrefix, mTrim);
    }

    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    @Override
    public boolean isUnmodifiable() {
        return mSource.isUnmodifiable();
    }

    byte[] applyPrefix(byte[] key) {
        return applyPrefix(key, 0, key.length);
    }

    byte[] applyPrefix(byte[] key, int offset, int length) {
        Utils.keyCheck(key);
        byte[] prefix = mPrefix;
        byte[] full = new byte[prefix.length + length];
        System.arraycopy(prefix, 0, full, 0, prefix.length);
        System.arraycopy(key, offset, full, prefix.length, length);
        return full;
    }
}
