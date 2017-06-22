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

import java.io.IOException;

import java.util.Comparator;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class UnmodifiableView implements Index {
    static View apply(View view) {
        return view.isUnmodifiable() ? view : new UnmodifiableView(view);
    }

    private final View mSource;

    UnmodifiableView(View source) {
        mSource = source;
    }

    @Override
    public String toString() {
        if (mSource instanceof Index) {
            return ViewUtils.toString(this);
        }
        return super.toString();
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
    public long getId() {
        if (mSource instanceof Index) {
            return ((Index) mSource).getId();
        }
        return 0;
    }

    @Override
    public byte[] getName() {
        if (mSource instanceof Index) {
            return ((Index) mSource).getName();
        }
        return null;
    }

    @Override
    public String getNameString() {
        if (mSource instanceof Index) {
            return ((Index) mSource).getNameString();
        }
        return null;
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new UnmodifiableCursor(mSource.newCursor(txn));
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.count(lowKey, highKey);
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        return mSource.load(txn, key);
    }

    @Override
    public boolean exists(Transaction txn, byte[] key) throws IOException {
        return mSource.exists(txn, key);
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        return mSource.touch(txn, key);
    }

    @Override
    public long evict(Transaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException
    {
        throw new UnmodifiableViewException();
    }

    @Override
    public LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        return mSource.tryLockShared(txn, key, nanosTimeout);
    }

    @Override
    public LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockShared(txn, key);
    }

    @Override
    public LockResult tryLockUpgradable(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        return mSource.tryLockUpgradable(txn, key, nanosTimeout);
    }

    @Override
    public LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockUpgradable(txn, key);
    }

    @Override
    public LockResult tryLockExclusive(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, ViewConstraintException
    {
        return mSource.tryLockExclusive(txn, key, nanosTimeout);
    }

    @Override
    public LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        return mSource.lockExclusive(txn, key);
    }

    @Override
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException {
        return mSource.lockCheck(txn, key);
    }

    /*
    @Override
    public Stream newStream() {
        return new UnmodifiableStream(mSource.newStream());
    }
    */

    @Override
    public View viewGe(byte[] key) {
        return apply(mSource.viewGe(key));
    }

    @Override
    public View viewGt(byte[] key) {
        return apply(mSource.viewGt(key));
    }

    @Override
    public View viewLe(byte[] key) {
        return apply(mSource.viewLe(key));
    }

    @Override
    public View viewLt(byte[] key) {
        return apply(mSource.viewLt(key));
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        return apply(mSource.viewPrefix(prefix, trim));
    }

    @Override
    public View viewTransformed(Transformer transformer) {
        return apply(mSource.viewTransformed(transformer));
    }

    @Override
    public View viewReverse() {
        return apply(mSource.viewReverse());
    }

    @Override
    public View viewUnmodifiable() {
        return this;
    }

    @Override
    public boolean isUnmodifiable() {
        return true;
    }

    @Override
    public Stats analyze(byte[] lowKey, byte[] highKey) throws IOException {
        if (mSource instanceof Index) {
            return ((Index) mSource).analyze(lowKey, highKey);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
        if (mSource instanceof Index) {
            return ((Index) mSource).verify(observer);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void drop() throws IOException {
        throw new UnmodifiableViewException();
    }
}
