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

package org.cojen.tupl.views;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Transformer;
import org.cojen.tupl.View;
import org.cojen.tupl.ViewConstraintException;

/**
 * View implementation vended by {@link View#viewReverse}.
 *
 * @author Brian S O'Neill
 */
public final class ReverseView implements View {
    private final View mSource;

    public ReverseView(View source) {
        mSource = source;
    }

    @Override
    public Ordering ordering() {
        return mSource.ordering().reverse();
    }

    @Override
    public Comparator<byte[]> comparator() {
        return mSource.comparator().reversed();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new ReverseCursor(mSource.newCursor(txn));
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public long count(byte[] lowKey, boolean lowInclusive,
                      byte[] highKey, boolean highInclusive)
        throws IOException
    {
        return mSource.count(highKey, highInclusive, lowKey, lowInclusive);
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
        mSource.store(txn, key, value);
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.exchange(txn, key, value);
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.insert(txn, key, value);
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.replace(txn, key, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.update(txn, key, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        return mSource.update(txn, key, oldValue, newValue);
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return mSource.delete(txn, key);
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return mSource.remove(txn, key, value);
    }

    @Override
    public LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        return mSource.touch(txn, key);
    }

    @Override
    public LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws LockFailureException, ViewConstraintException
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
        throws LockFailureException, ViewConstraintException
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
        throws LockFailureException, ViewConstraintException
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

    @Override
    public View viewGe(byte[] key) {
        return new ReverseView(mSource.viewLe(key));
    }

    @Override
    public View viewGt(byte[] key) {
        return new ReverseView(mSource.viewLt(key));
    }

    @Override
    public View viewLe(byte[] key) {
        return new ReverseView(mSource.viewGe(key));
    }

    @Override
    public View viewLt(byte[] key) {
        return new ReverseView(mSource.viewGt(key));
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        return new ReverseView(mSource.viewPrefix(prefix, trim));
    }

    @Override
    public View viewTransformed(Transformer transformer) {
        return new ReverseView(mSource.viewTransformed(transformer));
    }

    @Override
    public View viewKeys() {
        View sourceKeys = mSource.viewKeys();
        return sourceKeys == mSource ? this : new ReverseView(sourceKeys);
    }

    @Override
    public View viewReverse() {
        return mSource;
    }

    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    @Override
    public boolean isUnmodifiable() {
        return mSource.isUnmodifiable();
    }

    @Override
    public boolean isModifyAtomic() {
        return mSource.isModifyAtomic();
    }
}
