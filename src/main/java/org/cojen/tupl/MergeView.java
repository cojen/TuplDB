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
 * Base class for view unions, view intersections, and view differences.
 *
 * @author Brian S O'Neill
 */
abstract class MergeView implements View {
    final Combiner mCombiner;
    final Ordering mOrdering;
    final Comparator<byte[]> mComparator;
    final View mFirst;
    final View mSecond;

    MergeView(Combiner combiner, View first, View second) {
        // Determining the ordering also validates that no inputs are null.
        Ordering ordering = first.getOrdering();
        if (second.getOrdering() != ordering) {
            ordering = Ordering.UNSPECIFIED;
        }

        Comparator<byte[]> comparator = first.getComparator();
        if (comparator == null || !comparator.equals(second.getComparator())) {
            throw new IllegalArgumentException
                ("Consistent comparator ordering is required for " + type() + " view");
        }

        mCombiner = combiner;
        mOrdering = ordering;
        mComparator = comparator;
        mFirst = first;
        mSecond = second;
    }

    @Override
    public Ordering getOrdering() {
        return mOrdering;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mComparator;
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        Cursor first = mFirst.newCursor(Transaction.BOGUS);
        first.autoload(false);
        Cursor second = mSecond.newCursor(Transaction.BOGUS);
        second.autoload(false);
        return newCursor(txn, this, first, second);
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mFirst.newTransaction(durabilityMode);
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        if (mCombiner.combineLocks()) combine: {
            if (txn == null) {
                txn = newTransaction(null);
                txn.lockMode(LockMode.REPEATABLE_READ);
            } else if (txn.lockMode() == LockMode.READ_COMMITTED) {
                txn.enter();
            } else {
                break combine;
            }
            try {
                return doLoad(txn, key);
            } finally {
                txn.exit();
            }
        }

        return doLoad(txn, key);
    }

    protected abstract byte[] doLoad(Transaction txn, byte[] key) throws IOException;

    @Override
    public LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        if (mCombiner.combineLocks()) {
            if (txn == null) {
                txn = newTransaction(null);
                try {
                    txn.lockMode(LockMode.REPEATABLE_READ);
                    doTouch(txn, key);
                } finally {
                    try {
                        txn.reset();
                    } catch (IOException e) {
                        // Not expected.
                    }
                }
                return LockResult.UNOWNED;
            } else if (txn.lockMode() == LockMode.READ_COMMITTED) {
                LockResult result;
                final LockMode original = txn.lockMode();
                try {
                    txn.lockMode(LockMode.REPEATABLE_READ);
                    result = doTouch(txn, key);
                    if (result.isAcquired()) {
                        txn.unlock();
                        result = LockResult.UNOWNED;
                    }
                } finally {
                    txn.lockMode(original);
                }
                return result;
            }
        }

        return doTouch(txn, key);
    }

    private LockResult doTouch(Transaction txn, byte[] key) throws LockFailureException {
        LockResult r1 = mFirst.touch(txn, key);
        LockResult r2;
        try {
            r2 = mSecond.touch(txn, key);
        } catch (Throwable e) {
            throw ViewUtils.lockCleanup(e, txn, r1);
        }
        return lockCombine(txn, r1, r2);
    }

    @Override
    public LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        LockResult r1 = mFirst.lockShared(txn, key);
        LockResult r2;
        try {
            r2 = mSecond.lockShared(txn, key);
        } catch (Throwable e) {
            throw ViewUtils.lockCleanup(e, txn, r1);
        }
        return lockCombine(txn, r1, r2);
    }

    @Override
    public LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        LockResult r1 = mFirst.lockUpgradable(txn, key);
        LockResult r2;
        try {
            r2 = mSecond.lockUpgradable(txn, key);
        } catch (Throwable e) {
            throw ViewUtils.lockCleanup(e, txn, r1);
        }
        return lockCombine(txn, r1, r2);
    }

    @Override
    public LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException
    {
        LockResult r1 = mFirst.lockExclusive(txn, key);
        LockResult r2;
        try {
            r2 = mSecond.lockExclusive(txn, key);
        } catch (Throwable e) {
            throw ViewUtils.lockCleanup(e, txn, r1);
        }
        return lockCombine(txn, r1, r2);
    }

    @Override
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException {
        LockResult r1 = mFirst.lockCheck(txn, key);
        if (r1 == LockResult.UNOWNED) {
            return r1;
        }
        LockResult r2 = mSecond.lockCheck(txn, key);
        return r2 == LockResult.UNOWNED ? r2 : r1.commonOwned(r2);
    }

    @Override
    public boolean isUnmodifiable() {
        return mFirst.isUnmodifiable() && mSecond.isUnmodifiable();
    }

    private static LockResult lockCombine(Transaction txn, LockResult r1, LockResult r2) {
        if (r2.isAcquired()) {
            if (r1 == r2) {
                txn.unlockCombine();
            }
            return r2;
        } else {
            return r1.isAcquired() ? r1 : r1.commonOwned(r2);
        }
    }

    protected abstract MergeCursor newCursor(Transaction txn, MergeView view,
                                             Cursor first, Cursor second);

    protected abstract String type();
}
