/*
 *  Copyright (C) 2017 Cojen.org
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
 * Base class for cursor unions, cursor intersections, and cursor differences.
 *
 * @author Brian S O'Neill
 */
abstract class MergeCursor implements Cursor {
    // Actual values are important for xor as used by the select method to work properly.
    static final int DIRECTION_NORMAL = 0, DIRECTION_REVERSE = -1;

    final MergeView mView;
    final Comparator<byte[]> mComparator;
    final Cursor mFirst;
    final Cursor mSecond;

    int mDirection;
    byte[] mKey;
    byte[] mValue;
    int mCompare;

    MergeCursor(MergeView view, Comparator<byte[]> comparator, Cursor first, Cursor second) {
        mView = view;
        mComparator = comparator;
        mFirst = first;
        mSecond = second;
    }

    @Override
    public Ordering getOrdering() {
        return mView.mOrdering;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mComparator;
    }

    @Override
    public Transaction link(Transaction txn) {
        Transaction old = mFirst.link(txn);
        mSecond.link(txn);
        return old;
    }

    @Override
    public Transaction link() {
        return mFirst.link();
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
        return mFirst.autoload(mode) | mSecond.autoload(mode);
    }

    @Override
    public boolean autoload() {
        return mFirst.autoload();
    }

    @Override
    public int compareKeyTo(byte[] rkey) {
        return mKey == mFirst.key() ? mFirst.compareKeyTo(rkey) : mSecond.compareKeyTo(rkey);
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return mKey == mFirst.key() ? mFirst.compareKeyTo(rkey, offset, length)
            : mSecond.compareKeyTo(rkey, offset, length);
    }

    @FunctionalInterface
    static interface Action {
        LockResult perform() throws IOException;
    }

    /**
     * Performs the given action with an appropriate lock mode.
     */
    private LockResult perform(Action action) throws IOException {
        Transaction txn = link();
        if (txn == null) {
            txn = mView.newTransaction(null);
            try {
                link(txn);
                txn.lockMode(LockMode.REPEATABLE_READ);
                action.perform();
            } finally {
                link(null);
                txn.reset();
            }
            return LockResult.UNOWNED;
        } else if (txn.lockMode() == LockMode.READ_COMMITTED) {
            LockResult result;
            final LockMode original = txn.lockMode();
            try {
                txn.lockMode(LockMode.REPEATABLE_READ);
                result = action.perform();
                if (result.isAcquired()) {
                    txn.unlock();
                    result = LockResult.UNOWNED;
                }
            } finally {
                txn.lockMode(original);
            }
            return result;
        } else {
            return action.perform();
        }
    }

    @Override
    public LockResult first() throws IOException {
        return perform(() -> {
            LockResult r1 = mFirst.first();
            LockResult r2;
            try {
                r2 = mSecond.first();
            } catch (LockFailureException e) {
                lockCleanup(e, r1);
                throw e;
            }
            mDirection = DIRECTION_NORMAL;
            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = mFirst.next();
                try {
                    r2 = mSecond.next();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    @Override
    public LockResult last() throws IOException {
        return perform(() -> {
            LockResult r1 = mFirst.last();
            LockResult r2;
            try {
                r2 = mSecond.last();
            } catch (LockFailureException e) {
                lockCleanup(e, r1);
                throw e;
            }
            mDirection = DIRECTION_REVERSE;
            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = mFirst.previous();
                try {
                    r2 = mSecond.previous();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        if (amount == 0) {
            return mFirst.skip(0).commonOwned(mSecond.skip(0));
        }
        return ViewUtils.skipWithLocks(this, amount);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return ViewUtils.skipWithLocks(this, amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        return perform(() -> {
            LockResult r1, r2;
            int cmp = mCompare;
            if (cmp == 0) {
                r1 = mFirst.next();
                try {
                    r2 = mSecond.next();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
                mDirection = DIRECTION_NORMAL;
            } else {
                if (mDirection == DIRECTION_REVERSE) {
                    switchToNormal();
                    cmp = mCompare;
                }
                if (cmp < 0) {
                    r1 = mFirst.next();
                    r2 = LockResult.UNOWNED;
                } else {
                    r1 = LockResult.UNOWNED;
                    r2 = mSecond.next();
                }
            }
            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = mFirst.next();
                try {
                    r2 = mSecond.next();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    @FunctionalInterface
    static interface KeyAction {
        LockResult perform(Cursor c, byte[] key) throws IOException;
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        return nextCmp(limitKey, Cursor::nextLe);
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        return nextCmp(limitKey, Cursor::nextLt);
    }

    private LockResult nextCmp(byte[] limitKey, KeyAction action) throws IOException {
        return perform(() -> {
            LockResult r1, r2;
            int cmp = mCompare;
            if (cmp == 0) {
                r1 = action.perform(mFirst, limitKey);
                try {
                    r2 = action.perform(mSecond, limitKey);
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
                mDirection = DIRECTION_NORMAL;
            } else {
                if (mDirection == DIRECTION_REVERSE) {
                    switchToNormal();
                    cmp = mCompare;
                }
                if (cmp < 0) {
                    r1 = action.perform(mFirst, limitKey);
                    r2 = LockResult.UNOWNED;
                } else {
                    r1 = LockResult.UNOWNED;
                    r2 = action.perform(mSecond, limitKey);
                }
            }
            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = action.perform(mFirst, limitKey);
                try {
                    r2 = action.perform(mSecond, limitKey);
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    private void switchToNormal() throws IOException {
        mDirection = DIRECTION_NORMAL;
        try {
            // FIXME: Define seekGt (et al) methods in ViewUtils. TransformedCursor uses this
            // if the inverse key transform returns null, when calling findNearbyGt (et al).
            (mKey == mFirst.key() ? mSecond : mFirst).findNearbyGt(mKey);
        } catch (LockFailureException e) {
            try {
                select(LockResult.UNOWNED, LockResult.UNOWNED);
            } catch (Throwable e2) {
                Utils.suppress(e, e2);
            }
            throw e;
        }
        select(LockResult.UNOWNED, LockResult.UNOWNED);
    }

    @Override
    public LockResult previous() throws IOException {
        return perform(() -> {
            LockResult r1, r2;
            int cmp = mCompare;
            if (cmp == 0) {
                r1 = mFirst.previous();
                try {
                    r2 = mSecond.previous();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
                mDirection = DIRECTION_REVERSE;
            } else {
                if (mDirection == DIRECTION_NORMAL) {
                    switchToReverse();
                    cmp = mCompare;
                }
                if (cmp > 0) {
                    r1 = mFirst.previous();
                    r2 = LockResult.UNOWNED;
                } else {
                    r1 = LockResult.UNOWNED;
                    r2 = mSecond.previous();
                }
            }

            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = mFirst.previous();
                try {
                    r2 = mSecond.previous();
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        return previousCmp(limitKey, Cursor::previousGe);
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        return previousCmp(limitKey, Cursor::previousGt);
    }

    private LockResult previousCmp(byte[] limitKey, KeyAction action) throws IOException {
        return perform(() -> {
            LockResult r1, r2;
            int cmp = mCompare;
            if (cmp == 0) {
                r1 = action.perform(mFirst, limitKey);
                try {
                    r2 = action.perform(mSecond, limitKey);
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
                mDirection = DIRECTION_REVERSE;
            } else {
                if (mDirection == DIRECTION_NORMAL) {
                    switchToReverse();
                    cmp = mCompare;
                }
                if (cmp > 0) {
                    r1 = action.perform(mFirst, limitKey);
                    r2 = LockResult.UNOWNED;
                } else {
                    r1 = LockResult.UNOWNED;
                    r2 = action.perform(mSecond, limitKey);
                }
            }
            while (true) {
                LockResult result = select(r1, r2);
                if (result != null) {
                    return result;
                }
                r1 = action.perform(mFirst, limitKey);
                try {
                    r2 = action.perform(mSecond, limitKey);
                } catch (LockFailureException e) {
                    lockCleanup(e, r1);
                    throw e;
                }
            }
        });
    }

    private void switchToReverse() throws IOException {
        mDirection = DIRECTION_REVERSE;
        try {
            // FIXME: scan instead of find
            (mKey == mFirst.key() ? mSecond : mFirst).findNearbyLt(mKey);
        } catch (LockFailureException e) {
            try {
                select(LockResult.UNOWNED, LockResult.UNOWNED);
            } catch (Throwable e2) {
                Utils.suppress(e, e2);
            }
            throw e;
        }
        select(LockResult.UNOWNED, LockResult.UNOWNED);
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return doFind(key, Cursor::find);
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return doFind(key, Cursor::findGe);
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return doFind(key, Cursor::findGt);
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return doFind(key, Cursor::findLe);
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return doFind(key, Cursor::findLt);
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return doFind(key, Cursor::findNearby);
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return doFind(key, Cursor::findNearbyGe);
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return doFind(key, Cursor::findNearbyGt);
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return doFind(key, Cursor::findNearbyLe);
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return doFind(key, Cursor::findNearbyLt);
    }

    private LockResult doFind(byte[] key, KeyAction action) throws IOException {
        return perform(() -> {
            LockResult r1 = action.perform(mFirst, key);
            LockResult r2;
            try {
                r2 = action.perform(mSecond, key);
            } catch (LockFailureException e) {
                lockCleanup(e, r1);
                throw e;
            }
            mDirection = DIRECTION_NORMAL;
            LockResult result = select(r1, r2);
            return result == null ? LockResult.UNOWNED : result;
        });
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        // Implementing this is problematic. Common entries must be passed to the combiner. If
        // it returns null, then another random entry must be selected and so on, indefinitely.
        throw new UnsupportedOperationException
            ("Cannot move " + mView.type() + " cursor to a random entry");
    }

    @Override
    public LockResult lock() throws IOException {
        return perform(() -> {
            LockResult r1 = mFirst.lock();
            LockResult r2;
            try {
                r2 = mSecond.lock();
            } catch (LockFailureException e) {
                lockCleanup(e, r1);
                throw e;
            }
            LockResult result = select(r1, r2);
            return result == null ? LockResult.UNOWNED : result;
        });
    }

    @Override
    public LockResult load() throws IOException {
        return perform(() -> {
            LockResult r1 = mFirst.load();
            LockResult r2;
            try {
                r2 = mSecond.load();
            } catch (LockFailureException e) {
                lockCleanup(e, r1);
                throw e;
            }
            LockResult result = select(r1, r2);
            return result == null ? LockResult.UNOWNED : result;
        });
    }

    @Override
    public void store(byte[] value) throws IOException {
        byte[] key = key();
        ViewUtils.positionCheck(key);

        Transaction txn = link();
        if (txn == null) {
            txn = mView.newTransaction(null);
            try {
                link(txn);
                doStore(txn, key, value);
                txn.commit();
            } finally {
                link(null);
                txn.reset();
            }
        } else if (txn.lockMode() != LockMode.UNSAFE) {
            txn.enter();
            try {
                txn.lockMode(LockMode.UPGRADABLE_READ);
                doStore(txn, key, value);
                txn.commit();
            } finally {
                txn.exit();
            }
        } else {
            doStore(txn, key, value);
        }
    }

    @Override
    public Cursor copy() {
        MergeCursor copy = newCursor(mFirst.copy(), mSecond.copy());
        copy.mDirection = mDirection;
        copy.mKey = mKey;
        copy.mValue = ViewUtils.copyValue(mValue);
        copy.mCompare = mCompare;
        return copy;
    }

    @Override
    public void reset() {
        mDirection = DIRECTION_NORMAL;
        mKey = null;
        mValue = null;
        mCompare = 0;

        mFirst.reset();
        mSecond.reset();
    }

    private void lockCleanup(Throwable e, LockResult r1) {
        try {
            if (r1.isAcquired()) {
                mFirst.link().unlock();
            }
            select(LockResult.UNOWNED, LockResult.UNOWNED);
        } catch (Throwable e2) {
            Utils.suppress(e, e2);
        }
    }

    protected LockResult combine(LockResult r1, LockResult r2) {
        if (r1.isAcquired()) {
            if (r1 == r2) {
                mFirst.link().unlockCombine();
            }
            return r1;
        } else if (r2.isAcquired()) {
            return r2;
        } else {
            return r1.commonOwned(r2);
        }
    }

    protected abstract MergeCursor newCursor(Cursor first, Cursor second);

    /**
     * @return null if cursor must be moved before select can be called again
     */
    protected abstract LockResult select(LockResult r1, LockResult r2) throws IOException;

    /**
     * @param txn transaction with UPGRADABLE_READ or UNSAFE mode
     */
    protected abstract void doStore(Transaction txn, byte[] key, byte[] value) throws IOException;

    protected ViewConstraintException storeFail() {
        return new ViewConstraintException("Cannot separate value for " + mView.type() + " view");
    }
}
