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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Base class for cursor unions, cursor intersections, and cursor differences.
 *
 * @author Brian S O'Neill
 */
abstract class MergeCursor extends AbstractValueAccessor implements Cursor {
    // Actual values are important for xor, as used by the select method, to work properly.
    static final int DIRECTION_FORWARD = 0, DIRECTION_REVERSE = -1;

    final MergeView mView;
    final Cursor mFirst;
    final Cursor mSecond;

    Transaction mTxn;
    boolean mKeyOnly;

    int mDirection;
    byte[] mKey;
    byte[] mValue;
    int mCompare;

    MergeCursor(Transaction txn, MergeView view, Cursor first, Cursor second) {
        mView = view;
        mFirst = first;
        mSecond = second;
        mTxn = txn;
    }

    @Override
    public Ordering getOrdering() {
        return mView.mOrdering;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mView.getComparator();
    }

    @Override
    public Transaction link(Transaction txn) {
        Transaction old = mTxn;
        mTxn = txn;
        return old;
    }

    @Override
    public Transaction link() {
        return mTxn;
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
        boolean old = !mKeyOnly;
        mKeyOnly = !mode;
        return old;
    }

    @Override
    public boolean autoload() {
        return !mKeyOnly;
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

    @Override
    public boolean register() throws IOException {
        return mFirst.register() || mSecond.register();
    }

    @Override
    public void unregister() {
        mFirst.unregister();
        mSecond.unregister();
    }

    @FunctionalInterface
    static interface Action {
        LockResult perform(Transaction txn) throws IOException;
    }

    /**
     * Performs the given action with an appropriate lock mode.
     */
    private LockResult perform(Action action) throws IOException {
        Transaction txn = mTxn;
        if (mView.mCombiner.combineLocks()) {
            if (txn == null) {
                txn = mView.newTransaction(null);
                try {
                    txn.lockMode(LockMode.REPEATABLE_READ);
                    action.perform(txn);
                } finally {
                    txn.reset();
                }
                return LockResult.UNOWNED;
            } else if (txn.lockMode() == LockMode.READ_COMMITTED) {
                LockResult result;
                final LockMode original = txn.lockMode();
                try {
                    txn.lockMode(LockMode.REPEATABLE_READ);
                    result = action.perform(txn);
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
        
        return action.perform(txn);
    }

    @Override
    public LockResult first() throws IOException {
        return perform(txn -> {
            mKey = null;
            mValue = null;
            mCompare = 0;
            mDirection = DIRECTION_FORWARD;
            mFirst.first();
            mSecond.first();
            return selectNext(txn);
        });
    }

    @Override
    public LockResult last() throws IOException {
        return perform(txn -> {
            mKey = null;
            mValue = null;
            mCompare = 0;
            mDirection = DIRECTION_REVERSE;
            mFirst.last();
            mSecond.last();
            return selectPrevious(txn);
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
        return perform(txn -> {
            int cmp = mCompare;
            if (cmp == 0) {
                mFirst.next();
                mSecond.next();
                mDirection = DIRECTION_FORWARD;
            } else {
                if (mDirection == DIRECTION_REVERSE) {
                    switchToForward(txn);
                    cmp = mCompare;
                }
                if (cmp < 0) {
                    mFirst.next();
                } else {
                    mSecond.next();
                }
            }
            return selectNext(txn);
        });
    }

    private LockResult selectNext(Transaction txn) throws IOException {
        while (true) {
            LockResult result = select(txn);
            if (result != null) {
                return result;
            }
            boolean fn = mFirst.value() == null;
            boolean sn = mSecond.value() == null;
            if ((fn | !sn) && mFirst.key() != null) {
                mFirst.next();
            }
            if ((sn | !fn) && mSecond.key() != null) {
                mSecond.next();
            }
        }
    }

    @FunctionalInterface
    static interface KeyAction {
        void perform(Cursor c, byte[] key) throws IOException;
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
        return perform(txn -> {
            int cmp = mCompare;
            if (cmp == 0) {
                action.perform(mFirst, limitKey);
                action.perform(mSecond, limitKey);
                mDirection = DIRECTION_FORWARD;
            } else {
                if (mDirection == DIRECTION_REVERSE) {
                    switchToForward(txn);
                    cmp = mCompare;
                }
                action.perform(cmp < 0 ? mFirst : mSecond, limitKey);
            }
            return select(txn, limitKey, action);
        });
    }

    private LockResult select(Transaction txn, byte[] limitKey, KeyAction action)
        throws IOException
    {
        while (true) {
            LockResult result = select(txn);
            if (result != null) {
                return result;
            }
            boolean fn = mFirst.value() == null;
            boolean sn = mSecond.value() == null;
            if (fn | !sn) {
                action.perform(mFirst, limitKey);
            }
            if (sn | !fn) {
                action.perform(mSecond, limitKey);
            }
        }
    }

    private void switchToForward(Transaction txn) throws IOException {
        mDirection = DIRECTION_FORWARD;
        (mKey == mFirst.key() ? mSecond : mFirst).findNearbyGt(mKey);
        select(txn);
    }

    @Override
    public LockResult previous() throws IOException {
        return perform(txn -> {
            int cmp = mCompare;
            if (cmp == 0) {
                mFirst.previous();
                mSecond.previous();
                mDirection = DIRECTION_REVERSE;
            } else {
                if (mDirection == DIRECTION_FORWARD) {
                    switchToReverse(txn);
                    cmp = mCompare;
                }
                if (cmp > 0) {
                    mFirst.previous();
                } else {
                    mSecond.previous();
                }
            }
            return selectPrevious(txn);
        });
    }

    private LockResult selectPrevious(Transaction txn) throws IOException {
        while (true) {
            LockResult result = select(txn);
            if (result != null) {
                return result;
            }
            boolean fn = mFirst.value() == null;
            boolean sn = mSecond.value() == null;
            if ((fn | !sn) && mFirst.key() != null) {
                mFirst.previous();
            }
            if ((sn | !fn) && mSecond.key() != null) {
                mSecond.previous();
            }
        }
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
        return perform(txn -> {
            int cmp = mCompare;
            if (cmp == 0) {
                action.perform(mFirst, limitKey);
                action.perform(mSecond, limitKey);
                mDirection = DIRECTION_REVERSE;
            } else {
                if (mDirection == DIRECTION_FORWARD) {
                    switchToReverse(txn);
                    cmp = mCompare;
                }
                action.perform(cmp > 0 ? mFirst : mSecond, limitKey);
            }
            return select(txn, limitKey, action);
        });
    }

    private void switchToReverse(Transaction txn) throws IOException {
        mDirection = DIRECTION_REVERSE;
        (mKey == mFirst.key() ? mSecond : mFirst).findNearbyLt(mKey);
        select(txn);
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::find);
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::findGe);
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::findGt);
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        return doFind(DIRECTION_REVERSE, key, Cursor::findLe);
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        return doFind(DIRECTION_REVERSE, key, Cursor::findLt);
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::findNearby);
    }

    @Override
    public LockResult findNearbyGe(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::findNearbyGe);
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        return doFind(DIRECTION_FORWARD, key, Cursor::findNearbyGt);
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        return doFind(DIRECTION_REVERSE, key, Cursor::findNearbyLe);
    }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        return doFind(DIRECTION_REVERSE, key, Cursor::findNearbyLt);
    }

    private LockResult doFind(int direction, byte[] key, KeyAction action) throws IOException {
        return perform(txn -> {
            action.perform(mFirst, key);
            action.perform(mSecond, key);
            // Initial direction is just a hint. It can be switched later.
            mDirection = direction;
            LockResult result = select(txn);
            return result == null ? LockResult.UNOWNED : result;
        });
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        // Implementing this is problematic. Common entries must be passed to the combiner. If
        // it returns null, then another random entry must be selected and so on, indefinitely.
        reset();
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult lock() throws IOException {
        return load(false);
    }

    @Override
    public LockResult load() throws IOException {
        return load(true);
    }

    private LockResult load(boolean autoload) throws IOException {
        byte[] key = mKey;
        ViewUtils.positionCheck(key);

        return perform(txn -> {
            alignKeys(key);
            final boolean original = autoload(autoload);
            try {
                LockResult result = select(txn);
                return result == null ? LockResult.UNOWNED : result;
            } finally {
                autoload(original);
            }
        });
    }

    @Override
    public void store(byte[] value) throws IOException {
        byte[] key = mKey;
        ViewUtils.positionCheck(key);

        Transaction txn = mTxn;
        if (txn == null) {
            txn = mView.newTransaction(null);
            try {
                store(txn, key, value);
                txn.commit();
            } finally {
                txn.reset();
            }
        } else if (txn.lockMode() != LockMode.UNSAFE) {
            txn.enter();
            try {
                txn.lockMode(LockMode.UPGRADABLE_READ);
                store(txn, key, value);
                txn.commit();
            } finally {
                txn.exit();
            }
        } else {
            store(txn, key, value);
        }
    }

    private void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        alignKeys(key);

        try {
            mFirst.link(txn);
            mSecond.link(txn);
            doStore(key, value);
        } finally {
            mFirst.link(Transaction.BOGUS);
            mSecond.link(Transaction.BOGUS);
        }

        mValue = value;
    }

    private void alignKeys(byte[] key) throws IOException {
        if (mCompare != 0) {
            if (!Arrays.equals(key, mFirst.key())) {
                mFirst.findNearby(key);
            }
            if (!Arrays.equals(key, mSecond.key())) {
                mSecond.findNearby(key);
            }
            mCompare = 0;
        }
    }

    @Override
    public Cursor copy() {
        MergeCursor copy = newCursor(mFirst.copy(), mSecond.copy());
        copy.mTxn = mTxn;
        copy.mKeyOnly = mKeyOnly;
        copy.mDirection = mDirection;
        copy.mKey = mKey;
        copy.mValue = ViewUtils.copyValue(mValue);
        copy.mCompare = mCompare;
        return copy;
    }

    @Override
    public void reset() {
        mDirection = DIRECTION_FORWARD;
        mKey = null;
        mValue = null;
        mCompare = 0;

        mFirst.reset();
        mSecond.reset();
    }

    @Override
    public void close() {
        reset();
    }

    @Override
    public long valueLength() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void valueLength(long length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    int doValueRead(long pos, byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    void doValueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    void doValueClear(long pos, long length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    int valueStreamBufferSize(int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    void valueCheckOpen() {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by select method when the first value must be selected and the corresponding
     * second value doesn't exist. This cursor's key and value is set as a side effect.
     *
     * @return null if cursor must be moved before select can be called again
     */
    protected LockResult selectFirst(Transaction txn, byte[] key) throws IOException {
        mKey = key;
        mValue = Cursor.NOT_LOADED;

        mFirst.link(txn);
        try {
            LockResult r1 = autoload() ? mFirst.load() : mFirst.lock();
            LockResult r2 = mView.mSecond.touch(txn, key);
            byte[] value = mFirst.value();
            mValue = value;
            if (value == null) {
                if (r1.isAcquired()) {
                    txn.unlock();
                }
                if (r2.isAcquired()) {
                    txn.unlock();
                }
                return null;
            }
            return resultCombine(txn, r1, r2);
        } finally {
            mFirst.link(Transaction.BOGUS);
        }
    }

    /**
     * Called by select method when the second value must be selected and the corresponding
     * first value doesn't exist. This cursor's key and value are set as a side effect.
     *
     * @return null if cursor must be moved before select can be called again
     */
    protected LockResult selectSecond(Transaction txn, byte[] key) throws IOException {
        mKey = key;
        mValue = Cursor.NOT_LOADED;

        mSecond.link(txn);
        try {
            LockResult r1 = mView.mFirst.touch(txn, key);
            LockResult r2 = autoload() ? mSecond.load() : mSecond.lock();
            byte[] value = mSecond.value();
            mValue = value;
            if (value == null) {
                if (r1.isAcquired()) {
                    txn.unlock();
                }
                if (r2.isAcquired()) {
                    txn.unlock();
                }
                return null;
            }
            return resultCombine(txn, r1, r2);
        } finally {
            mSecond.link(Transaction.BOGUS);
        }
    }

    private LockResult lockOrLoad(Combiner combiner, Cursor from) throws IOException {
        return (autoload() || combiner.requireValues()) ? from.load() : from.lock();
    }

    /**
     * Called by select method when keys from both sources match. This cursor's key and value
     * are set as a side effect.
     *
     * @return null if cursor must be moved before select can be called again
     */
    protected LockResult selectCombine(Transaction txn, byte[] key) throws IOException {
        mKey = key;
        mValue = Cursor.NOT_LOADED;

        final Combiner combiner = mView.mCombiner;

        final LockResult r1, r2;
        final byte[] v1, v2;

        mFirst.link(txn);
        try {
            r1 = lockOrLoad(combiner, mFirst);
            v1 = mFirst.value();
        } finally {
            mFirst.link(Transaction.BOGUS);
        }

        mSecond.link(txn);
        try {
            r2 = lockOrLoad(combiner, mSecond);
            v2 = mSecond.value();
        } finally {
            mSecond.link(Transaction.BOGUS);
        }

        doCombine: {
            final byte[] value;
            if (v1 == null) {
                value = v2;
            } else if (v2 == null) {
                value = v1;
                mValue = value;
                break doCombine;
            } else {
                value = mView.mCombiner.combine(key, v1, v2);
            }

            mValue = value;

            if (value == null) {
                if (r1.isAcquired()) {
                    txn.unlock();
                }
                if (r2.isAcquired()) {
                    txn.unlock();
                }
                return null;
            }
        }

        return resultCombine(txn, r1, r2);
    }

    private LockResult resultCombine(Transaction txn, LockResult r1, LockResult r2) {
        if (r1.isAcquired()) {
            if (r1 == r2) {
                txn.unlockCombine();
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
    protected abstract LockResult select(Transaction txn) throws IOException;

    protected abstract void doStore(byte[] key, byte[] value) throws IOException;

    protected ViewConstraintException storeFail() {
        return new ViewConstraintException("Cannot separate value for " + mView.type() + " view");
    }
}
