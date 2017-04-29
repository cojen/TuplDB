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

import static org.cojen.tupl.BoundedView.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class BoundedCursor implements Cursor {
    final BoundedView mView;
    final Cursor mSource;

    private boolean mOutOfBounds;

    BoundedCursor(BoundedView view, Cursor source) {
        mView = view;
        mSource = source;
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
    public Transaction link(Transaction txn) {
        return mSource.link(txn);
    }

    @Override
    public Transaction link() {
        return mSource.link();
    }

    @Override
    public byte[] key() {
        return mSource.key();
    }

    @Override
    public byte[] value() {
        return mOutOfBounds ? null : mSource.value();
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
    public int compareKeyTo(byte[] rkey) {
        return mSource.compareKeyTo(rkey);
    }

    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        return mSource.compareKeyTo(rkey, offset, length);
    }

    @Override
    public LockResult first() throws IOException {
        BoundedView view = mView;
        if (view.mEnd == null) {
            return toFirst();
        }

        // If first is after the source end, then no key must be locked. Switch
        // how the source cursor behaves for this to function correctly.

        final Cursor source = mSource;
        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            toFirst();

            byte[] key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.endRangeCompare(key) > 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    private LockResult toFirst() throws IOException {
        LockResult result;

        BoundedView view = mView;
        byte[] start = view.mStart;
        Cursor source = mSource;
        if (start == null) {
            result = source.first();
        } else if ((view.mMode & START_EXCLUSIVE) == 0) {
            result = source.findGe(start);
        } else {
            result = source.findGt(start);
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult last() throws IOException {
        BoundedView view = mView;
        if (view.mStart == null) {
            return toLast();
        }

        // If last is before the source start, then no key must be locked.
        // Switch how the source cursor behaves for this to function correctly.

        final Cursor source = mSource;
        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            toLast();

            byte[] key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.startRangeCompare(key) < 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    private LockResult toLast() throws IOException {
        LockResult result;

        BoundedView view = mView;
        byte[] end = view.mEnd;
        Cursor source = mSource;
        if (end == null) {
            result = mSource.last();
        } else if ((view.mMode & END_EXCLUSIVE) == 0) {
            result = source.findLe(end);
        } else {
            result = source.findLt(end);
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult skip(long amount) throws IOException {
        BoundedView view = mView;
        byte[] limitKey;
        boolean inclusive;
        if (amount >= 0) {
            limitKey = view.mEnd;
            inclusive = (view.mMode & END_EXCLUSIVE) == 0;
        } else {
            limitKey = view.mStart;
            inclusive = (view.mMode & START_EXCLUSIVE) == 0;
        }

        LockResult result = mSource.skip(amount, limitKey, inclusive);

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        LockResult result;

        if (amount == 0 || limitKey == null) {
            result = mSource.skip(0);
        } else {
            BoundedView view = mView;

            if (amount > 0) {
                if (view.endRangeCompare(limitKey) > 0) {
                    limitKey = view.mEnd;
                    inclusive = (view.mMode & END_EXCLUSIVE) == 0;
                }
            } else {
                if (view.startRangeCompare(limitKey) < 0) {
                    limitKey = view.mStart;
                    inclusive = (view.mMode & START_EXCLUSIVE) == 0;
                }
            }

            result = mSource.skip(amount, limitKey, inclusive);
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult next() throws IOException {
        LockResult result;

        BoundedView view = mView;
        byte[] end = view.mEnd;
        Cursor source = mSource;
        if (end == null) {
            result = source.next();
        } else if ((view.mMode & END_EXCLUSIVE) == 0) {
            result = source.nextLe(end);
        } else {
            result = source.nextLt(end);
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        if (mView.endRangeCompare(limitKey) <= 0) {
            LockResult result = mSource.nextLe(limitKey);
            mOutOfBounds = false;
            return result;
        } else {
            return next();
        }
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        if (mView.endRangeCompare(limitKey) <= 0) {
            LockResult result = mSource.nextLt(limitKey);
            mOutOfBounds = false;
            return result;
        } else {
            return next();
        }
    }

    @Override
    public LockResult previous() throws IOException {
        LockResult result;

        BoundedView view = mView;
        byte[] start = view.mStart;
        Cursor source = mSource;
        if (start == null) {
            result = source.previous();
        } else if ((view.mMode & START_EXCLUSIVE) == 0) {
            result = source.previousGe(start);
        } else {
            result = source.previousGt(start);
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        if (mView.startRangeCompare(limitKey) >= 0) {
            LockResult result = mSource.previousGe(limitKey);
            mOutOfBounds = false;
            return result;
        } else {
            return previous();
        }
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        if (mView.startRangeCompare(limitKey) >= 0) {
            LockResult result = mSource.previousGt(limitKey);
            mOutOfBounds = false;
            return result;
        } else {
            return previous();
        }
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        if (mView.inRange(key)) {
            LockResult result = mSource.find(key);
            mOutOfBounds = false;
            return result;
        } else {
            mOutOfBounds = true;
            ViewUtils.findNoLock(mSource, key);
            return LockResult.UNOWNED;
        }
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        final Cursor source = mSource;
        if (view.mEnd == null) {
            LockResult result = source.findGe(key);
            mOutOfBounds = false;
            return result;
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findGe(key);
            mOutOfBounds = false;

            key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.endRangeCompare(key) > 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        final Cursor source = mSource;
        if (view.mEnd == null) {
            LockResult result = source.findGt(key);
            mOutOfBounds = false;
            return result;
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findGt(key);
            mOutOfBounds = false;

            key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.endRangeCompare(key) > 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        final Cursor source = mSource;
        if (view.mStart == null) {
            LockResult result = source.findLe(key);
            mOutOfBounds = false;
            return result;
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findLe(key);
            mOutOfBounds = false;

            key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.startRangeCompare(key) < 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        final Cursor source = mSource;
        if (view.mStart == null) {
            LockResult result = source.findLt(key);
            mOutOfBounds = false;
            return result;
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findLt(key);
            mOutOfBounds = false;

            key = source.key();
            if (key == null) {
                return LockResult.UNOWNED;
            }
            if (view.startRangeCompare(key) < 0) {
                source.reset();
                return LockResult.UNOWNED;
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return source.load();
    }

    @Override
    public LockResult findNearby(byte[] key) throws IOException {
        if (mView.inRange(key)) {
            LockResult result = mSource.findNearby(key);
            mOutOfBounds = false;
            return result;
        } else {
            mOutOfBounds = true;
            ViewUtils.findNearbyNoLock(mSource, key);
            return LockResult.UNOWNED;
        }
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        LockResult result = mSource.random
            (mView.adjustLowKey(lowKey),mView.adjustHighKey(highKey));
        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult lock() throws IOException {
        if (mOutOfBounds) {
            throw fail();
        } else {
            return mSource.lock();
        }
    }

    @Override
    public LockResult load() throws IOException {
        if (mOutOfBounds) {
            throw fail();
        } else {
            return mSource.load();
        }
    }

    @Override
    public void store(byte[] value) throws IOException {
        if (mOutOfBounds) {
            throw fail();
        } else {
            mSource.store(value);
        }
    }

    @Override
    public void commit(byte[] value) throws IOException {
        if (mOutOfBounds) {
            throw fail();
        } else {
            mSource.commit(value);
        }
    }

    /*
    @Override
    public Stream newStream() {
        return new SubStream(mView, mSource.newStream());
    }
    */

    @Override
    public Cursor copy() {
        BoundedCursor copy = new BoundedCursor(mView, mSource.copy());
        copy.mOutOfBounds = mOutOfBounds;
        return copy;
    }

    @Override
    public void reset() {
        mSource.reset();
        mOutOfBounds = false;
    }
}
