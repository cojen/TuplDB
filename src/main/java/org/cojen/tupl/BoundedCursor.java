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

import static org.cojen.tupl.BoundedView.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class BoundedCursor implements Cursor {
    private final BoundedView mView;
    private final Cursor mSource;

    BoundedCursor(BoundedView view, Cursor source) {
        mView = view;
        mSource = source;
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
        return mSource.key();
    }

    @Override
    public byte[] value() {
        return mSource.value();
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
        BoundedView view = mView;
        byte[] start = view.mStart;
        Cursor source = mSource;
        if (start == null) {
            return source.first();
        } else if ((view.mMode & START_EXCLUSIVE) == 0) {
            return source.findGe(start);
        } else {
            return source.findGt(start);
        }
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
        BoundedView view = mView;
        byte[] end = view.mEnd;
        Cursor source = mSource;
        if (end == null) {
            return mSource.last();
        } else if ((view.mMode & END_EXCLUSIVE) == 0) {
            return source.findLe(end);
        } else {
            return source.findLt(end);
        }
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
        return mSource.skip(amount, limitKey, inclusive);
    }

    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        if (amount == 0 || limitKey == null) {
            return mSource.skip(0);
        }

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

        return mSource.skip(amount, limitKey, inclusive);
    }

    @Override
    public LockResult next() throws IOException {
        BoundedView view = mView;
        byte[] end = view.mEnd;
        Cursor source = mSource;
        if (end == null) {
            return source.next();
        } else if ((view.mMode & END_EXCLUSIVE) == 0) {
            return source.nextLe(end);
        } else {
            return source.nextLt(end);
        }
    }

    @Override
    public LockResult nextLe(byte[] limitKey) throws IOException {
        if (mView.endRangeCompare(limitKey) <= 0) {
            return mSource.nextLe(limitKey);
        } else {
            return next();
        }
    }

    @Override
    public LockResult nextLt(byte[] limitKey) throws IOException {
        if (mView.endRangeCompare(limitKey) <= 0) {
            return mSource.nextLt(limitKey);
        } else {
            return next();
        }
    }

    @Override
    public LockResult previous() throws IOException {
        BoundedView view = mView;
        byte[] start = view.mStart;
        Cursor source = mSource;
        if (start == null) {
            return source.previous();
        } else if ((view.mMode & START_EXCLUSIVE) == 0) {
            return source.previousGe(start);
        } else {
            return source.previousGt(start);
        }
    }

    @Override
    public LockResult previousGe(byte[] limitKey) throws IOException {
        if (mView.startRangeCompare(limitKey) >= 0) {
            return mSource.previousGe(limitKey);
        } else {
            return previous();
        }
    }

    @Override
    public LockResult previousGt(byte[] limitKey) throws IOException {
        if (mView.startRangeCompare(limitKey) >= 0) {
            return mSource.previousGt(limitKey);
        } else {
            return previous();
        }
    }

    @Override
    public LockResult find(byte[] key) throws IOException {
        if (mView.inRange(key)) {
            return mSource.find(key);
        }
        reset();
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult findGe(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        final Cursor source = mSource;
        if (view.mEnd == null) {
            return source.findGe(key);
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findGe(key);

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
            return source.findGt(key);
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findGt(key);

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
            return source.findLe(key);
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findLe(key);

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
            return source.findLt(key);
        }

        final Transaction txn = source.link(Transaction.BOGUS);
        final boolean autoload = source.autoload(false);
        try {
            source.findLt(key);

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
            return mSource.findNearby(key);
        }
        reset();
        return LockResult.UNOWNED;
    }

    @Override
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.random(mView.adjustLowKey(lowKey), mView.adjustHighKey(highKey));
    }

    @Override
    public LockResult load() throws IOException {
        return mSource.load();
    }

    @Override
    public void store(byte[] value) throws IOException {
        mSource.store(value);
    }

    @Override
    public void commit(byte[] value) throws IOException {
        mSource.commit(value);
    }

    /*
    @Override
    public Stream newStream() {
        return new SubStream(mView, mSource.newStream());
    }
    */

    @Override
    public Cursor copy() {
        return new BoundedCursor(mView, mSource.copy());
    }

    @Override
    public void reset() {
        mSource.reset();
    }
}
