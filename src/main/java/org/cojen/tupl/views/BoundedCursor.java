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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Transaction;

import static org.cojen.tupl.views.BoundedView.*;

/**
 * Cursor implementation vended by {@link BoundedView}.
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
    public long valueLength() throws IOException {
        return mSource.valueLength();
    }

    @Override
    public void valueLength(long length) throws IOException {
        mSource.valueLength(length);
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        return mSource.valueRead(pos, buf, off, len);
    }

    @Override
    public void valueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        mSource.valueWrite(pos, buf, off, len);
    }

    @Override
    public void valueClear(long pos, long length) throws IOException {
        mSource.valueClear(pos, length);
    }

    @Override
    public InputStream newValueInputStream(long pos) throws IOException {
        return mSource.newValueInputStream(pos);
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        return mSource.newValueInputStream(pos, bufferSize);
    }

    @Override
    public OutputStream newValueOutputStream(long pos) throws IOException {
        return mSource.newValueOutputStream(pos);
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        return mSource.newValueOutputStream(pos, bufferSize);
    }

    @Override
    public Ordering ordering() {
        return mSource.ordering();
    }

    @Override
    public Comparator<byte[]> comparator() {
        return mSource.comparator();
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
    public boolean register() throws IOException {
        return mSource.register();
    }

    @Override
    public void unregister() {
        mSource.unregister();
    }

    @Override
    public LockResult first() throws IOException {
        LockResult result;

        final BoundedView view = mView;
        final byte[] start = view.mStart;
        final Cursor source = mSource;

        if (view.mEnd == null) {
            if (start == null) {
                result = source.first();
            } else if ((view.mMode & START_EXCLUSIVE) == 0) {
                result = source.findGe(start);
            } else {
                result = source.findGt(start);
            }
        } else if (start == null) {
            // If isolation level is read committed, then the first key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.first();
            byte[] key = source.key();
            if (key == null) {
                result = LockResult.UNOWNED;
            } else if (view.endRangeCompare(view.mEnd, key) > 0) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                source.reset();
                result = LockResult.UNOWNED;
            }
        } else if (view.endRangeCompare(view.mEnd, start) > 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else if ((view.mMode & START_EXCLUSIVE) == 0) {
            // If isolation level is read committed, then the first key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.find(start);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return next();
            }
        } else {
            ViewUtils.findNoLock(source, start);
            return next();
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult last() throws IOException {
        LockResult result;

        final BoundedView view = mView;
        final byte[] end = view.mEnd;
        final Cursor source = mSource;

        if (view.mStart == null) {
            if (end == null) {
                result = source.last();
            } else if ((view.mMode & END_EXCLUSIVE) == 0) {
                result = source.findLe(end);
            } else {
                result = source.findLt(end);
            }
        } else if (end == null) {
            // If isolation level is read committed, then the last key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.last();
            byte[] key = source.key();
            if (key == null) {
                result = LockResult.UNOWNED;
            } else if (view.startRangeCompare(view.mStart, key) < 0) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                source.reset();
                result = LockResult.UNOWNED;
            }
        } else if (view.startRangeCompare(view.mStart, end) < 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else if ((view.mMode & END_EXCLUSIVE) == 0) {
            // If isolation level is read committed, then the last key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.find(end);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return previous();
            }
        } else {
            ViewUtils.findNoLock(source, end);
            return previous();
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
        final BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mEnd == null) {
            result = source.findGe(key);
        } else if (view.endRangeCompare(view.mEnd, key) > 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            // If isolation level is read committed, then the first key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.find(key);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return next();
            }
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult findGt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mEnd == null) {
            result = source.findGt(key);
        } else if (view.endRangeCompare(view.mEnd, key) > 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            ViewUtils.findNoLock(source, key);
            return next();
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult findLe(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mStart == null) {
            result = source.findLe(key);
        } else if (view.startRangeCompare(view.mStart, key) < 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            // If isolation level is read committed, then the last key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.find(key);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return previous();
            }
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult findLt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mStart == null) {
            result = source.findLt(key);
        } else if (view.startRangeCompare(view.mStart, key) < 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            ViewUtils.findNoLock(source, key);
            return previous();
        }

        mOutOfBounds = false;
        return result;
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
    public LockResult findNearbyGe(byte[] key) throws IOException {
        final BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mEnd == null) {
            result = source.findNearbyGe(key);
        } else if (view.endRangeCompare(view.mEnd, key) > 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            // If isolation level is read committed, then the first key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.findNearby(key);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return next();
            }
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult findNearbyGt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.startRangeCompare(key) < 0) {
            return first();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mEnd == null) {
            result = source.findNearbyGt(key);
        } else if (view.endRangeCompare(view.mEnd, key) > 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            ViewUtils.findNearbyNoLock(source, key);
            return next();
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult findNearbyLe(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mStart == null) {
            result = source.findNearbyLe(key);
        } else if (view.startRangeCompare(view.mStart, key) < 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            // If isolation level is read committed, then the last key must be
            // locked. Otherwise, an uncommitted delete could be observed.
            result = source.findNearby(key);
            if (source.value() == null) {
                if (result == LockResult.ACQUIRED) {
                    source.link().unlock();
                }
                return previous();
            }
        }

        mOutOfBounds = false;
        return result;
     }

    @Override
    public LockResult findNearbyLt(byte[] key) throws IOException {
        BoundedView view = mView;
        if (view.endRangeCompare(key) > 0) {
            return last();
        }

        LockResult result;

        final Cursor source = mSource;
        if (view.mStart == null) {
            result = source.findNearbyLt(key);
        } else if (view.startRangeCompare(view.mStart, key) < 0) {
            source.reset();
            result = LockResult.UNOWNED;
        } else {
            ViewUtils.findNearbyNoLock(source, key);
            return previous();
        }

        mOutOfBounds = false;
        return result;
    }

    @Override
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive)
        throws IOException
    {
        byte[] start = mView.mStart;
        if (start != null && (lowKey == null || mView.startRangeCompare(start, lowKey) < 0)) {
            lowKey = start;
            lowInclusive = (mView.mMode & START_EXCLUSIVE) == 0;
        }

        byte[] end = mView.mEnd;
        if (end != null && (highKey == null || mView.endRangeCompare(end, highKey) > 0)) {
            highKey = end;
            highInclusive = (mView.mMode & END_EXCLUSIVE) == 0;
        }

        LockResult result = mSource.random(lowKey, lowInclusive, highKey, highInclusive);

        mOutOfBounds = false;
        return result;
    }

    @Override
    public boolean exists() throws IOException {
        if (mOutOfBounds) {
            throw fail();
        } else {
            return mSource.exists();
        }
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

    @Override
    public Cursor copy() {
        var copy = new BoundedCursor(mView, mSource.copy());
        copy.mOutOfBounds = mOutOfBounds;
        return copy;
    }

    @Override
    public void reset() {
        mSource.reset();
        mOutOfBounds = false;
    }

    @Override
    public void close() throws IOException {
        mSource.close();
        mOutOfBounds = false;
    }
}
