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

import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class BoundedView extends SubView {
    static View viewGe(View view, byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(view, key, null, 0);
    }

    static View viewGt(View view, byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(view, key, null, START_EXCLUSIVE);
    }

    static View viewLe(View view, byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(view, null, key, 0);
    }

    static View viewLt(View view, byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(view, null, key, END_EXCLUSIVE);
    }

    static View viewPrefix(View view, byte[] prefix, int trim) {
        prefixCheck(prefix, trim);

        byte[] end = prefix.clone();
        int mode;
        if (increment(end, 0, end.length)) {
            mode = END_EXCLUSIVE;
        } else {
            // Prefix is highest possible, so no need for an end bound.
            end = null;
            mode = 0;
        }

        view = new BoundedView(view, prefix, end, mode);

        if (trim > 0) {
            view = new TrimmedView(view, prefix, trim);
        }

        return view;
    }

    static final int START_EXCLUSIVE = 0xfffffffe, END_EXCLUSIVE = 1;

    final byte[] mStart;
    final byte[] mEnd;
    final int mMode;

    BoundedView(View source, byte[] start, byte[] end, int mode) {
        super(source);
        mStart = start;
        mEnd = end;
        mMode = mode;
    }

    @Override
    public Ordering getOrdering() {
        return mSource.getOrdering();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new BoundedCursor(this, mSource.newCursor(txn));
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        return mSource.count(adjustLowKey(lowKey), adjustHighKey(highKey));
    }

    @Override
    public View viewGe(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        if (startRangeCompare(key) <= 0) {
            return this;
        }
        return new BoundedView(mSource, key, mEnd, mMode & ~START_EXCLUSIVE);
    }

    @Override
    public View viewGt(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        if (startRangeCompare(key) < 0) {
            return this;
        }
        return new BoundedView(mSource, key, mEnd, mMode | START_EXCLUSIVE);
    }

    @Override
    public View viewLe(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        if (endRangeCompare(key) >= 0) {
            return this;
        }
        return new BoundedView(mSource, mStart, key, mMode & ~END_EXCLUSIVE);
    }

    @Override
    public View viewLt(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        if (endRangeCompare(key) > 0) {
            return this;
        }
        return new BoundedView(mSource, mStart, key, mMode | END_EXCLUSIVE);
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        SubView.prefixCheck(prefix, trim);

        // Note: Slight optimization is possible, by not creating a short-lived view object.
        // Current implementation is simpler, and optimization seems unnecessary.

        View view = viewGe(prefix);

        byte[] end = prefix.clone();
        if (increment(end, 0, end.length)) {
            view = view.viewLt(end);
        }

        if (trim > 0) {
            view = new TrimmedView(view, prefix, trim);
        }

        return view;
    }

    @Override
    boolean inRange(byte[] key) {
        return startRangeCompare(key) >= 0 && endRangeCompare(key) <= 0;
    }

    /**
     * @param key must not be null
     * @return <0 if less than start, 0 if equal (in range), >0 if higher (in range)
     */
    int startRangeCompare(byte[] key) {
        byte[] start = mStart;
        return start == null ? 1 : startRangeCompare(start, key);
    }

    /**
     * @param start must not be null
     * @param key must not be null
     * @return <0 if less than start, 0 if equal (in range), >0 if higher (in range)
     */
    int startRangeCompare(byte[] start, byte[] key) {
        int result = compareUnsigned(key, 0, key.length, start, 0, start.length);
        return result != 0 ? result : (mMode & START_EXCLUSIVE);
    }

    /**
     * @param key must not be null
     * @return <0 if less than end (in range), 0 if equal (in range), >0 if higher
     */
    int endRangeCompare(byte[] key) {
        byte[] end = mEnd;
        return end == null ? -1 : endRangeCompare(end, key);
    }

    /**
     * @param end must not be null
     * @param key must not be null
     * @return <0 if less than end (in range), 0 if equal (in range), >0 if higher
     */
    int endRangeCompare(byte[] end, byte[] key) {
        int result = compareUnsigned(key, 0, key.length, end, 0, end.length);
        return result != 0 ? result : (mMode & END_EXCLUSIVE);
    }

    byte[] adjustLowKey(byte[] lowKey) {
        byte[] start = mStart;
        if (start != null && (lowKey == null || startRangeCompare(start, lowKey) < 0)) {
            lowKey = start;
            if ((mMode & START_EXCLUSIVE) != 0) {
                // Switch to exclusive start behavior.
                lowKey = ViewUtils.appendZero(lowKey);
            }
        }
        return lowKey;
    }

    byte[] adjustHighKey(byte[] highKey) {
        byte[] end = mEnd;
        if (end != null && (highKey == null || endRangeCompare(end, highKey) > 0)) {
            highKey = end;
            if ((mMode & END_EXCLUSIVE) == 0) {
                // Switch to inclusive end behavior.
                highKey = ViewUtils.appendZero(highKey);
            }
        }
        return highKey;
    }
}
