/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table.expr;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Defines a growable circular buffer of values, which act upon moving ranges of values known
 * as frames. The implementations of the sum and average methods perform a full calculation
 * over each range rather than performing incremental calculations. Although less efficient,
 * it's more accurate. If the frame is constant, consider performing incremental calculations
 * instead.
 *
 * @author Brian S. O'Neill
 */
public final class WindowBuffer<V> extends ValueBuffer<V> {
    // Range of values that the buffer has, relative to the current row. Inclusive bounds.
    private int mStart, mEnd;

    public WindowBuffer(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Calculates an initial buffer capacity.
     *
     * @param frameStart inclusive frame start
     * @param frameEnd inclusive frame end
     */
    public static int capacityFor(long frameStart, long frameEnd) {
        long capacity = frameEnd - frameStart + 1;
        if (0 < capacity && capacity <= 1024) {
            return (int) capacity;
        } else {
            return 8;
        }
    }

    /**
     * Clear and add one value.
     */
    public void begin(V value) {
        init(value);
        mStart = mEnd = 0;
    }

    /**
     * Append a value and extend the end of the range.
     */
    public void append(V value) {
        add(value);
        mEnd++;
    }

    /**
     * Determine if enough values have been appended such that a complete calculation is
     * possible. If the frame end is constantly open (> max int), then this method would always
     * return false.
     *
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public boolean ready(long frameEnd) {
        return mEnd >= frameEnd;
    }

    /**
     * Increment the current row by one. Should only be called when the frame start is
     * constantly open (< min int) or is never a constant. It should be noted that an open
     * range never removes anything from the buffer, and so a buffer probably shouldn't be used
     * at all.
     */
    public void advance() {
        mStart--;
        mEnd--;
    }

    /**
     * Increment the current row by one, and remove a value if possible. If the frame start is
     * constantly open (< min int), then the zero-arg variant of this method should be called.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     */
    public void advanceAndRemove(long frameStart) {
        int start = mStart - 1;
        int end = mEnd - 1;
        if (frameStart > start) {
            if (end >= start) {
                remove(1);
            }
            start++;
        }
        mStart = start;
        mEnd = end;
    }

    /**
     * Increment the current row by one, and remove a value. This is a simplified variant of
     * the advanceAndRemove method which should only be called when the frame start is
     * constantly greater or equal to zero.
     */
    public void advanceAndRemove() {
        mEnd--;
        remove(1);
    }

    // FIXME: Define advanceAndRemoveGet variants, to be used with incremental modes.

    /**
     * Returns the number of non-null values which are available over the given range.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public int frameCount(long frameStart, long frameEnd) {
        // Note that mStart is always 0 when frameStart is always >= 0.
        int start = mStart;
        frameStart = Math.max(frameStart, start);
        frameEnd = Math.min(frameEnd, mEnd);
        long count = frameEnd - frameStart + 1;
        // Note that count can be negative only for ranges that don't include the current row
        // (which is zero).
        return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
    }

    /**
     * Returns the sum of non-null values over the given frame range. A value of zero is
     * assumed for any part of the frame range which lies outside the set of available values.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameSum(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the average of non-null values over the given frame range. If the divisor is
     * null or zero, then the average is null (NaN if null isn't allowed). A value of zero is
     * assumed for any part of the frame range which lies outside the set of available values.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameAverage(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Same as frameAverage except it always returns null instead of NaN when dividing by zero.
     */
    public V frameAverageNoNaN(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    public static final class OfLong extends ValueBuffer.OfLong {
        private int mStart, mEnd;

        public OfLong(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(long value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(long value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            frameStart = Math.max(frameStart, mStart);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return Math.max(0, (int) count);
        }

        public long frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0L : sum((int) (frameStart - start), (int) count);
        }

        public Long frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public Long frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfLongObj extends ValueBuffer.OfLongObj {
        private int mStart, mEnd;

        public OfLongObj(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(Long value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(Long value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
        }

        public long frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0L : sum((int) (frameStart - start), (int) count);
        }

        public Long frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public Long frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfULong extends ValueBuffer.OfULong {
        private int mStart, mEnd;

        public OfULong(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(long value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(long value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            frameStart = Math.max(frameStart, mStart);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return Math.max(0, (int) count);
        }

        public long frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0L : sum((int) (frameStart - start), (int) count);
        }

        public Long frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public Long frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfULongObj extends ValueBuffer.OfULongObj {
        private int mStart, mEnd;

        public OfULongObj(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(Long value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(Long value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
        }

        public long frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0L : sum((int) (frameStart - start), (int) count);
        }

        public Long frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public Long frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfDouble extends ValueBuffer.OfDouble {
        private int mStart, mEnd;

        public OfDouble(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(double value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(double value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            frameStart = Math.max(frameStart, mStart);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return Math.max(0, (int) count);
        }

        public double frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0.0 : sum((int) (frameStart - start), (int) count);
        }

        public double frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? Double.NaN : average((int) (frameStart - start), (int) count);
        }

        public Double frameAverageNoNaN(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }
    }

    public static final class OfDoubleObj extends ValueBuffer.OfDoubleObj {
        private int mStart, mEnd;

        public OfDoubleObj(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(Double value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(Double value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
        }

        public double frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0.0 : sum((int) (frameStart - start), (int) count);
        }

        public Double frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public Double frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfBigInteger extends ValueBuffer.OfBigInteger {
        private int mStart, mEnd;

        public OfBigInteger(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(BigInteger value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(BigInteger value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
        }

        public BigInteger frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? BigInteger.ZERO : sum((int) (frameStart - start), (int) count);
        }

        public BigInteger frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public BigInteger frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }

    public static final class OfBigDecimal extends ValueBuffer.OfBigDecimal {
        private int mStart, mEnd;

        public OfBigDecimal(int initialCapacity) {
            super(initialCapacity);
        }

        public void begin(BigDecimal value) {
            init(value);
            mStart = mEnd = 0;
        }

        public void append(BigDecimal value) {
            add(value);
            mEnd++;
        }

        public boolean ready(long frameEnd) {
            return mEnd >= frameEnd;
        }

        public void advance() {
            mStart--;
            mEnd--;
        }

        public void advanceAndRemove(long frameStart) {
            int start = mStart - 1;
            int end = mEnd - 1;
            if (frameStart > start) {
                if (end >= start) {
                    remove(1);
                }
                start++;
            }
            mStart = start;
            mEnd = end;
        }

        public void advanceAndRemove() {
            mEnd--;
            remove(1);
        }

        public int frameCount(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? 0 : count((int) (frameStart - start), (int) count);
        }

        public BigDecimal frameSum(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? BigDecimal.ZERO : sum((int) (frameStart - start), (int) count);
        }

        public BigDecimal frameAverage(long frameStart, long frameEnd) {
            // Note that mStart is always 0 when frameStart is always >= 0.
            int start = mStart;
            frameStart = Math.max(frameStart, start);
            frameEnd = Math.min(frameEnd, mEnd);
            long count = frameEnd - frameStart + 1;
            // Note that count can be negative only for ranges that don't include the current
            // row (which is zero).
            return count <= 0 ? null : average((int) (frameStart - start), (int) count);
        }

        public BigDecimal frameAverageNoNaN(long frameStart, long frameEnd) {
            return frameAverage(frameStart, frameEnd);
        }
    }
}
