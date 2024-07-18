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
import java.math.MathContext;

import org.cojen.tupl.core.Utils;

/**
 * Defines a growable circular buffer of values, used by window functions. For efficiency, no
 * special bounds checking operations are performed.
 *
 * @author Brian S. O'Neill
 */
public class ValueBuffer<V> {
    private V[] mValues;
    private int mFirst, mSize;

    /**
     * @param initialCapacity must be at least one
     */
    @SuppressWarnings("unchecked")
    public ValueBuffer(int initialCapacity) {
        initialCapacity = Utils.roundUpPower2(initialCapacity);
        mValues = (V[]) new Object[initialCapacity];
    }

    public final int size() {
        return mSize;
    }

    /**
     * Clear and add one value.
     */
    public final void init(V value) {
        mValues[0] = value;
        mFirst = 0;
        mSize = 1;
    }

    public final void clear() {
        mSize = 0;
    }

    /**
     * Add one value to the end.
     */
    public final void add(V value) {
        V[] values = mValues;
        int size = mSize;
        if (size >= values.length) {
            mValues = values = expand(values, mFirst);
            mFirst = 0;
        }
        values[(mFirst + size) & (values.length - 1)] = value;
        mSize = size + 1;
    }

    @SuppressWarnings("unchecked")
    private static <V> V[] expand(V[] values, int first) {
        var newValues = (V[]) new Object[values.length << 1];
        System.arraycopy(values, first, newValues, 0, values.length - first);
        System.arraycopy(values, 0, newValues, values.length - first, first);
        return newValues;
    }

    /**
     * Returns a value at the given index, where zero always represents the first value.
     */
    public final V get(int index) {
        V[] values = mValues;
        return values[(mFirst + index) & (values.length - 1)];
    }

    /**
     * Remove the given amount of values from the first.
     */
    public final void remove(int amount) {
        mFirst = (mFirst + amount) & (mValues.length - 1);
        mSize -= amount;
    }

    /**
     * Removes and returns the first value.
     */
    public final V removeFirst() {
        V[] values = mValues;
        int first = mFirst;
        mFirst = (first + 1) & (values.length - 1);
        mSize--;
        return values[first & (values.length - 1)];
    }

    /**
     * Returns the count of non-null values over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to count
     */
    public final int count(int from, int num) {
        return count(mValues, mFirst + from, num);
    }

    private static int count(Object[] values, int first, int num) {
        first &= (values.length - 1);
        int end = first + num;
        int count = 0;
        if (end > values.length) {
            while (first < values.length) {
                if (values[first++] != null) {
                    count++;
                }
            }
            end -= first;
            first = 0;
        }
        while (first < end) {
            if (values[first++] != null) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the sum of non-null values over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to sum together
     */
    public final V sum(int from, int num) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the average of non-null values over the given range. If the number is zero, then
     * an exception is thrown if the return type cannot be null or NaN.
     *
     * @param from inclusive first index
     * @param num number of values to average together
     */
    public final V average(int from, int num) {
        throw new UnsupportedOperationException();
    }

    public static class OfByte {
        private byte[] mValues;
        private int mFirst, mSize;

        public OfByte(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new byte[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(byte value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(byte value) {
            byte[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static byte[] expand(byte[] values, int first) {
            var newValues = new byte[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final byte get(int index) {
            byte[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final byte removeFirst() {
            byte[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }
    }

    public static class OfShort {
        private short[] mValues;
        private int mFirst, mSize;

        public OfShort(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new short[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(short value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(short value) {
            short[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static short[] expand(short[] values, int first) {
            var newValues = new short[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final short get(int index) {
            short[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final short removeFirst() {
            short[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }
    }

    public static class OfInt {
        private int[] mValues;
        private int mFirst, mSize;

        public OfInt(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new int[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(int value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(int value) {
            int[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static int[] expand(int[] values, int first) {
            var newValues = new int[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final int get(int index) {
            int[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final int removeFirst() {
            int[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }
    }

    public static class OfLong {
        private long[] mValues;
        private int mFirst, mSize;

        public OfLong(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new long[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(long value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(long value) {
            long[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static long[] expand(long[] values, int first) {
            var newValues = new long[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final long get(int index) {
            long[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final long removeFirst() {
            long[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final long sum(int from, int num) {
            long[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            long sum = 0;
            if (end > values.length) {
                while (first < values.length) {
                    sum = Math.addExact(sum, values[first++]);
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                sum = Math.addExact(sum, values[first++]);
            }
            return sum;
        }

        public final long average(int from, int num) {
            return sum(from, num) / num;
        }
    }

    public static class OfLongObj {
        private Long[] mValues;
        private int mFirst, mSize;

        public OfLongObj(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new Long[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(Long value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(Long value) {
            Long[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static Long[] expand(Long[] values, int first) {
            var newValues = new Long[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final Long get(int index) {
            Long[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final Long removeFirst() {
            Long[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final int count(int from, int num) {
            return ValueBuffer.count(mValues, mFirst + from, num);
        }

        public final Long sum(int from, int num) {
            Long[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            Long sum = 0L;
            if (end > values.length) {
                while (first < values.length) {
                    Long addend = values[first++];
                    if (addend != null) {
                        sum = Math.addExact(sum, addend);
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                Long addend = values[first++];
                if (addend != null) {
                    sum = Math.addExact(sum, addend);
                }
            }
            return sum;
        }

        public final Long average(int from, int num) {
            Long[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            Long sum = 0L;
            long count = 0;
            if (end > values.length) {
                while (first < values.length) {
                    Long addend = values[first++];
                    if (addend != null) {
                        sum = Math.addExact(sum, addend);
                        count++;
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                Long addend = values[first++];
                if (addend != null) {
                    sum = Math.addExact(sum, addend);
                    count++;
                }
            }
            return count == 0 ? null : (sum / count);
        }
    }

    public static class OfFloat {
        private float[] mValues;
        private int mFirst, mSize;

        public OfFloat(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new float[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(float value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(float value) {
            float[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static float[] expand(float[] values, int first) {
            var newValues = new float[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final float get(int index) {
            float[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final float removeFirst() {
            float[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }
    }

    public static class OfDouble {
        private double[] mValues;
        private int mFirst, mSize;

        public OfDouble(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new double[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(double value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(double value) {
            double[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static double[] expand(double[] values, int first) {
            var newValues = new double[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final double get(int index) {
            double[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final double removeFirst() {
            double[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final double sum(int from, int num) {
            double[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            double sum = 0;
            if (end > values.length) {
                while (first < values.length) {
                    sum += values[first++];
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                sum += values[first++];
            }
            return sum;
        }

        public final double average(int from, int num) {
            return sum(from, num) / num;
        }
    }

    public static class OfDoubleObj {
        private Double[] mValues;
        private int mFirst, mSize;

        public OfDoubleObj(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new Double[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(Double value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(Double value) {
            Double[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static Double[] expand(Double[] values, int first) {
            var newValues = new Double[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final Double get(int index) {
            Double[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final Double removeFirst() {
            Double[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final int count(int from, int num) {
            return ValueBuffer.count(mValues, mFirst + from, num);
        }

        public final Double sum(int from, int num) {
            Double[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            Double sum = 0.0;
            if (end > values.length) {
                while (first < values.length) {
                    Double addend = values[first++];
                    if (addend != null) {
                        sum += addend;
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                Double addend = values[first++];
                if (addend != null) {
                    sum += addend;
                }
            }
            return sum;
        }

        public final Double average(int from, int num) {
            Double[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            Double sum = 0.0;
            int count = 0;
            if (end > values.length) {
                while (first < values.length) {
                    Double addend = values[first++];
                    if (addend != null) {
                        sum += addend;
                        count++;
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                Double addend = values[first++];
                if (addend != null) {
                    sum += addend;
                    count++;
                }
            }
            return count == 0 ? null : (sum / count);
        }
    }

    public static class OfBigInteger {
        private BigInteger[] mValues;
        private int mFirst, mSize;

        public OfBigInteger(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new BigInteger[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(BigInteger value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(BigInteger value) {
            BigInteger[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static BigInteger[] expand(BigInteger[] values, int first) {
            var newValues = new BigInteger[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final BigInteger get(int index) {
            BigInteger[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final BigInteger removeFirst() {
            BigInteger[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final int count(int from, int num) {
            return ValueBuffer.count(mValues, mFirst + from, num);
        }

        public final BigInteger sum(int from, int num) {
            BigInteger[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            BigInteger sum = BigInteger.ZERO;
            if (end > values.length) {
                while (first < values.length) {
                    BigInteger addend = values[first++];
                    if (addend != null) {
                        sum = sum.add(addend);
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                BigInteger addend = values[first++];
                if (addend != null) {
                    sum = sum.add(addend);
                }
            }
            return sum;
        }

        public final BigInteger average(int from, int num) {
            BigInteger[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            BigInteger sum = BigInteger.ZERO;
            long count = 0;
            if (end > values.length) {
                while (first < values.length) {
                    BigInteger addend = values[first++];
                    if (addend != null) {
                        sum = sum.add(addend);
                        count++;
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                BigInteger addend = values[first++];
                if (addend != null) {
                    sum = sum.add(addend);
                    count++;
                }
            }
            return count == 0 ? null : sum.divide(BigInteger.valueOf(count));
        }
    }

    public static class OfBigDecimal {
        private BigDecimal[] mValues;
        private int mFirst, mSize;

        public OfBigDecimal(int initialCapacity) {
            initialCapacity = Utils.roundUpPower2(initialCapacity);
            mValues = new BigDecimal[initialCapacity];
        }

        public final int size() {
            return mSize;
        }

        public final void init(BigDecimal value) {
            mValues[0] = value;
            mFirst = 0;
            mSize = 1;
        }

        public final void clear() {
            mSize = 0;
        }

        public final void add(BigDecimal value) {
            BigDecimal[] values = mValues;
            int size = mSize;
            if (size >= values.length) {
                mValues = values = expand(values, mFirst);
                mFirst = 0;
            }
            values[(mFirst + size) & (values.length - 1)] = value;
            mSize = size + 1;
        }

        private static BigDecimal[] expand(BigDecimal[] values, int first) {
            var newValues = new BigDecimal[values.length << 1];
            System.arraycopy(values, first, newValues, 0, values.length - first);
            System.arraycopy(values, 0, newValues, values.length - first, first);
            return newValues;
        }

        public final BigDecimal get(int index) {
            BigDecimal[] values = mValues;
            return values[(mFirst + index) & (values.length - 1)];
        }

        public final void remove(int amount) {
            mFirst = (mFirst + amount) & (mValues.length - 1);
            mSize -= amount;
        }

        public final BigDecimal removeFirst() {
            BigDecimal[] values = mValues;
            int first = mFirst;
            mFirst = (first + 1) & (values.length - 1);
            mSize--;
            return values[first & (values.length - 1)];
        }

        public final int count(int from, int num) {
            return ValueBuffer.count(mValues, mFirst + from, num);
        }

        public final BigDecimal sum(int from, int num) {
            BigDecimal[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            BigDecimal sum = BigDecimal.ZERO;
            if (end > values.length) {
                while (first < values.length) {
                    BigDecimal addend = values[first++];
                    if (addend != null) {
                        sum = sum.add(addend);
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                BigDecimal addend = values[first++];
                if (addend != null) {
                    sum = sum.add(addend);
                }
            }
            return sum;
        }

        public final BigDecimal average(int from, int num) {
            BigDecimal[] values = mValues;
            int first = (mFirst + from) & (values.length - 1);
            int end = first + num;
            BigDecimal sum = BigDecimal.ZERO;
            long count = 0;
            if (end > values.length) {
                while (first < values.length) {
                    BigDecimal addend = values[first++];
                    if (addend != null) {
                        sum = sum.add(addend);
                        count++;
                    }
                }
                end -= first;
                first = 0;
            }
            while (first < end) {
                BigDecimal addend = values[first++];
                if (addend != null) {
                    sum = sum.add(addend);
                    count++;
                }
            }
            return count == 0 ? null : sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL64);
        }
    }
}
