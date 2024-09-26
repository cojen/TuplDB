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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

import java.util.function.Supplier;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ValueBufferTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ValueBufferTest.class.getName());
    }

    private Type mValueType;
    private Random mRnd;
    private Supplier mSupplier;

    @Test
    public void testByte() throws Exception {
        mValueType = BasicType.make(byte.class, Type.TYPE_BYTE);
        mRnd = new Random(-6715343171648090204L);
        mSupplier = () -> (byte) mRnd.nextInt();
        suite();
    }

    @Test
    public void testShort() throws Exception {
        mValueType = BasicType.make(short.class, Type.TYPE_SHORT);
        mRnd = new Random(2357406073265565177L);
        mSupplier = () -> (short) mRnd.nextInt();
        suite();
    }

    @Test
    public void testInt() throws Exception {
        mValueType = BasicType.make(int.class, Type.TYPE_INT);
        mRnd = new Random(1634804903719115094L);
        mSupplier = () -> mRnd.nextInt();
        suite();
    }

    @Test
    public void testLong() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_LONG);
        mRnd = new Random(-7824355477638793438L);
        mSupplier = () -> mRnd.nextLong();
        suite();
    }

    @Test
    public void testUByte() throws Exception {
        mValueType = BasicType.make(byte.class, Type.TYPE_UBYTE);
        mRnd = new Random(1459781713136547659L);
        mSupplier = () -> (byte) mRnd.nextInt();
        suite();
    }

    @Test
    public void testUShort() throws Exception {
        mValueType = BasicType.make(short.class, Type.TYPE_USHORT);
        mRnd = new Random(8024560920252118409L);
        mSupplier = () -> (short) mRnd.nextInt();
        suite();
    }

    @Test
    public void testUInt() throws Exception {
        mValueType = BasicType.make(int.class, Type.TYPE_UINT);
        mRnd = new Random(7973859961815430294L);
        mSupplier = () -> mRnd.nextInt();
        suite();
    }

    @Test
    public void testULong() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_ULONG);
        mRnd = new Random(2129251344674283834L);
        mSupplier = () -> mRnd.nextLong();
        suite();
    }

    @Test
    public void testFloat() throws Exception {
        mValueType = BasicType.make(float.class, Type.TYPE_FLOAT);
        mRnd = new Random(4768823056903811507L);
        mSupplier = () -> mRnd.nextFloat();
        suite();
    }

    @Test
    public void testDouble() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE);
        mRnd = new Random(5745920104587566125L);
        mSupplier = () -> mRnd.nextDouble();
        suite();
    }

    @Test
    public void testByteObj() throws Exception {
        mValueType = BasicType.make(byte.class, Type.TYPE_BYTE).nullable();
        mRnd = new Random(-2241069825208517093L);
        mSupplier = () -> mRnd.nextBoolean() ? null : (byte) mRnd.nextInt();
        suite();
    }

    @Test
    public void testShortObj() throws Exception {
        mValueType = BasicType.make(short.class, Type.TYPE_SHORT).nullable();
        mRnd = new Random(5883341748551672806L);
        mSupplier = () -> mRnd.nextBoolean() ? null : (short) mRnd.nextInt();
        suite();
    }

    @Test
    public void testIntObj() throws Exception {
        mValueType = BasicType.make(int.class, Type.TYPE_INT).nullable();
        mRnd = new Random(-9168323720729105101L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextInt();
        suite();
    }

    @Test
    public void testLongObj() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_LONG).nullable();
        mRnd = new Random(-8762546741252455949L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextLong();
        suite();
    }

    @Test
    public void testUByteObj() throws Exception {
        mValueType = BasicType.make(byte.class, Type.TYPE_UBYTE).nullable();
        mRnd = new Random(-2719914061850918560L);
        mSupplier = () -> mRnd.nextBoolean() ? null : (byte) mRnd.nextInt();
        suite();
    }

    @Test
    public void testUShortObj() throws Exception {
        mValueType = BasicType.make(short.class, Type.TYPE_USHORT).nullable();
        mRnd = new Random(5603383064693281342L);
        mSupplier = () -> mRnd.nextBoolean() ? null : (short) mRnd.nextInt();
        suite();
    }

    @Test
    public void testUIntObj() throws Exception {
        mValueType = BasicType.make(int.class, Type.TYPE_UINT).nullable();
        mRnd = new Random(-5529929162584630307L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextInt();
        suite();
    }

    @Test
    public void testULongObj() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_ULONG).nullable();
        mRnd = new Random(9024611825615556738L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextLong();
        suite();
    }

    @Test
    public void testFloatObj() throws Exception {
        mValueType = BasicType.make(float.class, Type.TYPE_FLOAT).nullable();
        mRnd = new Random(-6429286807093022663L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextFloat();
        suite();
    }

    @Test
    public void testDoubleObj() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE).nullable();
        mRnd = new Random(-2028328174386167224L);
        mSupplier = () -> mRnd.nextBoolean() ? null : mRnd.nextDouble();
        suite();
    }

    @Test
    public void testBigInteger() throws Exception {
        mValueType = BasicType.make(BigInteger.class, Type.TYPE_BIG_INTEGER).nullable();
        mRnd = new Random(2465279372061979173L);
        mSupplier = () -> mRnd.nextBoolean() ? null : BigInteger.valueOf(mRnd.nextLong());
        suite();
    }

    @Test
    public void testBigDecimal() throws Exception {
        mValueType = BasicType.make(BigDecimal.class, Type.TYPE_BIG_DECIMAL).nullable();
        mRnd = new Random(2209082465884494284L);
        mSupplier = () -> mRnd.nextBoolean() ? null : BigDecimal.valueOf(mRnd.nextLong());
        suite();
    }

    private Object[] values(int amt) {
        var values = new Object[amt];
        for (int i=0; i<amt; i++) {
            values[i] = mSupplier.get();
        }
        return values;
    }

    private void suite() throws Exception {
        basic();
        count();
        min();
        minNL();
        max();
        maxNL();
        sum();
        average();
    }

    private void basic() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Class<?> valueClass = mValueType.clazz();

        var sizeMethod = bufferClass.getMethod("size");
        var initMethod = bufferClass.getMethod("init", valueClass);
        var clearMethod = bufferClass.getMethod("clear");
        var addMethod = bufferClass.getMethod("add", valueClass);
        var getMethod = bufferClass.getMethod("get", int.class);
        var removeMethod = bufferClass.getMethod("remove", int.class);
        var removeFirstMethod = bufferClass.getMethod("removeFirst");

        int initialCapacity = mRnd.nextInt(1, 20);
        Object buffer = bufferClass.getConstructor(int.class).newInstance(initialCapacity);

        assertEquals(0, sizeMethod.invoke(buffer));

        Object[] values = values(mRnd.nextInt(10, 20));
        initMethod.invoke(buffer, values[0]);
        assertEquals(1, sizeMethod.invoke(buffer));
        clearMethod.invoke(buffer);
        assertEquals(0, sizeMethod.invoke(buffer));
        initMethod.invoke(buffer, values[0]);
        assertEquals(1, sizeMethod.invoke(buffer));
        assertEquals(values[0], getMethod.invoke(buffer, 0));
        for (int i=1; i<values.length; i++) {
            addMethod.invoke(buffer, values[i]);
            assertEquals(i + 1, sizeMethod.invoke(buffer));
            assertEquals(values[i], getMethod.invoke(buffer, i));
        }
        removeMethod.invoke(buffer, 3);
        assertEquals(values.length - 3, sizeMethod.invoke(buffer));
        for (int i=3; i<values.length; i++) {
            assertEquals(values[i], getMethod.invoke(buffer, i - 3));
        }
        for (int i=3; i<values.length; i++) {
            assertEquals(values[i], removeFirstMethod.invoke(buffer));
            assertEquals(values.length - i - 1, sizeMethod.invoke(buffer));
        }
    }

    /**
     * @return a ValueBuffer instance
     */
    private Object fill(Class<?> bufferClass, Object[] values) throws Exception {
        Class<?> valueClass = mValueType.clazz();

        var initMethod = bufferClass.getMethod("init", valueClass);
        var addMethod = bufferClass.getMethod("add", valueClass);

        int initialCapacity = mRnd.nextInt(1, 20);
        Object buffer = bufferClass.getConstructor(int.class).newInstance(initialCapacity);

        initMethod.invoke(buffer, values[0]);

        for (int i=1; i<values.length; i++) {
            addMethod.invoke(buffer, values[i]);
        }

        if (mRnd.nextBoolean()) {
            // Try to force the circular buffer wrap around.
            var removeFirstMethod = bufferClass.getMethod("removeFirst");
            for (int i=0; i<values.length; i++) {
                addMethod.invoke(buffer, values[i]);
                removeFirstMethod.invoke(buffer);
            }
        }

        return buffer;
    }

    @FunctionalInterface
    private interface OverRange {
        void accept(int from, int num) throws Exception;
    }

    private void overRanges(Object[] values, OverRange op) throws Exception {
        for (int i=0; i<20; i++) {
            int from, num;
            while (true) {
                from = mRnd.nextInt(values.length);
                num = mRnd.nextInt(values.length) - from;
                if (num >= 0) {
                    break;
                }
            }

            op.accept(from, num);
        }
    }

    private void count() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var countMethod = bufferClass.getMethod("count", int.class, int.class);

        overRanges(values, (from, num) -> {
            int actual = (int) countMethod.invoke(buffer, from, num);

            int expect = 0;
            for (int j=from; j<from+num; j++) {
                Object value = values[j];
                if (value != null) {
                    expect++;
                }
            }

            assertEquals(expect, actual);
        });
    }

    @SuppressWarnings("unchecked")
    private Object[] copyAndSortNullHigh(Object[] values, int from, int num) throws Exception {
        var copy = new Object[num];
        System.arraycopy(values, from, copy, 0, num);
        Arrays.sort(copy, Comparator.nullsLast(baseComparator()));
        return copy;
    }

    @SuppressWarnings("unchecked")
    private Object[] copyAndSortNullLow(Object[] values, int from, int num) throws Exception {
        var copy = new Object[num];
        System.arraycopy(values, from, copy, 0, num);
        Arrays.sort(copy, Comparator.nullsFirst(baseComparator()));
        return copy;
    }

    private Comparator baseComparator() throws Exception {
        if (!mValueType.isUnsignedInteger()) {
            return Comparator.naturalOrder();
        }

        Class<?> clazz = mValueType.clazz();
        Class<?> unboxed = Variable.unboxedType(clazz);
        Method m = Variable.boxedType(clazz).getMethod("compareUnsigned", unboxed, unboxed);

        return (a, b) -> {
            try {
                return (int) m.invoke(null, a, b);
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        };
    }

    private Object maxNum() {
        Class<?> clazz = mValueType.unboxedType();

        if (mValueType.isUnsignedInteger()) {
            if (clazz == byte.class) {
                return (byte) 0xff;
            } else if (clazz == short.class) {
                return (short) 0xffff;
            } else if (clazz == int.class) {
                return ~0;
            } else if (clazz == long.class) {
                return ~0L;
            } else {
                throw new AssertionError();
            }
        } else {
            if (clazz == byte.class) {
                return Byte.MAX_VALUE;
            } else if (clazz == short.class) {
                return Short.MAX_VALUE;
            } else if (clazz == int.class) {
                return Integer.MAX_VALUE;
            } else if (clazz == long.class) {
                return Long.MAX_VALUE;
            } else if (clazz == float.class) {
                return Float.MAX_VALUE;
            } else if (clazz == double.class) {
                return Double.MAX_VALUE;
            } else if (clazz == BigInteger.class || clazz == BigDecimal.class) {
                return null;
            } else {
                throw new AssertionError();
            }
        }
    }

    private Object minNum() {
        Class<?> clazz = mValueType.unboxedType();

        if (mValueType.isUnsignedInteger()) {
            if (clazz == byte.class) {
                return (byte) 0;
            } else if (clazz == short.class) {
                return (short) 0;
            } else if (clazz == int.class) {
                return 0;
            } else if (clazz == long.class) {
                return 0L;
            } else {
                throw new AssertionError();
            }
        } else {
            if (clazz == byte.class) {
                return Byte.MIN_VALUE;
            } else if (clazz == short.class) {
                return Short.MIN_VALUE;
            } else if (clazz == int.class) {
                return Integer.MIN_VALUE;
            } else if (clazz == long.class) {
                return Long.MIN_VALUE;
            } else if (clazz == float.class) {
                return Float.MIN_VALUE;
            } else if (clazz == double.class) {
                return Double.MIN_VALUE;
            } else if (clazz == BigInteger.class || clazz == BigDecimal.class) {
                return null;
            } else {
                throw new AssertionError();
            }
        }
    }

    private void min() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var minMethod = bufferClass.getMethod("min", int.class, int.class);

        overRanges(values, (from, num) -> {
            Object actual = minMethod.invoke(buffer, from, num);

            Object[] sorted = copyAndSortNullHigh(values, from, num);

            if (sorted.length != 0) {
                assertEquals(sorted[0], actual);
            } else if (mValueType.isNullable()) {
                assertNull(actual);
            } else {
                assertEquals(maxNum(), actual);
            }
        });
    }

    private void minNL() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var minMethod = bufferClass.getMethod("minNL", int.class, int.class);

        overRanges(values, (from, num) -> {
            Object actual = minMethod.invoke(buffer, from, num);

            Object[] sorted = copyAndSortNullLow(values, from, num);

            if (sorted.length != 0) {
                assertEquals(sorted[0], actual);
            } else {
                assertEquals(maxNum(), actual);
            }
        });
    }

    private void max() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var maxMethod = bufferClass.getMethod("max", int.class, int.class);

        overRanges(values, (from, num) -> {
            Object actual = maxMethod.invoke(buffer, from, num);

            Object[] sorted = copyAndSortNullHigh(values, from, num);
            Collections.reverse(Arrays.asList(sorted));

            if (sorted.length != 0) {
                assertEquals(sorted[0], actual);
            } else {
                assertEquals(minNum(), actual);
            }
        });
    }

    private void maxNL() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var maxMethod = bufferClass.getMethod("maxNL", int.class, int.class);

        overRanges(values, (from, num) -> {
            Object actual = maxMethod.invoke(buffer, from, num);

            Object[] sorted = copyAndSortNullLow(values, from, num);
            Collections.reverse(Arrays.asList(sorted));

            if (sorted.length != 0) {
                assertEquals(sorted[0], actual);
            } else if (mValueType.isNullable()) {
                assertNull(actual);
            } else {
                assertEquals(minNum(), actual);
            }
        });
    }

    /**
     * @return null of overflowed
     */
    private Object sum(Object[] values, int from, int num) {
        Class<?> clazz = mValueType.unboxedType();

        if (mValueType.isUnsignedInteger()) {
            if (clazz == byte.class) {
                long sum = 0;
                for (int i=from; i<from+num; i++) {
                    var value = (Byte) values[i];
                    if (value != null) {
                        sum += value & 0xffL;
                    }
                }
                return (sum & ~0xff) == 0 ? (byte) sum : null;
            } else if (clazz == short.class) {
                long sum = 0;
                for (int i=from; i<from+num; i++) {
                    var value = (Short) values[i];
                    if (value != null) {
                        sum += value & 0xffffL;
                    }
                }
                return (sum & ~0xffff) == 0 ? (short) sum : null;
            } else if (clazz == int.class) {
                long sum = 0;
                for (int i=from; i<from+num; i++) {
                    var value = (Integer) values[i];
                    if (value != null) {
                        sum += value & 0xffff_ffffL;
                    }
                }
                return (sum & ~0xffff_ffffL) == 0 ? (int) sum : null;
            } else if (clazz == long.class) {
                BigInteger sum = BigInteger.ZERO;
                for (int i=from; i<from+num; i++) {
                    var value = (Long) values[i];
                    if (value != null) {
                        BigInteger addend = BigInteger.valueOf(value >>> 1).shiftLeft(1);
                        if ((value & 1) != 0) {
                            addend = addend.add(BigInteger.ONE);
                        }
                        sum = sum.add(addend);
                    }
                }
                if (sum.compareTo(new BigInteger("ffffffffffffffff", 16)) > 0) {
                    return null;
                }
                return sum.longValue();
            } else {
                throw new AssertionError();
            }
        }

        if (clazz == byte.class) {
            byte sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Byte) values[i];
                if (value != null) {
                    try {
                        sum = Arithmetic.Byte.addExact(sum, value);
                    } catch (ArithmeticException e) {
                        return null;
                    }
                }
            }
            return sum;
        } else if (clazz == short.class) {
            short sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Short) values[i];
                if (value != null) {
                    try {
                        sum = Arithmetic.Short.addExact(sum, value);
                    } catch (ArithmeticException e) {
                        return null;
                    }
                }
            }
            return sum;
        } else if (clazz == int.class) {
            int sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Integer) values[i];
                if (value != null) {
                    try {
                        sum = Math.addExact(sum, value);
                    } catch (ArithmeticException e) {
                        return null;
                    }
                }
            }
            return sum;
        } else if (clazz == long.class) {
            long sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Long) values[i];
                if (value != null) {
                    try {
                        sum = Math.addExact(sum, value);
                    } catch (ArithmeticException e) {
                        return null;
                    }
                }
            }
            return sum;
        } else if (clazz == float.class) {
            float sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Float) values[i];
                if (value != null) {
                    sum += value;
                }
            }
            return sum;
        } else if (clazz == double.class) {
            double sum = 0;
            for (int i=from; i<from+num; i++) {
                var value = (Double) values[i];
                if (value != null) {
                    sum += value;
                }
            }
            return sum;
        } else if (clazz == BigInteger.class) {
            BigInteger sum = BigInteger.ZERO;
            for (int i=from; i<from+num; i++) {
                var value = (BigInteger) values[i];
                if (value != null) {
                    sum = sum.add(value);
                }
            }
            return sum;
        } else if (clazz == BigDecimal.class) {
            BigDecimal sum = BigDecimal.ZERO;
            for (int i=from; i<from+num; i++) {
                var value = (BigDecimal) values[i];
                if (value != null) {
                    sum = sum.add(value);
                }
            }
            return sum;
        } else {
            throw new AssertionError();
        }
    }

    private void sum() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var sumMethod = bufferClass.getMethod("sum", int.class, int.class);

        overRanges(values, (from, num) -> {
            Object expect = sum(values, from, num);

            try {
                Object actual = sumMethod.invoke(buffer, from, num);
                assertEquals(expect, actual);
            } catch (InvocationTargetException e) {
                if (!(e.getCause() instanceof ArithmeticException ae)) {
                    throw e;
                }
                assertTrue(ae.getMessage().contains("overflow"));
                assertNull(expect);
            }
        });
    }

    private Object divide(Object sum, int count) {
        Class<?> clazz = mValueType.unboxedType();

        if (mValueType.isUnsignedInteger()) {
            if (sum instanceof Byte v) {
                return (byte) ((v & 0xff) / count);
            } else if (sum instanceof Short v) {
                return (short) ((v & 0xffff) / count);
            } else if (sum instanceof Integer v) {
                return (int) ((v & 0xffff_ffffL) / count);
            } else if (sum instanceof Long v) {
                BigInteger bi = BigInteger.valueOf(v >>> 1).shiftLeft(1);
                if ((v & 1) != 0) {
                    bi = bi.add(BigInteger.ONE);
                }
                return bi.divide(BigInteger.valueOf(count)).longValue();
            } else {
                throw new AssertionError();
            }
        } else {
            if (sum instanceof Byte v) {
                return (byte) (v / count);
            } else if (sum instanceof Short v) {
                return (short) (v / count);
            } else if (sum instanceof Integer v) {
                return v / count;
            } else if (sum instanceof Long v) {
                return v / count;
            } else if (sum instanceof Float v) {
                return v / count;
            } else if (sum instanceof Double v) {
                return v / count;
            } else if (sum instanceof BigInteger v) {
                return v.divide(BigInteger.valueOf(count));
            } else if (sum instanceof BigDecimal v) {
                return v.divide(BigDecimal.valueOf(count), MathContext.DECIMAL64);
            } else {
                throw new AssertionError();
            }
        }
    }

    private void average() throws Exception {
        Class<?> bufferClass = ValueBuffer.forType(mValueType);
        Object[] values = values(mRnd.nextInt(10, 20));
        Object buffer = fill(bufferClass, values);

        var sumMethod = bufferClass.getMethod("sum", int.class, int.class);
        var countMethod = bufferClass.getMethod("count", int.class, int.class);
        var averageMethod = bufferClass.getMethod("average", int.class, int.class);

        overRanges(values, (from, num) -> {
            int count = (int) countMethod.invoke(buffer, from, num);

            try {
                Object sum = sumMethod.invoke(buffer, from, num);
                Object actual = averageMethod.invoke(buffer, from, num);

                if (count == 0) {
                    if (mValueType.isNullable()) {
                        assertNull(actual);
                    } else {
                        assertEquals("NaN", actual.toString());
                    }
                } else if (count == 1) {
                    if (actual instanceof BigDecimal bd) {
                        assertEquals(bd.divide(BigDecimal.ONE, MathContext.DECIMAL64), actual);
                    } else {
                        assertEquals(sum, actual);
                    }
                } else {
                    Object expect = divide(sum, count);
                    assertEquals(expect, actual);
                }
            } catch (InvocationTargetException e) {
                if (!(e.getCause() instanceof ArithmeticException ae)) {
                    throw e;
                }
                if (ae.getMessage().contains("zero")) {
                    assertEquals(0, count);
                    assertTrue(!mValueType.isNullable());
                } else {
                    assertTrue(ae.getMessage().contains("overflow"));
                    assertNull(sum(values, from, num));
                }
            }
        });
    }
}
