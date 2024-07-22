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

import java.lang.invoke.MethodHandles;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.function.BiFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a growable circular buffer of values, which act upon moving ranges of values known
 * as frames. The implementations of the sum and average methods perform a full calculation
 * over each range rather than performing incremental calculations. Although less efficient,
 * it's more accurate. If the frame is constant, consider performing incremental calculations
 * instead.
 *
 * @author Brian S. O'Neill
 */
public abstract class WindowBuffer<V> extends ValueBuffer<V> {
    private static final Map<Class, Class> mRegistry = new ConcurrentHashMap<>();
    private static final Map<Class, Class> mUnsignedRegistry = new ConcurrentHashMap<>();

    /**
     * Returns a window buffer class suitable for storing the given value type. The class has
     * one constructor which specifies the initial buffer capacity, and numerical operations
     * are only supported for numerical value types.
     */
    public static Class<?> forType(Type type) {
        Map<Class, Class> registry = type.isUnsignedInteger() ? mUnsignedRegistry : mRegistry;

        Class<?> clazz = type.clazz();
        Class<?> bufferClass = registry.get(clazz);

        if (bufferClass == null) {
            synchronized (registry) {
                bufferClass = registry.get(clazz);
                if (bufferClass == null) {
                    bufferClass = generateClass(type);
                    registry.put(clazz, bufferClass);
                }
            }
        }

        return bufferClass;
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
    public abstract void begin(V value);

    /**
     * Append a value and extend the end of the range.
     */
    public abstract void append(V value);

    /**
     * Determine if enough values have been appended such that a complete calculation is
     * possible. If the frame end is constantly open (> max int), then this method would always
     * return false.
     *
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public abstract boolean ready(long frameEnd);

    /**
     * Increment the current row by one. Should only be called when the frame start is
     * constantly open (< min int) or is never a constant. It should be noted that an open
     * range never removes anything from the buffer, and so a buffer probably shouldn't be used
     * at all.
     */
    public abstract void advance();

    /**
     * Increment the current row by one, and remove a value if possible. If the frame start is
     * constantly greater than or equal to zero, then the zero-arg variant of this method
     * should be called.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     */
    public abstract void advanceAndRemove(long frameStart);

    /**
     * Increment the current row by one, and remove a value. This is a simplified variant of
     * the advanceAndRemove method which should only be called when the frame start is
     * constantly greater than or equal to zero.
     */
    public abstract void advanceAndRemove();

    // FIXME: Define advanceAndRemoveGet variants, to be used with incremental modes.

    /**
     * Returns the number of non-null values which are available over the given range.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public abstract int frameCount(long frameStart, long frameEnd);

    /**
     * Returns the minimum value over the given range. If possible, returns null if the
     * effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMin(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the minimum value over the given range. Returns null if the effective range is
     * empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMinOrNull(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the minimum value over the given range, treating nulls as low instead of high.
     * If possible, returns null if the effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMinNL(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the minimum value over the given range, treating nulls as low instead of high.
     * Returns null if the effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMinOrNullNL(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range. If possible, returns null if the
     * effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMax(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range. Returns null if the effective range is
     * empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMaxOrNull(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range, treating nulls as low instead of high.
     * If possible, returns null if the effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMaxNL(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range, treating nulls as low instead of high.
     * Returns null if the effective range is empty.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public V frameMaxOrNullNL(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
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
     * null or zero, then the average is null (or NaN if cannot be null). A value of zero is
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
    public V frameAverageOrNull(long frameStart, long frameEnd) {
        throw new UnsupportedOperationException();
    }

    private static Class generateClass(Type type) {
        Class clazz = type.clazz();

        ClassMaker cm = ClassMaker.begin(WindowBuffer.class.getName(), MethodHandles.lookup());

        cm.public_().extend(ValueBuffer.forType(type));

        // Define fields which track the range of values that the buffer has, relative to the
        // current row. Start and end bounds are inclusive.
        cm.addField(int.class, "start").private_();
        cm.addField(int.class, "end").private_();

        {
            MethodMaker mm = cm.addConstructor(int.class).public_();
            mm.invokeSuperConstructor(mm.param(0));
        }

        {
            MethodMaker mm = cm.addMethod(null, "begin", clazz).public_().final_();
            mm.invoke("init", mm.param(0));
            mm.field("start").set(0);
            mm.field("end").set(0);
        }

        {
            MethodMaker mm = cm.addMethod(null, "append", clazz).public_().final_();
            mm.invoke("add", mm.param(0));
            mm.field("end").inc(1);
        }

        {
            MethodMaker mm = cm.addMethod(boolean.class, "ready", long.class).public_().final_();
            mm.return_(mm.field("end").ge(mm.param(0)));
        }

        {
            MethodMaker mm = cm.addMethod(null, "advance").public_().final_();
            mm.field("start").inc(-1);
            mm.field("end").inc(-1);
        }

        {
            MethodMaker mm = cm.addMethod(null, "advanceAndRemove", long.class).public_().final_();

            var frameStartVar = mm.param(0);

            var startField = mm.field("start");
            var endField = mm.field("end");

            var startVar = startField.sub(1);
            var endVar = endField.sub(1);
 
            frameStartVar.ifGt(startVar, () -> {
                endVar.ifGe(startVar, () -> mm.invoke("remove", 1));
                startVar.inc(1);
            });

            startField.set(startVar);
            endField.set(endVar);
        }

        {
            MethodMaker mm = cm.addMethod(null, "advanceAndRemove").public_().final_();
            mm.field("end").inc(-1);
            mm.invoke("remove", 1);
        }

        {
            MethodMaker mm = cm.addMethod
                (int.class, "frameCount", long.class, long.class).public_().final_();
            makeFrameCode(mm, 0, "count");
        }

        if (type.isNumber()) {
            addNumericalMethods(cm, type);
        }

        return cm.finish();
    }

    private static void addNumericalMethods(ClassMaker cm, Type type) {
        Class clazz = type.clazz();
        Class unboxed = type.unboxedType();
        Class boxed = type.boxedType();

        {
            MethodMaker mm = cm.addMethod
                (unboxed, "frameSum", long.class, long.class).public_().final_();

            var emptyResultVar = mm.var(unboxed);

            if (!Arithmetic.zero(emptyResultVar)) {
                throw new AssertionError();
            }

            makeFrameCode(mm, emptyResultVar, "sum");
        }

        {
            Class resultType;
            Object emptyResult;

            if (clazz == float.class) {
                resultType = clazz;
                emptyResult = Float.NaN;
            } else if (clazz == double.class) {
                resultType = clazz;
                emptyResult = Double.NaN;
            } else {
                resultType = boxed;
                emptyResult = null;
            }

            MethodMaker mm = cm.addMethod
                (resultType, "frameAverage", long.class, long.class).public_().final_();

            makeFrameCode(mm, emptyResult, "average");
        }

        {
            MethodMaker mm = cm.addMethod
                (boxed, "frameAverageOrNull", long.class, long.class).public_().final_();

            if (clazz != float.class && clazz != double.class) {
                mm.return_(mm.invoke("frameAverage", mm.param(0), mm.param(1)));
            } else {
                makeFrameCode(mm, null, "average");
            }
        }

        {
            MethodMaker mm = cm.addMethod
                (clazz, "frameMin", long.class, long.class).public_().final_();
            Object emptyResult = clazz.isPrimitive() ? Arithmetic.max(type) : null;
            makeFrameCode(mm, emptyResult, "min");
        }

        {
            MethodMaker mm = cm.addMethod
                (boxed, "frameMinOrNull", long.class, long.class).public_().final_();
            makeFrameCode(mm, null, "min");
        }

        {
            MethodMaker mm = cm.addMethod
                (clazz, "frameMaxNL", long.class, long.class).public_().final_();
            Object emptyResult = clazz.isPrimitive() ? Arithmetic.min(type) : null;
            makeFrameCode(mm, emptyResult, "maxNL");
        }

        {
            MethodMaker mm = cm.addMethod
                (boxed, "frameMaxOrNullNL", long.class, long.class).public_().final_();
            makeFrameCode(mm, null, "maxNL");
        }

        {
            MethodMaker mm = cm.addMethod
                (clazz, "frameMinNL", long.class, long.class).public_().final_();

            if (clazz.isPrimitive()) {
                mm.return_(mm.invoke("frameMin", mm.param(0), mm.param(1)));
            } else {
                Object emptyResult = clazz.isPrimitive() ? Arithmetic.max(type) : null;
                makeFrameCode(mm, emptyResult, "minNL");
            }
        }

        {
            MethodMaker mm = cm.addMethod
                (boxed, "frameMinOrNullNL", long.class, long.class).public_().final_();

            if (clazz.isPrimitive()) {
                mm.return_(mm.invoke("frameMinOrNull", mm.param(0), mm.param(1)));
            } else {
                makeFrameCode(mm, null, "minNL");
            }
        }

        {
            MethodMaker mm = cm.addMethod
                (clazz, "frameMax", long.class, long.class).public_().final_();

            if (clazz.isPrimitive()) {
                mm.return_(mm.invoke("frameMaxNL", mm.param(0), mm.param(1)));
            } else {
                Object emptyResult = clazz.isPrimitive() ? Arithmetic.min(type) : null;
                makeFrameCode(mm, emptyResult, "max");
            }
        }

        {
            MethodMaker mm = cm.addMethod
                (boxed, "frameMaxOrNull", long.class, long.class).public_().final_();

            if (clazz.isPrimitive()) {
                mm.return_(mm.invoke("frameMaxOrNullNL", mm.param(0), mm.param(1)));
            } else {
                makeFrameCode(mm, null, "max");
            }
        }
    }

    /**
     * @param mm params must be (long frameStart, long frameEnd)
     * @param emptyResult to be returned from the method when the effective count is <= 0
     * @param method accepts an int "from" and "num" variables and computes a result to be returned
     */
    private static void makeFrameCode(MethodMaker mm, Object emptyResult, String method) {
        makeFrameCode(mm, emptyResult, (fromVar, numVar) -> mm.invoke(method, fromVar, numVar));
    }

    /**
     * @param mm params must be (long frameStart, long frameEnd)
     * @param emptyResult to be returned from the method when the effective count is <= 0
     * @param op accepts an int "from" and "num" variables and computes a result to be returned
     */
    private static void makeFrameCode(MethodMaker mm, Object emptyResult,
                                      BiFunction<Variable, Variable, Variable> op)
    {
        var frameStartVar = mm.param(0);
        var frameEndVar = mm.param(1);

        // Note that start is always 0 when frame start is always >= 0.
        var startVar = mm.field("start").get();

        var mathVar = mm.var(Math.class);
        frameStartVar.set(mathVar.invoke("max", frameStartVar, startVar));
        frameEndVar.set(mathVar.invoke("min", frameEndVar, mm.field("end")));

        var countVar = frameEndVar.sub(frameStartVar).add(1);

        // Note that count can be negative only for ranges that don't include the current
        // row (which is zero).
        countVar.ifLe(0, () -> mm.return_(emptyResult));

        var fromVar = frameStartVar.sub(startVar).cast(int.class);
        var numVar = countVar.cast(int.class);

        mm.return_(op.apply(fromVar, numVar));
    }
}

