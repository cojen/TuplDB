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

import java.math.BigDecimal;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.Converter;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

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

    static final int DEFAULT_MIN_CAPACITY = 8;

    /**
     * Calculates an initial buffer capacity.
     *
     * @param frameStart inclusive frame start
     * @param frameEnd inclusive frame end
     * @return a capacity clamped to [8..1024]
     */
    public static int capacityFor(long frameStart, long frameEnd) {
        return clampCapacity(frameEnd - frameStart + 1);
    }

    /**
     * Calculates an initial buffer capacity.
     *
     * @param groupSize minimum number of rows expected per group
     * @param groupEnd group range end value
     * @return a capacity clamped to [8..1024]
     */
    public static int capacityForGroup(int groupSize, long groupEnd) {
        return clampCapacity(groupSize * groupEnd);
    }

    /**
     * @return a capacity clamped to [8..1024]
     */
    public static int clampCapacity(long capacity) {
        return capacity <= 0 ? DEFAULT_MIN_CAPACITY : (int) Math.min(capacity, 1024);
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
     * Returns the inclusive buffer end position, relative to the current row (which is zero).
     * When the end is zero, then the current row is the end. When the end is positive, then
     * rows exist after the current row. The end can become negative when advance has been
     * called, kicking out the current row. The append method must be called to add back a
     * current row.
     */
    public abstract int end();

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

    /**
     * Update the frame start position and remove all values lower than the new position. This
     * method has no effect if the given position is less than or equal to the current start
     * position. The given frame start position is clamped such that it isn't larger than zero,
     * thus preventing the current row (and higher) from being removed.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     */
    public abstract void trimStart(long frameStart);

    /**
     * Finds a frame row position relative to the current row by counting groups. A group is
     * defined by a cluster of rows which have the same value. The returned position references
     * the first row of the starting group.
     *
     * <p>Note: To work correctly, the buffered values must be grouped by value.
     *
     * @param delta Number of groups to skip relative to the current row. The delta is
     * typically less than or equal to zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MAX_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findGroupStart(long delta);

    /**
     * Finds a frame row position relative to the current row by counting groups. A group is
     * defined by a cluster of rows which have the same value. The returned position references
     * the last row of the ending group.
     *
     * <p>Note: To work correctly, the buffered values must be grouped by value.
     *
     * @param delta Number of groups to skip relative to the current row. The delta is
     * typically greater than or equal to zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MIN_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findGroupEnd(long delta);

    /**
     * Finds a frame row position whose value matches the current row value plus a delta value.
     * The returned position is the lowest whose value is greater than or equal to the
     * calculated range start value.
     *
     * <p>Note: To work correctly, the buffered values must have an ascending order.
     *
     * @param delta The range start value is calculated as the current value plus the delta.
     * The delta is typically less than zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MAX_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findRangeStartAsc(double delta);

    /**
     * Finds a frame row position whose value matches the current row value plus a delta value.
     * The returned position is the highest whose value is less than or equal to the calculated
     * range end value.
     *
     * <p>Note: To work correctly, the buffered values must have an ascending order.
     *
     * @param delta The range end value is calculated as the current value plus the delta. The
     * delta is typically greater than zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MIN_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findRangeEndAsc(double delta);

    /**
     * Finds a frame row position whose value matches the current row value plus a delta value.
     * The returned position is the lowest whose value is less than or equal to the calculated
     * range start value.
     *
     * <p>Note: To work correctly, the buffered values must have a descending order.
     *
     * @param delta The range start value is calculated as the current value plus the delta.
     * The delta is typically less than zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MIN_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findRangeStartDesc(double delta);

    /**
     * Finds a frame row position whose value matches the current row value plus a delta value.
     * The returned position is the highest whose value is greater than or equal to the
     * calculated range end value.
     *
     * <p>Note: To work correctly, the buffered values must have a descending order.
     *
     * @param delta The range end value is calculated as the current value plus the delta. The
     * delta is typically greater than zero, but any value is allowed.
     * @return a frame position relative to the current row (which is zero), or MAX_VALUE if
     * out of bounds (effective range is empty)
     */
    public abstract long findRangeEndDesc(double delta);

    /**
     * Returns the number of non-null values which are available over the given range.
     *
     * @param frameStart inclusive frame start, relative to the current row (which is zero)
     * @param frameEnd inclusive frame end, relative to the current row (which is zero)
     */
    public abstract int frameCount(long frameStart, long frameEnd);

    /**
     * Returns a value at the given frame position. If the position is outside the effective
     * frame range, then a suitable default value is returned. See Converter.setDefault.
     *
     * @param framePos frame position relative to the current row (which is zero)
     */
    public abstract V frameGet(long framePos);

    /**
     * Returns a value at the given frame position. Returns null if the position outside the
     * effective frame range.
     *
     * @param framePos frame position relative to the current row (which is zero)
     */
    public abstract V frameGetOrNull(long framePos);

    /**
     * Returns a value at the given frame position. If the position is outside the effective
     * frame range, then first available value is returned. The caller is responsible for
     * ensuring that the buffer has at least one value.
     *
     * @param framePos frame position relative to the current row (which is zero)
     */
    public abstract V frameGetOrFirst(long framePos);

    /**
     * Returns a value at the given frame position. If the position is outside the effective
     * frame range, then first available value is returned. The caller is responsible for
     * ensuring that the buffer has at least one value.
     *
     * @param framePos frame position relative to the current row (which is zero)
     */
    public abstract V frameGetOrLast(long framePos);

    /**
     * Returns a value at the current row position. The caller is responsible for ensuring that
     * the buffer has a current row value.
     */
    public abstract V frameCurrent();

    /**
     * Returns the minimum value over the given range. If the effective range is empty, then
     * null is returned if permitted. If the result cannot be null, then the maximum numerical
     * value is returned when the effective range is empty.
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
     * Returns the maximum value over the given range. If the effective range is empty, then
     * null is returned if permitted. If the result cannot be null, then the minimum numerical
     * value is returned when the effective range is empty.
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
            MethodMaker mm = cm.addMethod(int.class, "end").public_().final_();
            mm.return_(mm.field("end"));
        }

        {
            MethodMaker mm = cm.addMethod(null, "advance").public_().final_();
            mm.field("start").dec(1);
            mm.field("end").dec(1);
        }

        {
            MethodMaker mm = cm.addMethod(null, "advanceAndRemove", long.class).public_().final_();

            /*
              int start = this.start - 1;
              int end = this.end - 1;
              if (frameStart > start) {
                  if (end >= start) {
                      remove(1);
                  }
                  start++;
              }
              this.start = start;
              this.end = end;
            */

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
            mm.field("end").dec(1);
            mm.invoke("remove", 1);
        }

        {
            MethodMaker mm = cm.addMethod(null, "trimStart", long.class).public_().final_();

            /*
              frameStart = Math.min(frameStart, 0L);
              long amt = frameStart - start;
              if (amt > 0) {
                  remove((int) amt);
                  start = (int) frameStart;
              }
             */

            var frameStartVar = mm.var(Math.class).invoke("min", mm.param(0), 0L);
            var startField = mm.field("start");
            var amtVar = frameStartVar.sub(startField);
            amtVar.ifGt(0, () -> {
                mm.invoke("remove", amtVar.cast(int.class));
                startField.set(frameStartVar.cast(int.class));
            });
        }

        {
            makeFindGroup(type, cm, true);
            makeFindGroup(type, cm, false);
        }

        if (type.isNumber()) {
            makeFindRange(type, cm, true, true);
            makeFindRange(type, cm, false, true);
            makeFindRange(type, cm, true, false);
            makeFindRange(type, cm, false, false);
        }

        {
            MethodMaker mm = cm.addMethod
                (int.class, "frameCount", long.class, long.class).public_().final_();
            makeFrameCode(mm, 0, "count");
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "frameGet", long.class).public_().final_();
            makeFrameGet(mm, "get");
            var emptyResultVar = mm.var(clazz);
            Converter.setDefault(mm, type, emptyResultVar);
            mm.return_(emptyResultVar);
        }

        {
            Class boxed = type.boxedType();
            MethodMaker mm = cm.addMethod(boxed, "frameGetOrNull", long.class).public_().final_();
            makeFrameGet(mm, "get");
            mm.return_(null);
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "frameGetOrFirst", long.class).public_().final_();

            /*
              long index = framePos - this.start;
              index = Math.max(0, index);
              return get((int) index);
            */

            var indexVar = mm.param(0).sub(mm.field("start"));
            indexVar.set(mm.var(Math.class).invoke("max", 0L, indexVar));
            mm.return_(mm.invoke("get", indexVar.cast(int.class)));
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "frameGetOrLast", long.class).public_().final_();

            /*
              framePos = Math.min(framePos, this.end);
              long index = framePos - this.start;
              return get((int) index);
            */

            var framePosVar = mm.var(Math.class).invoke("min", mm.param(0), mm.field("end"));
            var indexVar = framePosVar.sub(mm.field("start"));
            mm.return_(mm.invoke("get", indexVar.cast(int.class)));
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "frameCurrent").public_().final_();
            mm.return_(mm.invoke("get", mm.field("start").neg().cast(int.class)));
        }

        if (type.isNumber()) {
            addNumericalMethods(cm, type);
        }

        return cm.finish();
    }

    private static void makeFindGroup(Type type, ClassMaker cm, boolean forStart) {
        String name = "findGroup" + (forStart ? "Start" : "End");
        MethodMaker mm = cm.addMethod(long.class, name, long.class).public_().final_();

        final var deltaVar = mm.param(0);
        final var posVar = mm.var(long.class).set(0);
        final var startVar = mm.field("start").get();

        /*
          <op> is <= if forStart, >= if for end
          if (delta <op> 0) {
              // ...
          } else {
              // ...
          }
        */

        final Class<?> clazz = type.clazz();
        final var valueVar = mm.var(clazz);
        final var prevVar = mm.var(clazz);
        final var nextVar = mm.var(clazz);

        if (forStart) {
            deltaVar.ifLe(0, () -> {
                Label begin = mm.label();
                makeFindGroupPrev(posVar, startVar, begin, valueVar, prevVar, forStart);
                /*
                  delta++;
                  if (delta > 0) {
                      return pos + 1;
                  }
                  value = prev;
                */
                deltaVar.inc(1);
                deltaVar.ifGt(0, () -> mm.return_(posVar.add(1)));
                valueVar.set(prevVar);
                begin.goto_();
            },
            () -> { // else
                var endVar = mm.field("end").get();
                Label begin = mm.label();
                makeFindGroupNext(posVar, startVar, endVar, begin, valueVar, nextVar, forStart);
                /*
                  delta--;
                  if (delta <= 0) {
                      return pos;
                  }
                  value = next;
                */
                deltaVar.dec(1);
                deltaVar.ifLe(0, () -> mm.return_(posVar));
                valueVar.set(nextVar);
                begin.goto_();
            });
        } else {
            deltaVar.ifGe(0, () -> {
                var endVar = mm.field("end").get();
                Label begin = mm.label();
                makeFindGroupNext(posVar, startVar, endVar, begin, valueVar, nextVar, forStart);
                /*
                  delta--;
                  if (delta < 0) {
                      return pos - 1;
                  }
                  value = next;
                */
                deltaVar.dec(1);
                deltaVar.ifLt(0, () -> mm.return_(posVar.sub(1)));
                valueVar.set(nextVar);
                begin.goto_();
            },
            () -> { // else
                Label begin = mm.label();
                makeFindGroupPrev(posVar, startVar, begin, valueVar, prevVar, forStart);
                /*
                  delta++;
                  if (delta >= 0) {
                      return pos;
                  }
                  value = prev;
                */
                deltaVar.inc(1);
                deltaVar.ifGe(0, () -> mm.return_(posVar));
                valueVar.set(prevVar);
                begin.goto_();
            });
        }
    }

    /**
     * @param posVar type is long
     * @param startVar type is long
     * @param begin is positioned by this method
     * @param valueVar type is V
     * @param prevVar type is V
     */
    private static void makeFindGroupPrev(Variable posVar, Variable startVar,
                                          Label begin, Variable valueVar, Variable prevVar,
                                          boolean forStart)
    {
        MethodMaker mm = posVar.methodMaker();

        /*
          long index = pos - start;
          if (index <= 0) {
              return forStart ? start : Long.MIN_VALUE;
          }
          value = get((int) index);
          begin: while (true) {
              pos--;
              index = pos - start;
              if (index < 0) {
                  return forStart ? start : Long.MIN_VALUE;
              }
              prev = get((int) index);
              if (prev == value) {
                  continue begin;
              }
              ...
        */

        var indexVar = posVar.sub(startVar);
        Label inBounds = mm.label();
        indexVar.ifGt(0, inBounds);
        mm.return_(forStart ? startVar : Long.MIN_VALUE);
        inBounds.here();
        valueVar.set(mm.invoke("get", indexVar.cast(int.class)));
        begin.here();
        posVar.dec(1);
        indexVar.set(posVar.sub(startVar));
        inBounds = mm.label();
        indexVar.ifGe(0, inBounds);
        mm.return_(forStart ? startVar : Long.MIN_VALUE);
        inBounds.here();
        prevVar.set(mm.invoke("get", indexVar.cast(int.class)));
        makeFindGroupCompare(prevVar, valueVar, begin);
    }

    /**
     * @param posVar type is long
     * @param startVar type is long
     * @param endVar type is long
     * @param valueVar type is V
     * @param nextVar type is V
     */
    private static void makeFindGroupNext(Variable posVar, Variable startVar, Variable endVar,
                                          Label begin, Variable valueVar, Variable nextVar,
                                          boolean forStart)
    {
        MethodMaker mm = posVar.methodMaker();

        /*
          if (pos >= end) {
              return forStart ? Long.MAX_VALUE : end;
          }
          V value = get((int) (pos - start));
          begin: while (true) {
              pos++;
              if (pos > end) {
                  return forStart ? Long.MAX_VALUE : end;
              }
              V next = get((int) (pos - start));
              if (next == value) {
                  continue begin;
              }
              ...
        */

        Label inBounds = mm.label();
        posVar.ifLt(endVar, inBounds);
        mm.return_(forStart ? Long.MAX_VALUE : endVar);
        inBounds.here();
        valueVar.set(mm.invoke("get", posVar.sub(startVar).cast(int.class)));
        begin.here();
        posVar.inc(1);
        inBounds = mm.label();
        posVar.ifLe(endVar, inBounds);
        mm.return_(forStart ? Long.MAX_VALUE : endVar);
        inBounds.here();
        nextVar.set(mm.invoke("get", posVar.sub(startVar).cast(int.class)));
        makeFindGroupCompare(nextVar, valueVar, begin);
    }

    /**
     * @param a type is V
     * @param b type is V
     * @param equal branch here when the values are equal
     */
    private static void makeFindGroupCompare(Variable a, Variable b, Label equal) {
        Class<?> clazz = a.classType();

        if (clazz.isPrimitive()) {
            a.ifEq(b, equal);
            return;
        }

        a.ifEq(null, () -> b.ifEq(null, equal), () -> {
            if (clazz == BigDecimal.class) {
                a.invoke("compareTo", b).ifEq(0, equal);
            } else {
                a.invoke("equals", b).ifTrue(equal);
            }
        });
    }

    private static void makeFindRange(Type type, ClassMaker cm,
                                      boolean forStart, boolean ascending)
    {
        String name = "findRange" + (forStart ? "Start" : "End") + (ascending ? "Asc" : "Desc");
        MethodMaker mm = cm.addMethod(long.class, name, double.class);
        mm.public_().final_();

        var deltaVar = mm.param(0);

        /*
          long index = -start;

          if (delta <deltaOp> 0) {
              ... forward scan ...
          } else {
              ... reverse scan ...
          }

          return index + start;
        */

        var startVar = mm.field("start");
        var indexVar = mm.var(long.class).set(startVar.neg());

        //int deltaOp = forStart ? (ascending ? OP_GT : OP_LE) : (ascending ? OP_GE : OP_LT);

        Label reverse = mm.label();

        if (forStart) {
            if (ascending) {
                deltaVar.ifLe(0, reverse);
            } else {
                deltaVar.ifGt(0, reverse);
            }
        } else {
            if (ascending) {
                deltaVar.ifLt(0, reverse);
            } else {
                deltaVar.ifGe(0, reverse);
            }
        }

        int cmpOp = forStart ? OP_GE : OP_GT;

        if (!ascending) {
            cmpOp = flipOperator(cmpOp);
        }

        makeFindRangeLoop(type, mm, deltaVar, indexVar, true, cmpOp);
        Label cont = mm.label().goto_();
        reverse.here();
        makeFindRangeLoop(type, mm, deltaVar, indexVar, false, flipOperator(cmpOp));

        cont.here();
        mm.return_(indexVar.add(startVar));
    }

    /**
     * @param deltaVar variable of double type
     */
    private static void makeFindRangeLoop(Type type, MethodMaker mm,
                                          Variable deltaVar, Variable indexVar,
                                          boolean forward, int cmpOp)
    {
        Label done = mm.label();

        var doubleType = doubleType(type);

        if (forward) {
            /*
              long endIndex = end - start;
              if (index < endIndex) {
                  double find = get(-start) + delta;                
                  do {
                      index++;
                      double value = get((int) index);
                      if (value <cmpOp> find) {
                          index--; // only when cmpOp doesn't have '='
                          goto done;
                      }
                  } while (index < endIndex);
              }
              return forward ? Long.MAX_VALUE : Long.MIN_VALUE; // only when cmpOp has '='
            */

            var endIndexVar = mm.field("end").sub(mm.field("start"));
            Label loopEnd = mm.label();
            indexVar.ifGe(endIndexVar, loopEnd);

            var findVar = makeFindVar(type, doubleType, deltaVar);

            Label doStart = mm.label().here();
            indexVar.inc(1);
            var valueVar = convert(type, mm.invoke("get", indexVar.cast(int.class)), doubleType);

            Label stop = mm.label();
            Label cont = mm.label();
            CompareUtils.compare(mm, doubleType, valueVar, doubleType, findVar, cmpOp, stop, cont);
            stop.here();

            if (!hasEqualComponent(cmpOp)) {
                indexVar.dec(1);
            }
            done.goto_();

            cont.here();
            indexVar.ifLt(endIndexVar, doStart);

            loopEnd.here();

            if (hasEqualComponent(cmpOp)) {
                mm.return_(forward ? Long.MAX_VALUE : Long.MIN_VALUE);
            }
        } else {
            /*
              if (index > 0) {
                  double find = get(-start) + delta;
                  do {
                      index--;
                      double value = get((int) index);
                      if (value <cmpOp> find) {
                          index++; // only when cmpOp doesn't have '='
                          goto done;
                      }
                  } while (index > 0);
              }
              return forward ? Long.MAX_VALUE : Long.MIN_VALUE; // only when cmpOp has '='
            */

            Label loopEnd = mm.label();
            indexVar.ifLe(0, loopEnd);

            var findVar = makeFindVar(type, doubleType, deltaVar);

            Label doStart = mm.label().here();
            indexVar.dec(1);
            var valueVar = convert(type, mm.invoke("get", indexVar.cast(int.class)), doubleType);

            Label stop = mm.label();
            Label cont = mm.label();
            CompareUtils.compare(mm, doubleType, valueVar, doubleType, findVar, cmpOp, stop, cont);
            stop.here();

            if (!hasEqualComponent(cmpOp)) {
                indexVar.inc(1);
            }
            done.goto_();

            cont.here();
            indexVar.ifGt(0, doStart);

            loopEnd.here();

            if (hasEqualComponent(cmpOp)) {
                mm.return_(forward ? Long.MAX_VALUE : Long.MIN_VALUE);
            }
        }

        done.here();
    }

    /**
     * @param deltaVar variable of double type
     * @return variable of type double or Double
     */
    private static Variable makeFindVar(Type type, Type doubleType, Variable deltaVar) {
        MethodMaker mm = deltaVar.methodMaker();
        final var findVar = convert(type, mm.invoke("get", mm.field("start").neg()), doubleType);

        if (!doubleType.isNullable()) {
            findVar.set(findVar.add(deltaVar));
        } else {
            Label skip = mm.label();
            findVar.ifEq(null, skip);
            findVar.set(findVar.add(deltaVar));
            skip.here();
        }

        return findVar;
    }

    private static Variable convert(ColumnInfo srcInfo, Variable srcVar, Type dstType) {
        MethodMaker mm = srcVar.methodMaker();
        Variable dstVar = mm.var(dstType.clazz());
        Converter.convertLossy(mm, srcInfo, srcVar, dstType, dstVar);
        return dstVar;
    }

    /**
     * Returns a double or nullable Double type.
     */
    private static Type doubleType(ColumnInfo info) {
        if (info.isNullable()) {
            return BasicType.make(Double.class, Type.TYPE_DOUBLE | Type.TYPE_NULLABLE);
        } else {
            return BasicType.make(double.class, Type.TYPE_DOUBLE);
        }
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
        /*
          int start = this.start;
          frameStart = Math.max(frameStart, start);
          frameEnd = Math.min(frameEnd, this.end);
          long count = frameEnd - frameStart + 1;
          if (count <= 0) {
              return <emptyResult>;
          }
          int from = (int) (frameStart - start);
          int num = (int) count;
          return <op>(from, num);
        */

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

    /**
     * @param mm params must be (long framePos)
     * @param method accepts an int "index" variable and computes a result to be returned,
     * unless the result would be empty
     */
    private static void makeFrameGet(MethodMaker mm, String method) {
        makeFrameGet(mm, indexVar -> mm.invoke(method, indexVar));
    }

    /**
     * @param mm params must be (long framePos)
     * @param op accepts an int "index" variable and computes a result to be returned, unless
     * the result would be empty
     */
    private static void makeFrameGet(MethodMaker mm, Function<Variable, Variable> op) {
        /*
          long index = framePos - this.start;
          if (index >= 0 && framePos <= this.end) {
              return <op>((int) index);
          }
          empty...
        */

        var framePosVar = mm.param(0);
        var indexVar = framePosVar.sub(mm.field("start"));
        Label empty = mm.label();
        indexVar.ifLt(0, empty);
        framePosVar.ifGt(mm.field("end"), empty);
        mm.return_(op.apply(indexVar.cast(int.class)));
        empty.here();
    }
}
