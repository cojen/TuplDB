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
import java.math.BigInteger;
import java.math.MathContext;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import java.util.function.Consumer;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.core.Utils;

/**
 * Defines a growable circular buffer of values, used by window functions. For efficiency, no
 * special bounds checking operations are performed.
 *
 * @author Brian S. O'Neill
 */
public abstract class ValueBuffer<V> {
    private static final Map<Class, Class> mRegistry = new ConcurrentHashMap<>();
    private static final Map<Class, Class> mUnsignedRegistry = new ConcurrentHashMap<>();

    /**
     * Returns a value buffer class suitable for storing the given value type. The class has
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

    public abstract int size();

    /**
     * Clear and add one value.
     */
    public abstract void init(V value);

    public abstract void clear();

    /**
     * Add one value to the end.
     */
    public abstract void add(V value);

    /**
     * Returns a value at the given index, where zero always represents the first value.
     */
    public abstract V get(int index);

    /**
     * Remove the given amount of values from the first.
     */
    public abstract void remove(int amount);

    /**
     * Removes and returns the first value.
     */
    public abstract V removeFirst();

    /**
     * Returns the count of non-null values over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to count
     */
    public abstract int count(int from, int num);

    /**
     * Returns the minimum value over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to examine
     */
    public final V min(int from, int num) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the minimum value over the given range, treating nulls as low instead of high.
     *
     * @param from inclusive first index
     * @param num number of values to examine
     */
    public final V minNL(int from, int num) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to examine
     */
    public final V max(int from, int num) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the maximum value over the given range, treating nulls as low instead of high.
     *
     * @param from inclusive first index
     * @param num number of values to examine
     */
    public final V maxNL(int from, int num) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the sum of non-null values over the given range.
     *
     * @param from inclusive first index
     * @param num number of values to sum together
     * @return a non-null value
     * @throws ArithmeticException if overflows
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
     * @throws ArithmeticException if overflows, or if division by zero cannot be represented
     */
    public final V average(int from, int num) {
        throw new UnsupportedOperationException();
    }

    static boolean hasNull(Object[] values, int first, int num) {
        first &= (values.length - 1);
        int end = first + num;
        if (end > values.length) {
            while (first < values.length) {
                if (values[first++] == null) {
                    return true;
                }
            }
            end -= first;
            first = 0;
        }
        while (first < end) {
            if (values[first++] == null) {
                return true;
            }
        }
        return false;
    }

    static int count(Object[] values, int first, int num) {
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

    private static Class generateClass(Type type) {
        Class clazz = type.clazz();
        Class arrayClass = clazz.arrayType();

        ClassMaker cm = ClassMaker.begin(ValueBuffer.class.getName(), MethodHandles.lookup());

        cm.public_();

        boolean addBridges = false;

        if (!type.isNumber() && !type.isPrimitive()) {
            cm.extend(ValueBuffer.class);
            addBridges = true;
        }

        cm.addField(arrayClass, "values").private_();
        cm.addField(int.class, "first").private_();
        cm.addField(int.class, "size").private_();

        {
            MethodMaker mm = cm.addConstructor(int.class).public_();
            mm.invokeSuperConstructor();
            var initialCapacityVar = mm.var(Utils.class).invoke("roundUpPower2", mm.param(0));
            mm.field("values").set(mm.new_(arrayClass, initialCapacityVar));
        }

        {
            MethodMaker mm = cm.addMethod(int.class, "size").public_().final_();
            mm.return_(mm.field("size"));
        }

        {
            MethodMaker mm = cm.addMethod(null, "init", clazz).public_().final_();
            mm.field("values").aset(0, mm.param(0));
            mm.field("first").set(0);
            mm.field("size").set(1);

            if (addBridges) {
                mm = cm.addMethod(null, "init", Object.class).public_().final_().bridge();
                mm.this_().invoke(void.class, "init", null, mm.param(0).cast(clazz));
            }
        }

        {
            MethodMaker mm = cm.addMethod(null, "clear").public_().final_();
            mm.field("size").set(0);
        }

        {
            MethodMaker mm = cm.addMethod
                (arrayClass, "expand", arrayClass, int.class).private_().static_();
            var valuesVar = mm.param(0);
            var firstVar = mm.param(1);
            var newValuesVar = mm.new_(arrayClass, valuesVar.alength().shl(1));
            var sys = mm.var(System.class);
            var lenVar = valuesVar.alength().sub(firstVar);
            sys.invoke("arraycopy", valuesVar, firstVar, newValuesVar, 0, lenVar);
            sys.invoke("arraycopy", valuesVar, 0, newValuesVar, lenVar, firstVar);
            mm.return_(newValuesVar);
        }

        {
            MethodMaker mm = cm.addMethod(null, "add", clazz).public_().final_();

            var valuesField = mm.field("values");
            var firstField = mm.field("first");
            var sizeField = mm.field("size");

            var valuesVar = valuesField.get();
            var sizeVar = sizeField.get();

            sizeVar.ifGe(valuesVar.alength(), () -> {
                valuesVar.set(valuesVar.methodMaker().invoke("expand", valuesVar, firstField));
                valuesField.set(valuesVar);
                firstField.set(0);
            });

            valuesVar.aset(ixVar(valuesVar, firstField, sizeVar), mm.param(0));
            sizeField.set(sizeVar.add(1));

            if (addBridges) {
                mm = cm.addMethod(null, "add", Object.class).public_().final_().bridge();
                mm.this_().invoke(void.class, "add", null, mm.param(0).cast(clazz));
            }
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "get", int.class).public_().final_();
            var valuesVar = mm.field("values").get();
            var firstField = mm.field("first");
            mm.return_(valuesVar.aget(ixVar(valuesVar, firstField, mm.param(0))));

            if (addBridges) {
                mm = cm.addMethod(Object.class, "get", int.class).public_().final_().bridge();
                mm.return_(mm.this_().invoke(clazz, "get", null, mm.param(0)));
            }
        }

        {
            MethodMaker mm = cm.addMethod(null, "remove", int.class).public_().final_();
            var amountVar = mm.param(0);
            var valuesField = mm.field("values");
            var firstField = mm.field("first");
            var sizeField = mm.field("size");
            firstField.set(ixVar(valuesField, firstField, amountVar));
            sizeField.set(sizeField.sub(amountVar));
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "removeFirst").public_().final_();
            var firstField = mm.field("first");
            var valuesVar = mm.field("values").get();
            var firstVar = firstField.get();
            firstField.set(ixVar(valuesVar, firstVar, 1));
            mm.field("size").inc(-1);
            mm.return_(valuesVar.aget(ixVar(valuesVar, firstVar, null)));

            if (addBridges) {
                mm = cm.addMethod(Object.class, "removeFirst").public_().final_().bridge();
                mm.return_(mm.this_().invoke(clazz, "removeFirst", null));
            }
        }

        {
            MethodMaker mm = cm.addMethod
                (int.class, "count", int.class, int.class).public_().final_();
            var fromVar = mm.param(0);
            var numVar = mm.param(1);
            if (clazz.isPrimitive()) {
                mm.return_(numVar);
            } else {
                var valuesField = mm.field("values");
                mm.return_(mm.var(ValueBuffer.class).invoke
                           ("count", valuesField, mm.field("first").add(fromVar), numVar));
            }
        }

        if (type.isNumber()) {
            addNumericalMethods(cm, type);
        }

        return cm.finish();
    }

    /**
     * @param offset can be null for 0
     */
    private static Variable ixVar(Variable valuesVar, Variable firstVar, Object offset) {
        if (offset != null) {
            firstVar = firstVar.add(offset);
        }
        return firstVar.and(valuesVar.alength().sub(1));
    }

    private static void addNumericalMethods(ClassMaker cm, Type type) {
        Class clazz = type.clazz();
        Class unboxed = type.unboxedType();

        {
            MethodMaker mm = cm.addMethod(unboxed, "sum", int.class, int.class).public_().final_();

            var sumVar = mm.var(unboxed);

            if (!Arithmetic.zero(sumVar)) {
                throw new AssertionError();
            }

            makeLoop(mm, type, true, addendVar -> {
                sumVar.set(Arithmetic.eval(type, Token.T_PLUS, sumVar, addendVar));
            });

            mm.return_(sumVar);
        }

        {
            MethodMaker mm = cm.addMethod
                (clazz, "average", int.class, int.class).public_().final_();

            Variable sumVar;
            Variable countVar;

            if (clazz.isPrimitive()) {
                // Since there's no nulls, no need to count them up.
                countVar = mm.param(1);
                sumVar = mm.invoke("sum", mm.param(0), countVar);
            } else {
                sumVar = mm.var(unboxed);

                if (!Arithmetic.zero(sumVar)) {
                    throw new AssertionError();
                }

                countVar = mm.var(int.class).set(0);

                makeLoop(mm, type, true, addendVar -> {
                    sumVar.set(Arithmetic.eval(type, Token.T_PLUS, sumVar, addendVar));
                    countVar.inc(1);
                });

                countVar.ifEq(0, () -> mm.return_(null));
            }

            Variable divisorVar;

            if (clazz == BigDecimal.class) {
                divisorVar = mm.var(clazz).invoke("valueOf", countVar);
                mm.return_(sumVar.invoke("divide", divisorVar,
                                         mm.var(MathContext.class).field("DECIMAL64")));
            } else {
                if (clazz == BigInteger.class) {
                    divisorVar = mm.var(clazz).invoke("valueOf", countVar);
                } else {
                    divisorVar = countVar.cast(unboxed);
                }
                mm.return_(Arithmetic.eval(type, Token.T_DIV, sumVar, divisorVar));
            }
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "min", int.class, int.class).public_().final_();
            var minVar = mm.var(clazz);

            if (clazz.isPrimitive()) {
                minVar.set(Arithmetic.max(type));

                makeLoop(mm, type, true, valueVar -> {
                    minVar.set(Arithmetic.min(type, minVar, valueVar));
                });
            } else {
                minVar.set(null); // null is the absolute max

                makeLoop(mm, type, true, valueVar -> {
                    minVar.ifEq(null, () -> minVar.set(valueVar),
                                () -> minVar.set(Arithmetic.min(type, minVar, valueVar)));
                });
            }

            mm.return_(minVar);
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "maxNL", int.class, int.class).public_().final_();
            var maxVar = mm.var(clazz);

            if (clazz.isPrimitive()) {
                maxVar.set(Arithmetic.min(type));

                makeLoop(mm, type, true, valueVar -> {
                    maxVar.set(Arithmetic.max(type, maxVar, valueVar));
                });
            } else {
                maxVar.set(null); // null is the absolute min

                makeLoop(mm, type, true, valueVar -> {
                    maxVar.ifEq(null, () -> maxVar.set(valueVar),
                                () -> maxVar.set(Arithmetic.max(type, maxVar, valueVar)));
                });
            }

            mm.return_(maxVar);
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "minNL", int.class, int.class).public_().final_();
            var minVar = mm.var(clazz);
            Object maxVal = Arithmetic.max(type);

            if (maxVal == null) {
                // Need to treat nulls specially because there's no MAX_VALUE.
                var fromVar = mm.param(0);
                var numVar = mm.param(1);
                var valuesVar = mm.field("values").get();
                mm.var(ValueBuffer.class).invoke
                    ("hasNull", valuesVar, mm.field("first").add(fromVar), numVar)
                    .ifTrue(() -> minVar.set(null), // null is the absolute min
                            () -> minVar.set(mm.invoke("min", fromVar, numVar)));
            } else if (clazz.isPrimitive()) {
                minVar.set(mm.invoke("min", mm.param(0), mm.param(1)));
            } else {
                minVar.set(maxVal);

                makeLoop(mm, type, false, valueVar -> {
                    valueVar.ifEq(null, () -> mm.return_(null)); // null is the absolute min
                    minVar.set(Arithmetic.min(type, minVar, valueVar));
                });
            }

            mm.return_(minVar);
        }

        {
            MethodMaker mm = cm.addMethod(clazz, "max", int.class, int.class).public_().final_();
            var maxVar = mm.var(clazz);
            Object minVal = Arithmetic.min(type);

            if (minVal == null) {
                // Need to treat nulls specially because there's no MIN_VALUE.
                var fromVar = mm.param(0);
                var numVar = mm.param(1);
                var valuesVar = mm.field("values").get();
                mm.var(ValueBuffer.class).invoke
                    ("hasNull", valuesVar, mm.field("first").add(fromVar), numVar)
                    .ifTrue(() -> maxVar.set(null), // null is the absolute max
                            () -> maxVar.set(mm.invoke("maxNL", fromVar, numVar)));
            } else if (clazz.isPrimitive()) {
                maxVar.set(mm.invoke("maxNL", mm.param(0), mm.param(1)));
            } else {
                maxVar.set(minVal);

                makeLoop(mm, type, false, valueVar -> {
                    valueVar.ifEq(null, () -> mm.return_(null)); // null is the absolute max
                    maxVar.set(Arithmetic.max(type, maxVar, valueVar));
                });
            }

            mm.return_(maxVar);
        }
    }

    /**
     * @param mm params must be (int from, int num)
     * @param skipNulls when true, the op never receives null values
     * @param op accepts a value from the buffer
     */
    private static void makeLoop(MethodMaker mm, Type type, boolean skipNulls,
                                 Consumer<Variable> op)
    {
        /*
          <type>[] values = this.values;
          int first = (this.first + from) & (values.length - 1); // see ixVar
          int end = first + num;
          if (end > values.length) {
              while (first < values.length) {
                  <type> value = values[first++];
                  if (!skipNulls || value != null) {
                      <op>(value)
                  }
              }
              end -= first;
              first = 0;
          }
          while (first < end) {
              <type> value = values[first++];
              if (!skipNulls || value != null) {
                  <op>(value)
              }
          }
        */

        Class<?> clazz = type.clazz();

        var fromVar = mm.param(0);
        var numVar = mm.param(1);

        var valuesVar = mm.field("values").get();
        var firstVar = ixVar(valuesVar, mm.field("first"), fromVar);
        var endVar = firstVar.add(numVar);

        Label next = mm.label();
        endVar.ifLe(valuesVar.alength(), next);

        {
            Label startLabel = mm.label().here();
            Label endLabel = mm.label();
            firstVar.ifGe(valuesVar.alength(), endLabel);
            var valueVar = valuesVar.aget(firstVar);
            firstVar.inc(1);
            if (skipNulls && !clazz.isPrimitive()) {
                valueVar.ifEq(null, startLabel);
            }
            op.accept(valueVar);
            startLabel.goto_();
            endLabel.here();
            endVar.set(endVar.sub(firstVar));
            firstVar.set(0);
        }

        next.here();

        {
            Label startLabel = mm.label().here();
            Label endLabel = mm.label();
            firstVar.ifGe(endVar, endLabel);
            var valueVar = valuesVar.aget(firstVar);
            firstVar.inc(1);
            if (skipNulls && !clazz.isPrimitive()) {
                valueVar.ifEq(null, startLabel);
            }
            op.accept(valueVar);
            startLabel.goto_();
            endLabel.here();
        }
    }
}
