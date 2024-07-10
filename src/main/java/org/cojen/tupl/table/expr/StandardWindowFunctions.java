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

import java.util.List;
import java.util.Map;

import java.util.function.Consumer;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.table.expr.Type.*;

/**
 * Defines a collection of standard window functions. Many of them should only be constructed
 * by the standard aggregation functions, when the validate step sees that a named window
 * function parameter was provided. This effectively converts the aggregation function into a
 * window function.
 *
 * @author Brian S. O'Neill
 * @see StandardFunctionFinder
 */
final class StandardWindowFunctions {
    /**
     * Defines a window function which computes a moving average.
     */
    // FIXME: Only supports "row" type range. Need to support "range" and "groups" too.
    static class avg extends FunctionApplier.Grouped {
        private final Type mOriginalType;
        private final String mArgName;

        // Range start/end, if constant. See the endConstant and startConstant methods.
        private Long mStartConstant, mEndConstant;

        // References a Range instance. Is null if the start and are both constant.
        private String mRangeFieldName;

        // References a WindowBuffer instance.
        private String mBufferFieldName;

        // Tracks the number of remaining values that the step method must produce.
        private String mRemainingFieldName;

        /**
         * @param argName the named argument which refers to a Range
         */
        avg(Type type, Type originalType, String argName) {
            super(type);
            mOriginalType = originalType;
            mArgName = argName;
        }

        @Override
        public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                        Consumer<String> reasons)
        {
            // Should have been validated before being constructed.
            throw new AssertionError();
        }

        // FIXME: If the range could extend outside the window, then nulls are possible, so
        // update the type. Have the avg class do this?

        @Override
        public void begin(GroupContext context) {
            var valueVar = context.args().get(0).eval(true);

            LazyValue rangeVal = context.namedArgs().get(mArgName);

            mStartConstant = startConstant(rangeVal);
            mEndConstant = endConstant(rangeVal);

            if (mStartConstant == null || mEndConstant == null) {
                var rangeVar = rangeVal.eval(true);
                mRangeFieldName = context.newWorkField(Range.class).set(rangeVar).name();
            }

            final Class<?> bufferType;
            {
                Class<?> clazz = type().clazz();
                if (clazz == double.class) {
                    bufferType = WindowBuffer.OfDouble.class;
                } else if (clazz == Double.class) {
                    bufferType = WindowBuffer.OfDoubleObj.class;
                } else if (clazz == BigDecimal.class) {
                    bufferType = WindowBuffer.OfBigDecimal.class;
                } else {
                    // See StandardFunctionFinder.avg.
                    throw new AssertionError();
                }
            }

            var bufferField = context.newWorkField(bufferType, true, f -> {
                MethodMaker mm = f.methodMaker();
                Variable bufferVar;
                if (mStartConstant != null && mEndConstant != null) {
                    int capacity = WindowBuffer.capacityFor(mStartConstant, mEndConstant);
                    bufferVar = mm.new_(bufferType, capacity);
                } else {
                    var rangeVar = rangeVal.eval(true);
                    bufferVar = mm.var(WindowBuffer.class).invoke
                        ("capacityFor", rangeVar.invoke("start"), rangeVar.invoke("end"));
                }
                f.set(bufferVar);
            });

            bufferField.invoke("begin", valueVar);
            mBufferFieldName = bufferField.name();

            // Set the initial value with the highest bit set to indicate not finished yet.
            mRemainingFieldName = context.newWorkField(long.class).set((1L << 63) + 1).name();
        }

        @Override
        public void accumulate(GroupContext context) {
            var valueVar = context.args().get(0).eval(true);

            MethodMaker mm = context.methodMaker();

            if (mRangeFieldName != null) {
                LazyValue rangeVal = context.namedArgs().get(mArgName);
                // If the range is a constant evaluated at run time, then no need to update it.
                // FIXME: If range is constant, then assign in the constuctor. The
                // FunctionApplier will need a new "init" method or something.
                if (!rangeVal.expr().isConstant()) {
                    mm.field(mRangeFieldName).set(rangeVal.eval(true));
                }
            }

            mm.field(mBufferFieldName).invoke("append", valueVar);
            mm.field(mRemainingFieldName).inc(1);
        }

        @Override
        public void finished(GroupContext context) {
            // Clear the highest bit.
            var field = context.methodMaker().field(mRemainingFieldName);
            field.set(field.and(~(1L << 63)));
        }

        @Override
        public void check(GroupContext context) {
            MethodMaker mm = context.methodMaker();

            Label ready = mm.label();
            mm.field(mRemainingFieldName).ifGt(0L, ready);

            if (!isOpenEnd()) {
                var bufferField = mm.field(mBufferFieldName);
                Variable readyVar;
                if (mEndConstant != null) {
                    readyVar = bufferField.invoke("ready", Math.toIntExact(mEndConstant));
                } else {
                    var rangeVar = mm.field(mRangeFieldName);
                    readyVar = bufferField.invoke("ready", rangeVar.invoke("end"));
                }
                readyVar.ifTrue(ready);
            }

            mm.return_(null); // not ready
            ready.here();
        }

        @Override
        public Variable step(GroupContext context) {
            MethodMaker mm = context.methodMaker();

            Variable rangeVar = null;
            if (mRangeFieldName != null) {
                rangeVar = mm.field(mRangeFieldName).get();
            }

            var bufferVar = mm.field(mBufferFieldName).get();

            Object start, end;

            if (mStartConstant != null) {
                start = mStartConstant;
            } else {
                start = rangeVar.invoke("start");
            }

            if (mEndConstant != null) {
                end = mEndConstant;
            } else {
                end = rangeVar.invoke("end");
            }

            var resultVar = bufferVar.invoke("frameAverage", start, end);

            mm.field(mRemainingFieldName).inc(-1);

            if (isOpenStart()) {
                bufferVar.invoke("advance");
            } else if (mStartConstant != null && mStartConstant >= 0) {
                bufferVar.invoke("advanceAndRemove");
            } else {
                bufferVar.invoke("advanceAndRemove", start);
            }

            return resultVar;
        }

        private boolean isOpenStart() {
            return mStartConstant == Long.MIN_VALUE;
        }

        private boolean isOpenEnd() {
            return mEndConstant == Long.MAX_VALUE;
        }

        /**
         * @return null if the range start isn't a constant, Long.MIN_VALUE if open, or else a
         * 32-bit integer value
         */
        private static Long startConstant(LazyValue rangeVal) {
            if (rangeVal.isConstant()) {
                return verifyBoundary(((Range) rangeVal.constantValue()).start());
            }
            Expr start = ((RangeExpr) rangeVal.expr()).start();
            if (start instanceof ConstantExpr c) {
                return verifyBoundary((Long) c.value());
            }
            return null;
        }

        /**
         * @return null if the range end isn't a constant, Long.MAX_VALUE if open, or else a
         * 32-bit integer value
         */
        private static Long endConstant(LazyValue rangeVal) {
            if (rangeVal.isConstant()) {
                return verifyBoundary(((Range) rangeVal.constantValue()).end());
            }
            Expr end = ((RangeExpr) rangeVal.expr()).end();
            if (end instanceof ConstantExpr c) {
                return verifyBoundary((Long) c.value());
            }
            return null;
        }

        private static Long verifyBoundary(Long value) {
            if ((Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE)
                || value == Long.MIN_VALUE || value == Long.MAX_VALUE)
            {
                return value;
            }
            // RangeExpr is responsible for clamping the range.
            throw new IllegalStateException();
        }
    }
}
