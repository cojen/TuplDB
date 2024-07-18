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
import org.cojen.maker.FieldMaker;
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

        // True if the range start/end is a compile-time or run-time constant.
        private boolean mIsStartConstant, mIsEndConstant;

        // Is set if the range start/end is a compile-time constant.
        private Long mStartConstant, mEndConstant;

        // References the start/end of the frame range. Is null if start/end is
        // compile-constant. The field type is long when the start/end is a runtime constant,
        // and is a ValueBuffer.OfLong when the start/end is variable.
        private String mStartFieldName, mEndFieldName;

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

        @Override
        public void init(GroupContext context) {
            LazyValue rangeVal = context.namedArgs().get(mArgName);

            if (rangeVal.isConstant()) {
                mIsStartConstant = true;
                mIsEndConstant = true;
                var range = (Range) rangeVal.constantValue();
                mStartConstant = verifyBoundary(range.start());
                mEndConstant = verifyBoundary(range.end());
            } else if (rangeVal.expr() instanceof RangeExpr re) {
                if (re.start().isConstant()) {
                    mIsStartConstant = true;
                    if (re.start() instanceof ConstantExpr c) {
                        mStartConstant = verifyBoundary((Number) c.value());
                    }
                }
                if (re.end().isConstant()) {
                    mIsEndConstant = true;
                    if (re.end() instanceof ConstantExpr c) {
                        mEndConstant = verifyBoundary((Number) c.value());
                    }
                }
            }

            MethodMaker mm = context.methodMaker();

            if (mStartConstant == null) {
                String fieldName;
                if (mIsStartConstant) {
                    fieldName = context.newWorkField(long.class).final_().name();
                    mm.field(fieldName).set(rangeVal.eval(true).invoke("start"));
                } else {
                    fieldName = context.newWorkField(ValueBuffer.OfLong.class).final_().name();
                    mm.field(fieldName).set(mm.new_(ValueBuffer.OfLong.class, 8));
                }
                mStartFieldName = fieldName;
            }

            if (mEndConstant == null) {
                String fieldName;
                if (mIsEndConstant) {
                    fieldName = context.newWorkField(long.class).final_().name();
                    mm.field(fieldName).set(rangeVal.eval(true).invoke("end"));
                } else {
                    fieldName = context.newWorkField(ValueBuffer.OfLong.class).final_().name();
                    mm.field(fieldName).set(mm.new_(ValueBuffer.OfLong.class, 8));
                }
                mEndFieldName = fieldName;
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

            FieldMaker bufferFieldMaker = context.newWorkField(bufferType);
            mBufferFieldName = bufferFieldMaker.name();

            if (mStartConstant != null && mEndConstant != null) {
                bufferFieldMaker.final_();
                int capacity = WindowBuffer.capacityFor(mStartConstant, mEndConstant);
                mm.field(mBufferFieldName).set(mm.new_(bufferType, capacity));
            }

            mRemainingFieldName = context.newWorkField(long.class).name();
        }

        @Override
        public void begin(GroupContext context) {
            var valueVar = context.args().get(0).eval(true);

            LazyValue rangeVal = context.namedArgs().get(mArgName);

            MethodMaker mm = context.methodMaker();

            if (isStartVariable()) {
                mm.field(mStartFieldName).invoke("add", rangeVal.eval(true).invoke("start"));
            }

            if (isEndVariable()) {
                mm.field(mEndFieldName).invoke("add", rangeVal.eval(true).invoke("end"));
            }

            var bufferField = mm.field(mBufferFieldName);

            if (mStartConstant == null || mEndConstant == null) {
                var rangeVar = rangeVal.eval(true);
                var capacityVar = mm.var(WindowBuffer.class).invoke
                    ("capacityFor", rangeVar.invoke("start"), rangeVar.invoke("end"));
                Class<?> bufferType = bufferField.classType();
                bufferField.set(mm.new_(bufferType, capacityVar));
            }

            bufferField.invoke("begin", valueVar);

            // Set the initial value with the highest bit set to indicate not finished yet.
            mm.field(mRemainingFieldName).set((1L << 63) + 1);
        }

        @Override
        public void accumulate(GroupContext context) {
            var valueVar = context.args().get(0).eval(true);

            LazyValue rangeVal = context.namedArgs().get(mArgName);

            MethodMaker mm = context.methodMaker();

            if (isStartVariable()) {
                mm.field(mStartFieldName).invoke("add", rangeVal.eval(true).invoke("start"));
            }

            if (isEndVariable()) {
                mm.field(mEndFieldName).invoke("add", rangeVal.eval(true).invoke("end"));
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
                final Object end;
                if (mIsEndConstant) {
                    end = mEndConstant != null ? mEndConstant : mm.field(mEndFieldName);
                } else {
                    end = mm.field(mEndFieldName).invoke("get", 0);
                }
                mm.field(mBufferFieldName).invoke("ready", end).ifTrue(ready);
            }

            mm.return_(null); // not ready
            ready.here();
        }

        @Override
        public Variable step(GroupContext context) {
            MethodMaker mm = context.methodMaker();

            var bufferVar = mm.field(mBufferFieldName).get();

            final Object start, end;

            if (mIsStartConstant) {
                start = mStartConstant != null ? mStartConstant : mm.field(mStartFieldName);
            } else {
                start = mm.field(mStartFieldName).invoke("removeFirst");
            }

            if (mIsEndConstant) {
                end = mEndConstant != null ? mEndConstant : mm.field(mEndFieldName);
            } else {
                end = mm.field(mEndFieldName).invoke("removeFirst");
            }

            var resultVar = bufferVar.invoke("frameAverage", start, end);

            mm.field(mRemainingFieldName).inc(-1);

            if (isOpenStart() || !mIsStartConstant) {
                bufferVar.invoke("advance");
            } else if (mStartConstant != null && mStartConstant >= 0) {
                bufferVar.invoke("advanceAndRemove");
            } else {
                bufferVar.invoke("advanceAndRemove", start);
            }

            return resultVar;
        }

        private boolean isOpenStart() {
            return mStartConstant != null && mStartConstant == Long.MIN_VALUE;
        }

        private boolean isOpenEnd() {
            return mEndConstant != null && mEndConstant == Long.MAX_VALUE;
        }

        private boolean isStartVariable() {
            return !mIsStartConstant && mStartConstant == null;
        }

        private boolean isEndVariable() {
            return !mIsEndConstant && mEndConstant == null;
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

        private static Long verifyBoundary(Number value) {
            if (value instanceof Long v) {
                return verifyBoundary(v);
            }
            if (value instanceof Integer v) {
                return (long)((int) v);
            }
            // RangeExpr is responsible for clamping the range.
            throw new IllegalStateException();
        }
    }
}
