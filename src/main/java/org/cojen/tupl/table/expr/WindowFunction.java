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

import java.util.List;
import java.util.Map;

import java.util.function.Consumer;

import org.cojen.maker.Label;
import org.cojen.maker.FieldMaker;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Ordering;

import org.cojen.tupl.table.Converter;

import static org.cojen.tupl.table.expr.Type.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
abstract class WindowFunction extends FunctionApplier.Grouped {
    /**
     * Defines a window frame specification.
     *
     * @param argName "rows", "groups", or "range"
     * @param expr must have a Range type (not to be confused with MODE_RANGE)
     * @param ordering the expected value ordering, which is needed by MODE_RANGE
     */
    record Frame(String argName, Expr expr, Ordering ordering) {
        LazyValue rangeVal(GroupContext context) {
            return context.namedArgs().get(argName);
        }

        Variable evalRangeStart(GroupContext context) {
            return ((RangeExpr.Lazy) rangeVal(context)).evalStart(true);
        }

        Variable evalRangeEnd(GroupContext context) {
            return ((RangeExpr.Lazy) rangeVal(context)).evalEnd(true);
        }

        /**
         * Returns true if the range and is guaranteed to include the current row of a window
         * frame, which is denoted with a constant value of zero.
         */
        boolean includesCurrent() {
            return expr.isRangeWithCurrent();
        }

        private int mode() {
            return switch (argName) {
                case "rows" -> MODE_ROWS; case "groups" -> MODE_GROUPS; case "range" -> MODE_RANGE;
                default -> throw new IllegalStateException();
            };
        }

        private boolean isOrderDependent() {
            return ordering != Ordering.UNSPECIFIED && mode() == MODE_RANGE;
        }

        private String orderStr() {
            return switch (ordering) {
                case ASCENDING -> "Asc";
                case DESCENDING -> "Desc";
                default -> throw new AssertionError();
            };
        }
    }

    private static final int MODE_ROWS = 1, MODE_GROUPS = 2, MODE_RANGE = 3;

    // Defines the default ValueBuffer and WindowBuffer capacity when a more accurate capacity
    // cannot be determined.
    private static final int DEFAULT_CAPACITY = WindowBuffer.DEFAULT_MIN_CAPACITY;

    // The estimated group size to use when using calculating buffer capacity for MODE_GROUPS.
    private static final int GROUP_SIZE = 4;

    private final Type mValueType;
    private final Type mOriginalType;
    private final Frame mFrame;

    // True if the range start/end is a compile-time or run-time constant.
    private boolean mIsStartConstant, mIsEndConstant;

    // Is set if the range start/end is a compile-time constant. Type is Long for MODE_ROWS and
    // MODE_GROUPS, and is Double for MODE_RANGE.
    private Number mStartConstant, mEndConstant;

    private int mMode;

    // References the start/end of the range. Is null if start/end is compile-constant. The
    // field type is long when the start/end is a runtime constant, and is a ValueBuffer.OfLong
    // when the start/end is variable.
    // FIXME: Type might be different when using MODE_RANGE.
    private String mStartFieldName, mEndFieldName;

    // References a WindowBuffer instance.
    private String mBufferFieldName;

    // Tracks the number of remaining values that the step method must produce.
    private String mRemainingFieldName;

    // Is used by MODE_GROUPS and MODE_RANGE to determine if enough rows have been accumulated
    // before finding the group or range end position.
    private String mCheckAmountFieldName;

    // Is used by MODE_GROUPS and MODE_RANGE when the check method has found the window buffer
    // end position. When uninitialized, the value is Long.MAX_VALUE, which is never returned
    // by the WindowBuffer find end methods.
    private String mFoundEndFieldName;

    /**
     * @param resultType the result type produced by the function
     * @param valueType the intermediate value type which might need to be buffered
     * @param originalType the function argument type which was passed into it
     */
    WindowFunction(Type resultType, Type valueType, Type originalType, Frame frame) {
        super(resultType);
        mValueType = valueType;
        mOriginalType = originalType;
        mFrame = frame;
    }

    @Override
    public final boolean isOrderDependent() {
        return mFrame.isOrderDependent();
    }

    @Override
    public final boolean hasNamedParameters() {
        return true;
    }

    @Override
    public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                    Map<String, ProjExpr> projectionMap,
                                    Consumer<String> reasons)
    {
        return this;
    }

    @Override
    public void init(GroupContext context) {
        LazyValue rangeVal = mFrame.rangeVal(context);

        if (rangeVal.isConstant()) {
            mIsStartConstant = true;
            mIsEndConstant = true;
            var range = (Range) rangeVal.constantValue();
            mStartConstant = start(range);
            mEndConstant = end(range);
        } else if (rangeVal.expr() instanceof RangeExpr re) {
            if (re.start().isConstant()) {
                mIsStartConstant = true;
                if (re.start() instanceof ConstantExpr c) {
                    mStartConstant = start(Range.make((Number) c.value(), null));
                }
            }
            if (re.end().isConstant()) {
                mIsEndConstant = true;
                if (re.end() instanceof ConstantExpr c) {
                    mEndConstant = end(Range.make(null, (Number) c.value()));
                }
            }
        }

        MethodMaker mm = context.methodMaker();

        if (mStartConstant == null) {
            String fieldName;
            if (mIsStartConstant) {
                fieldName = context.newWorkField(long.class).final_().name();
                mm.field(fieldName).set(mFrame.evalRangeStart(context));
            } else {
                Type valueType = BasicType.make(long.class, Type.TYPE_LONG);
                Class<?> bufferType = ValueBuffer.forType(valueType);
                fieldName = context.newWorkField(bufferType).final_().name();
                mm.field(fieldName).set(mm.new_(bufferType, DEFAULT_CAPACITY));
            }
            mStartFieldName = fieldName;
        }

        if (mEndConstant == null) {
            String fieldName;
            if (mIsEndConstant) {
                fieldName = context.newWorkField(long.class).final_().name();
                mm.field(fieldName).set(mFrame.evalRangeEnd(context));
            } else {
                Type valueType = BasicType.make(long.class, Type.TYPE_LONG);
                Class<?> bufferType = ValueBuffer.forType(valueType);
                fieldName = context.newWorkField(bufferType).final_().name();
                mm.field(fieldName).set(mm.new_(bufferType, DEFAULT_CAPACITY));
            }
            mEndFieldName = fieldName;
        }

        Class<?> bufferType = bufferType();
        FieldMaker bufferFieldMaker = context.newWorkField(bufferType);
        mBufferFieldName = bufferFieldMaker.name();

        mMode = isOpenStart() && isOpenEnd() ? MODE_ROWS : mFrame.mode();

        if (mMode == MODE_RANGE) {
            bufferFieldMaker.final_();
            mm.field(mBufferFieldName).set(mm.new_(bufferType, DEFAULT_CAPACITY));
        } else if (mStartConstant != null && mEndConstant != null) {
            bufferFieldMaker.final_();
            int capacity = WindowBuffer.capacityFor((Long) mStartConstant, (Long) mEndConstant);
            if (mMode == MODE_GROUPS) {
                capacity = WindowBuffer.clampCapacity(capacity * (long) GROUP_SIZE);
            }
            mm.field(mBufferFieldName).set(mm.new_(bufferType, capacity));
        }

        mRemainingFieldName = context.newWorkField(long.class).name();

        if (mMode != MODE_ROWS) {
            mCheckAmountFieldName = context.newWorkField(int.class).name();
            var checkField = mm.field(mCheckAmountFieldName);
            if (mMode != MODE_GROUPS || !mIsEndConstant) {
                checkField.set(DEFAULT_CAPACITY);
            } else if (mEndConstant != null) {
                checkField.set(WindowBuffer.capacityForGroup(GROUP_SIZE, (Long) mEndConstant));
            } else {
                var capacityVar = mm.var(WindowBuffer.class)
                    .invoke("capacityFor", GROUP_SIZE, mm.field(mEndFieldName));
                checkField.set(capacityVar);
            }

            mFoundEndFieldName = context.newWorkField(long.class).name();
            mm.field(mFoundEndFieldName).set(Long.MAX_VALUE);
        }
    }

    @Override
    public void begin(GroupContext context) {
        var valueVar = evalArg(context);

        MethodMaker mm = context.methodMaker();

        if (isStartVariable()) {
            mm.field(mStartFieldName).invoke("add", mFrame.evalRangeStart(context));
        }

        if (isEndVariable()) {
            mm.field(mEndFieldName).invoke("add", mFrame.evalRangeEnd(context));
        }

        var bufferField = mm.field(mBufferFieldName);

        if (mMode != MODE_RANGE && (mStartConstant == null || mEndConstant == null)) {
            var startVar = mFrame.evalRangeStart(context);
            var endVar = mFrame.evalRangeEnd(context);
            var windowBufferVar = mm.var(WindowBuffer.class);
            var capacityVar = windowBufferVar.invoke("capacityFor", startVar, endVar);
            if (mMode == MODE_GROUPS) {
                capacityVar = windowBufferVar.invoke
                    ("clampCapacity", capacityVar.cast(long.class).mul(GROUP_SIZE));
            }
            Class<?> bufferType = bufferField.classType();
            bufferField.set(mm.new_(bufferType, capacityVar));
        }

        bufferField.invoke("begin", valueVar);

        // Set the initial value with the highest bit set to indicate not finished yet.
        mm.field(mRemainingFieldName).set((1L << 63) + 1);
    }

    @Override
    public void accumulate(GroupContext context) {
        var valueVar = evalArg(context);

        MethodMaker mm = context.methodMaker();

        if (isStartVariable()) {
            mm.field(mStartFieldName).invoke("add", mFrame.evalRangeStart(context));
        }

        if (isEndVariable()) {
            mm.field(mEndFieldName).invoke("add", mFrame.evalRangeEnd(context));
        }

        mm.field(mBufferFieldName).invoke("append", valueVar);
        mm.field(mRemainingFieldName).inc(1);
    }

    protected Variable evalArg(GroupContext context) {
        var valueVar = context.args().get(0).eval(true);

        if (!mValueType.equals(mOriginalType)) {
            MethodMaker mm = context.methodMaker();
            var convertedVar = mm.var(mValueType.clazz());
            Converter.convertLossy(mm, mOriginalType, valueVar, mValueType, convertedVar);
            valueVar = convertedVar;
        }

        return valueVar;
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
        Label notReady = null;

        var remainingVar = mm.field(mRemainingFieldName).get();
        remainingVar.ifGt(0L, ready);

        if (!isOpenEnd()) {
            if (mEndConstant == null || ((Number) mEndConstant).doubleValue() < 0) {
                // The current row isn't guaranteed to be included, so prevent the remaining
                // amount from decrementing below zero. This check isn't necessary when the end
                // is known to always include the current row because then the buffer ready
                // check is sufficient. As long as there's something in the buffer, then the
                // remaining amount must be at least one.
                notReady = mm.label();
                remainingVar.and(~(1L << 63)).ifEq(0, notReady);
            }

            final Object end;
            if (mIsEndConstant) {
                end = mEndConstant != null ? mEndConstant : mm.field(mEndFieldName);
            } else {
                end = mm.field(mEndFieldName).invoke("get", 0);
            }

            var bufferVar = mm.field(mBufferFieldName);
            var bufferEndVar = bufferVar.invoke("end");

            if (mMode == MODE_ROWS) {
                // Is ready when the buffer end is greater than or equal to the desired end.
                bufferEndVar.ifGe(end, ready);
            } else {
                // First check if enough rows have been accumulated.
                var checkField = mm.field(mCheckAmountFieldName);
                final var checkVar = checkField.get();
                if (notReady == null) {
                    notReady = mm.label();
                }
                bufferEndVar.ifLt(checkVar, notReady);

                final Variable foundEndVar;

                if (mMode == MODE_GROUPS) {
                    foundEndVar = bufferVar.invoke("findGroupEnd", end);
                } else {
                    assert mMode == MODE_RANGE;
                    // FIXME: pass last endPos if delta is constant
                    foundEndVar = bufferVar.invoke("findRangeEnd" + mFrame.orderStr(), end, 0L);
                }

                // If the found end is greater than or equal to the buffer end, then there's no
                // guarantee that the end of the group has been reached.
                Label needsMore = mm.label();
                foundEndVar.ifGe(bufferEndVar, needsMore);
                mm.field(mFoundEndFieldName).set(foundEndVar);
                ready.goto_();

                needsMore.here();
                // Expand the check amount for the next time.
                checkVar.set(checkVar.shl(1));
                checkVar.ifLe(0, () -> checkVar.set(Integer.MAX_VALUE));
                checkField.set(checkVar);
            }
        }

        if (notReady != null) {
            notReady.here();
        }

        mm.return_(null); // not ready

        ready.here();
    }

    @Override
    public Variable step(GroupContext context) {
        MethodMaker mm = context.methodMaker();

        var bufferVar = mm.field(mBufferFieldName).get();

        Object start, end;

        if (mIsStartConstant) {
            start = mStartConstant != null ? mStartConstant : mm.field(mStartFieldName);
        } else {
            start = mm.field(mStartFieldName).invoke("removeFirst");
        }

        if (mMode != MODE_ROWS && !isOpenStart()) {
            if (mMode == MODE_GROUPS) {
                start = bufferVar.invoke("findGroupStart", start);
            } else {
                assert mMode == MODE_RANGE;
                // FIXME: pass last startPos if delta is constant
                start = bufferVar.invoke("findRangeStart" + mFrame.orderStr(), start, 0L);
            }
        }

        if (mIsEndConstant) {
            end = mEndConstant != null ? mEndConstant : mm.field(mEndFieldName);
        } else {
            end = mm.field(mEndFieldName).invoke("removeFirst");
        }

        if (mMode != MODE_ROWS && !isOpenEnd()) {
            var foundEndField = mm.field(mFoundEndFieldName); 
            var endVar = foundEndField.get();
            Label notFound = mm.label();
            endVar.ifEq(Long.MAX_VALUE, notFound);
            foundEndField.set(Long.MAX_VALUE);
            Label cont = mm.label().goto_();

            notFound.here();

            if (mMode == MODE_GROUPS) {
                endVar.set(bufferVar.invoke("findGroupEnd", end));
            } else {
                assert mMode == MODE_RANGE;
                // FIXME: pass last endPos if delta is constant
                endVar.set(bufferVar.invoke("findRangeEnd" + mFrame.orderStr(), end, 0L));
            }

            cont.here();
            end = endVar;
        }

        // FIXME: For MODE_GROUPS, if the range is constant, and the buffer value is the same
        // as before, then no need to compute a new target value.
        var resultVar = compute(bufferVar, start, end);

        mm.field(mRemainingFieldName).inc(-1);

        if (isOpenStart() || !mIsStartConstant) {
            bufferVar.invoke("advance");
        } else if (mMode != MODE_ROWS) {
            bufferVar.invoke("trimStart", start);
            bufferVar.invoke("advance");
        } else if (mStartConstant != null && ((Number) mStartConstant).doubleValue() >= 0) {
            bufferVar.invoke("advanceAndRemove");
        } else {
            bufferVar.invoke("advanceAndRemove", start);
        }

        return resultVar;
    }

    protected final Type valueType() {
        return mValueType;
    }

    protected final Type originalType() {
        return mOriginalType;
    }

    protected final Frame frame() {
        return mFrame;
    }

    /**
     * Returns a suitable WindowBuffer class.
     */
    protected Class<?> bufferType() {
        return WindowBuffer.forType(mValueType);
    }

    /**
     * Compute a result over the given frame.
     *
     * @param bufferVar refers to a WindowBuffer
     * @param frameStart is a Variable or a constant
     * @param frameEnd is a Variable or a constant
     * @return a variable with the result type, or it must be trivially convertable to the
     * result type
     */
    protected abstract Variable compute(Variable bufferVar, Object frameStart, Object frameEnd);

    private Number start(Range range) {
        if (mFrame.mode() == MODE_RANGE) {
            return range.start_double();
        } else {
            return range.start_long();
        }
    }

    private Number end(Range range) {
        if (mFrame.mode() == MODE_RANGE) {
            return range.end_double();
        } else {
            return range.end_long();
        }
    }

    private boolean isOpenStart() {
        return mStartConstant != null &&
            (mStartConstant == (Number) Long.MIN_VALUE ||
             mStartConstant == (Number) Double.NEGATIVE_INFINITY);
    }

    private boolean isOpenEnd() {
        return mEndConstant != null &&
            (mEndConstant == (Number) Long.MAX_VALUE ||
             mEndConstant == (Number) Double.POSITIVE_INFINITY);
    }

    private boolean isStartVariable() {
        return !mIsStartConstant && mStartConstant == null;
    }

    private boolean isEndVariable() {
        return !mIsEndConstant && mEndConstant == null;
    }
}
