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

import org.cojen.tupl.table.Converter;

import static org.cojen.tupl.table.expr.Type.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
// FIXME: Only supports "row" type range. Need to support "range" and "groups" too.
abstract class WindowFunction extends FunctionApplier.Grouped {
    /**
     * Defines a window frame specification.
     *
     * @param argName "rows", "range", or "groups"
     * @param expr must have a Range type
     */
    record Frame(String argName, Expr expr) {
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
    }

    private final Type mValueType;
    private final Type mOriginalType;
    private final Frame mFrame;

    // True if the range start/end is a compile-time or run-time constant.
    private boolean mIsStartConstant, mIsEndConstant;

    // Is set if the range start/end is a compile-time constant.
    private Long mStartConstant, mEndConstant;

    // References the start/end of the frame range. Is null if start/end is compile-constant.
    // The field type is long when the start/end is a runtime constant, and is a
    // ValueBuffer.OfLong when the start/end is variable.
    private String mStartFieldName, mEndFieldName;

    // References a WindowBuffer instance.
    private String mBufferFieldName;

    // Tracks the number of remaining values that the step method must produce.
    private String mRemainingFieldName;

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
    public final boolean hasNamedParameters() {
        return true;
    }

    @Override
    public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
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
            mStartConstant = range.start_long();
            mEndConstant = range.end_long();
        } else if (rangeVal.expr() instanceof RangeExpr re) {
            if (re.start().isConstant()) {
                mIsStartConstant = true;
                if (re.start() instanceof ConstantExpr c) {
                    mStartConstant = Range.make((Number) c.value(), null).start_long();
                }
            }
            if (re.end().isConstant()) {
                mIsEndConstant = true;
                if (re.end() instanceof ConstantExpr c) {
                    mEndConstant = Range.make(null, (Number) c.value()).end_long();
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
                mm.field(fieldName).set(mm.new_(bufferType, 8));
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
                mm.field(fieldName).set(mm.new_(bufferType, 8));
            }
            mEndFieldName = fieldName;
        }

        Class<?> bufferType = bufferType();
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
        var valueVar = evalArg(context);

        MethodMaker mm = context.methodMaker();

        if (isStartVariable()) {
            mm.field(mStartFieldName).invoke("add", mFrame.evalRangeStart(context));
        }

        if (isEndVariable()) {
            mm.field(mEndFieldName).invoke("add", mFrame.evalRangeEnd(context));
        }

        var bufferField = mm.field(mBufferFieldName);

        if (mStartConstant == null || mEndConstant == null) {
            var startVar = mFrame.evalRangeStart(context);
            var endVar = mFrame.evalRangeEnd(context);
            var capacityVar = mm.var(WindowBuffer.class).invoke("capacityFor", startVar, endVar);
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
            if (mEndConstant == null || mEndConstant < 0) {
                // Prevent the remaining amount from decrementing below zero. This check isn't
                // necessary when the end is known to always include the current row because
                // the the buffer ready check is sufficient. As long as there's something in
                // the buffer, then the remaining amount must be at least one.
                notReady = mm.label();
                remainingVar.and(~(1L << 63)).ifEq(0, notReady);
            }

            final Object end;
            if (mIsEndConstant) {
                end = mEndConstant != null ? mEndConstant : mm.field(mEndFieldName);
            } else {
                end = mm.field(mEndFieldName).invoke("get", 0);
            }

            mm.field(mBufferFieldName).invoke("ready", end).ifTrue(ready);
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

        var resultVar = compute(bufferVar, start, end);

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
}
