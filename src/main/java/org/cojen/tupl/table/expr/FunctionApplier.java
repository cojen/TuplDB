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

import org.cojen.maker.Field;
import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Ordering;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.Converter;

/**
 * Generates code to invoke a function.
 *
 * @author Brian S. O'Neill
 * @see FunctionFinder
 */
public abstract class FunctionApplier {
    private final Type mType;

    private FunctionApplier(Type type) {
        mType = type;
    }

    /**
     * Checks if the number of function arguments provided is legal.
     *
     * @param min minimum number of required arguments
     * @param max maximum number of required arguments
     * @param actual actual number of arguments provided
     * @param reason if false is returned, a reason is provided
     * @return true if the number is legal
     */
    public static boolean checkNumArgs(int min, int max, int actual, Consumer<String> reason) {
        if (min <= actual && actual <= max) {
            return true;
        }

        final String message;

        message: {
            if (min == 0) {
                if (max == 0) {
                    message = "no unnamed arguments are allowed";
                    break message;
                } else if (max == 1) {
                    message = "at most 1 unnamed argument is allowed";
                    break message;
                } else {
                    message = "at most " + max + " unnamed arguments are allowed";
                    break message;
                }
            } else if (min == 1) {
                if (max == 1) {
                    message = "exactly 1 unnamed argument is required";
                    break message;
                } else if (max == Integer.MAX_VALUE) {
                    message = "at least 1 unnamed argument is required";
                    break message;
                }
            } else if (min == max) {
                message = "exactly " + min + " unnamed arguments are required";
                break message;
            } else if (max == Integer.MAX_VALUE) {
                message = "at least " + min + " unnamed arguments are required";
                break message;
            }

            message = min + " to " + max + " unnamed arguments are required";
        }

        reason.accept(message);
        return false;
    }

    /**
     * Returns true if one of the named arguments is "rows", "range", or "groups".
     */
    public static boolean hasFrame(Map<String, Expr> namedArgs) {
        return namedArgs.containsKey("rows")
            || namedArgs.containsKey("range")
            || namedArgs.containsKey("groups");
    }

    /**
     * Returns a frame object corresponding to the named argument of "rows", "range", or
     * "groups". If the frame is malformed or if not exactly one is provided, then null is
     * returned a reason is provided.
     *
     * @param projectionMap is used when mode is "range" to validate that arg 0 is ordered; see
     * CallExpr.make; can be null if empty
     * @param reason if null is returned, a reason is provided
     */
    public static WindowFunction.Frame accessFrame(List<Expr> args, Map<String, Expr> namedArgs,
                                                   Map<String, ProjExpr> projectionMap,
                                                   Consumer<String> reason)
    {
        Ordering ordering = Ordering.UNSPECIFIED;

        if (projectionMap != null && !projectionMap.isEmpty() && !args.isEmpty()) {
            Expr arg = args.getFirst();
            if (arg.isConstant()) {
                ordering = Ordering.ASCENDING; // could be DESCENDING too; doesn't matter
            } else {
                String n;
                if (arg instanceof Attr attr && projectionMap.containsKey((n = attr.name()))) {
                    Map.Entry<String, ProjExpr> first = projectionMap.entrySet().iterator().next();
                    if (first.getKey().equals(n)) {
                        ordering = first.getValue().ordering();
                    }
                }
            }
        }

        check: {
            String name = "rows";
            Expr expr = namedArgs.get(name);

            Expr rangeExpr = namedArgs.get("range");
            if (rangeExpr != null) {
                if (expr != null) {
                    break check;
                }
                name = "range";
                expr = rangeExpr;

                if (ordering == Ordering.UNSPECIFIED) {
                    reason.accept("range frame type requires ordered values");
                    return null;
                }

                if (!args.getFirst().type().isNumber()) {
                    reason.accept("range argument type must be a number");
                    return null;
                }
            }

            Expr groupsExpr = namedArgs.get("groups");
            if (groupsExpr != null) {
                if (expr != null) {
                    break check;
                }
                name = "groups";
                expr = groupsExpr;
            }

            if (expr == null) {
                reason.accept("no frame type is specified");
                return null;
            }

            if (expr.type().clazz() != Range.class) {
                reason.accept("frame type must be a range");
                return null;
            }

            if (namedArgs.size() > 1) {
                reason.accept("unsupported named argument");
                return null;
            }

            return new WindowFunction.Frame(name, expr, ordering);
        }

        reason.accept("more than one type of frame is specified");
        return null;
    }

    /**
     * Validate that the function accepts the given arguments, and returns a new applier
     * instance. If any arguments should be converted, directly replace elements of the args
     * list with the desired type.
     *
     * @param args non-null array of unnamed argument types
     * @param namedArgs non-null map of named argument types
     * @param projectionMap see CallExpr.make; can be null if empty
     * @param reasons if validation fails, optionally provide reasons
     * @return the new applier, or else null if validation fails
     */
    public abstract FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                             Map<String, ProjExpr> projectionMap,
                                             Consumer<String> reasons);

    /**
     * Returns the type that the function returns, or else null if this applier isn't
     * validated.
     */
    public final Type type() {
        return mType;
    }

    /**
     * Returns true (by default) if the applied function yields the same result when given the
     * same arguments.
     */
    public boolean isPureFunction() {
        return true;
    }

    /**
     * Returns true if the function needs a projection group.
     */
    public abstract boolean isGrouping();

    /**
     * Returns true if this is an accumulating function and the generated code depends on the
     * input values arriving in a specific order. This generally implies the use of a
     * WindowFunction with a "range" frame mode.
     */
    public abstract boolean isOrderDependent();

    /**
     * Returns true if the function accepts named parameters.
     */
    public boolean hasNamedParameters() {
        return false;
    }

    public static interface FunctionContext {
        MethodMaker methodMaker();

        /**
         * Returns the arguments passed to the function.
         */
        default List<LazyValue> args() {
            throw new IllegalStateException();
        }

        /**
         * Returns the named arguments passed to the function.
         */
        default Map<String, LazyValue> namedArgs() {
            throw new IllegalStateException();
        }
    }

    /**
     * A plain function is a plain function.
     */
    public abstract static class Plain extends FunctionApplier {
        protected Plain(Type type) {
            super(type);
        }

        /**
         * Generate code to apply the function.
         *
         * @param resultVar non-null result variable to set
         */
        public abstract void apply(FunctionContext context, Variable resultVar);

        /**
         * Override and return true if the function requires a group number or group row number.
         */
        @Override
        public boolean isGrouping() {
            return false;
        }

        @Override
        public final boolean isOrderDependent() {
            return false;
        }
    }

    public static interface GroupContext extends FunctionContext {
        /**
         * Defines a new field with the given type. Should only be called by an init method.
         */
        default FieldMaker newWorkField(Class<?> type) {
            throw new IllegalStateException();
        }

        /**
         * Returns a variable for the current row number, which spans the entire query. Row
         * numbers begin at 1L.
         */
        default Variable rowNum() {
            throw new IllegalStateException();
        }

        /**
         * Returns a variable for the current group number, which spans the entire group. Group
         * numbers begin at 1L.
         */
        default Variable groupNum() {
            throw new IllegalStateException();
        }

        /**
         * Returns a variable for the current group row number. Group row numbers begin at 1L.
         */
        default Variable groupRowNum() {
            throw new IllegalStateException();
        }
    }

    abstract static class Accumulator extends FunctionApplier {
        private Accumulator(Type type) {
            super(type);
        }

        @Override
        public final boolean isGrouping() {
            return true;
        }

        @Override
        public boolean isOrderDependent() {
            return false;
        }

        abstract void init(GroupContext context);

        abstract void begin(GroupContext context);

        abstract void accumulate(GroupContext context);
    }

    /**
     * A grouped function operates over groups of rows, producing one result for each input
     * value.
     */
    public abstract static class Grouped extends Accumulator {
        protected Grouped(Type type) {
            super(type);
        }

        /**
         * Generate code for the constructor. The given context can be called to make and
         * initialize all of the necessary work fields, and the applier instance needs to
         * maintain references to their names.
         */
        @Override
        public abstract void init(GroupContext context);

        /**
         * Generate code for the first row in the group.
         */
        @Override
        public abstract void begin(GroupContext context);

        /**
         * Generate code for each row in the group, other than the first.
         */
        @Override
        public abstract void accumulate(GroupContext context);

        /**
         * Generate code which is called after all rows in the group have been provided.
         */
        public abstract void finished(GroupContext context);

        /**
         * Generate code which should return null if the step method has nothing to produce.
         */
        public abstract void check(GroupContext context);

        /**
         * Generate code to produce a result.
         */
        public abstract Variable step(GroupContext context);
    }

    /**
     * An aggregate function computes an aggregate over a group of rows. One result is produced
     * for a group of input values.
     */
    public abstract static class Aggregated extends Accumulator {
        protected Aggregated(Type type) {
            super(type);
        }

        /**
         * Generate code for the constructor. The given context can be called to make and
         * initialize all of the necessary work fields, and the applier instance needs to
         * maintain references to their names.
         */
        @Override
        public abstract void init(GroupContext context);

        /**
         * Generate code for the first row in the group.
         */
        @Override
        public abstract void begin(GroupContext context);

        /**
         * Generate code for each row in the group, other than the first.
         */
        @Override
        public abstract void accumulate(GroupContext context);

        /**
         * Generate code for when the group is finished, producing an aggregate result.
         */
        public abstract Variable finish(GroupContext context);
    }

    /**
     * Defines an aggregate function which needs a simple work field.
     */
    public abstract static class BasicAggregated extends Aggregated {
        protected final Type mOriginalType;

        protected String mWorkFieldName;

        protected BasicAggregated(Type type, Type originalType) {
            super(type);
            mOriginalType = originalType;
        }

        @Override
        public void init(GroupContext context) {
            mWorkFieldName = context.newWorkField(type().clazz()).name();
        }

        @Override
        public void begin(GroupContext context) {
            workField(context).set(convert(eval(context.args().getFirst())));
        }

        @Override
        public Variable finish(GroupContext context) {
            return workField(context);
        }

        protected final Field workField(GroupContext context) {
            return context.methodMaker().field(mWorkFieldName);
        }

        protected Variable eval(LazyValue arg) {
            return arg.eval(true);
        }

        /**
         * Converts a value to the work field type. The original variable is returned if no
         * conversion is necessary.
         */
        protected Variable convert(Variable value) {
            if (type().clazz() == value.classType()) {
                return value;
            }
            MethodMaker mm = value.methodMaker();
            var converted = mm.var(type().clazz());
            Converter.convertLossy(mm, mOriginalType, value, type(), converted);
            return converted;
        }

        /**
         * Converts a non-null value to the work field type. The original variable is returned
         * if no conversion is necessary.
         */
        protected Variable convertNonNull(Variable value) {
            if (type().clazz() == value.classType()) {
                return value;
            }

            ColumnInfo srcInfo = mOriginalType;
            if (srcInfo.isNullable()) {
                srcInfo = srcInfo.copy();
                srcInfo.typeCode &= ~Type.TYPE_NULLABLE;
            }

            MethodMaker mm = value.methodMaker();
            var converted = mm.var(type().clazz());

            Converter.convertLossy(mm, srcInfo, value, type(), converted);

            return converted;
        }
    }

    /**
     * Defines an aggregate function which computes against a simple work field.
     */
    public abstract static class ComputeAggregated extends BasicAggregated {
        protected ComputeAggregated(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        public void accumulate(GroupContext context) {
            Field workField = workField(context);
            Type type = type();
            Label done = null;

            Variable left = workField;

            if (type.isNullable()) {
                left = left.get();
                done = context.methodMaker().label();
                left.ifEq(null, done);
            }

            Variable right = eval(context.args().getFirst());

            if (type.isNullable()) {
                Label notNull = context.methodMaker().label();
                right.ifNe(null, notNull);
                workField.set(null);
                done.goto_();
                notNull.here();
            }

            workField.set(compute(type, convertNonNull(left), convertNonNull(right)));

            if (done != null) {
                done.here();
            }
        }

        @Override
        protected Variable eval(LazyValue arg) {
            return arg.eval(!type().isNullable());
        }

        /**
         * Generate code to compute an incremental result against two non-null values.
         */
        protected abstract Variable compute(Type type, Variable left, Variable right);
    }

    /**
     * Defines an aggregate function which performs numerical computation against a simple work
     * field.
     */
    public abstract static class NumericalAggregated extends ComputeAggregated {
        protected NumericalAggregated(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                        Map<String, ProjExpr> projectionMap,
                                        Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, args.size(), reason)) {
                return null;
            }

            Type type = args.getFirst().type();

            if (!type.isNumber()) {
                reason.accept("argument must be a number");
                return null;
            }

            return validate(type, args, namedArgs, projectionMap, reason);
        }

        protected abstract FunctionApplier validate(Type type,
                                                    List<Expr> args, Map<String, Expr> namedArgs,
                                                    Map<String, ProjExpr> projectionMap,
                                                    Consumer<String> reason);
    }

    /**
     * Defines an aggregate function which performs numerical computation against a simple work
     * field, skipping over null values.
     */
    public abstract static class NullSkipNumericalAggregated extends NumericalAggregated {
        // Used to count the number of non-null values.
        private String mCountFieldName;

        protected NullSkipNumericalAggregated(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        public final void init(GroupContext context) {
            if (!nullSkip()) {
                super.init(context);
                return;
            }

            if (requireCountField()) {
                mCountFieldName = context.newWorkField(long.class).name();
            }

            mWorkFieldName = context.newWorkField(type().clazz()).name();
        }

        @Override
        public final void begin(GroupContext context) {
            if (!nullSkip()) {
                super.begin(context);
                return;
            }

            var arg = convert(eval(context.args().getFirst()));
            workField(context).set(arg);

            if (mCountFieldName != null) {
                MethodMaker mm = context.methodMaker();
                Field countField = mm.field(mCountFieldName);
                Label notNull = mm.label();
                arg.ifNe(null, notNull);
                countField.set(0L);
                Label done = mm.label().goto_();
                notNull.here();
                countField.set(1L);
                done.here();
            }
        }

        @Override
        public final void accumulate(GroupContext context) {
            if (!nullSkip()) {
                super.accumulate(context);
                return;
            }

            var workField = workField(context);
            Variable left = workField.get();
            Variable right = convert(eval(context.args().getFirst()));

            MethodMaker mm = context.methodMaker();
            Label done = mm.label();

            Label hasLeft = mm.label();
            left.ifNe(null, hasLeft);
            right.ifEq(null, done);
            workField.set(right);
            Label updated = mm.label().goto_();
            hasLeft.here();
            right.ifEq(null, done);
            workField.set(compute(type(), left, right));
            updated.here();
            if (mCountFieldName != null) {
                mm.field(mCountFieldName).inc(1L);
            }

            done.here();
        }

        @Override
        protected final Variable eval(LazyValue arg) {
            return !nullSkip() ? super.eval(arg) : arg.eval(true);
        }

        /**
         * Returns true if a field which counts the number of non-null values is needed.
         */
        protected boolean requireCountField() {
            return false;
        }

        /**
         * Returns a field which counts the number of non-null values.
         */
        protected final Variable countField(GroupContext context) {
            String name = mCountFieldName;
            return name == null ? context.groupRowNum() : context.methodMaker().field(name);
        }

        private boolean nullSkip() {
            return type().isNullable();
        }
    }

    /**
     * Defines an aggregate function which performs numerical computation against a simple work
     * field, treating null values as zero.
     */
    public abstract static class NullZeroNumericalAggregated extends NumericalAggregated {
        // Used to count the number of non-null values.
        private String mCountFieldName;

        protected NullZeroNumericalAggregated(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        public final void init(GroupContext context) {
            if (!nullAsZero()) {
                super.init(context);
                return;
            }

            if (requireCountField()) {
                mCountFieldName = context.newWorkField(long.class).name();
            }

            mWorkFieldName = context.newWorkField(type().clazz()).name();
        }

        @Override
        public final void begin(GroupContext context) {
            if (!nullAsZero()) {
                super.begin(context);
                return;
            }

            MethodMaker mm = context.methodMaker();

            Field countField;
            if (mCountFieldName != null) {
                countField = mm.field(mCountFieldName);
            } else {
                countField = null;
            }

            Field workField = workField(context);

            Variable arg = eval(context.args().getFirst());

            Label notNull = mm.label();
            arg.ifNe(null, notNull);
            Arithmetic.zero(workField);
            if (countField != null) {
                countField.set(0L);
            }
            Label done = mm.label().goto_();
            notNull.here();
            workField.set(convertNonNull(arg));
            if (countField != null) {
                countField.set(1L);
            }
            done.here();
        }

        @Override
        public final void accumulate(GroupContext context) {
            if (!nullAsZero()) {
                super.accumulate(context);
                return;
            }

            var workField = workField(context);
            Variable arg = eval(context.args().getFirst());

            MethodMaker mm = context.methodMaker();
            Label done = mm.label();
            arg.ifEq(null, done);
            workField.set(compute(type(), workField, convertNonNull(arg)));
            if (mCountFieldName != null) {
                context.methodMaker().field(mCountFieldName).inc(1L);
            }
            done.here();
        }

        @Override
        protected final Variable eval(LazyValue arg) {
            return !nullAsZero() ? super.eval(arg) : arg.eval(true);
        }

        /**
         * Returns true if a field which counts the number of non-null values is needed.
         */
        protected boolean requireCountField() {
            return false;
        }

        /**
         * Returns a (long) field which counts the number of non-null values.
         */
        protected final Variable countField(GroupContext context) {
            String name = mCountFieldName;
            return name == null ? context.groupRowNum() : context.methodMaker().field(name);
        }

        protected final Type originalType() {
            return mOriginalType;
        }

        /**
         * Returns true if nulls are possible and can be replaced with zero.
         */
        private boolean nullAsZero() {
            return mOriginalType.isNullable() && Arithmetic.canZero(mOriginalType.clazz());
        }
    }
}
