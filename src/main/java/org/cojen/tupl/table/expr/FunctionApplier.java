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
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

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
                    message = "no arguments are allowed";
                    break message;
                } else if (max == 1) {
                    message = "at most 1 argument is allowed";
                    break message;
                } else {
                    message = "at most " + max + " arguments are allowed";
                    break message;
                }
            } else if (min == 1) {
                if (max == 1) {
                    message = "exactly 1 argument is required";
                    break message;
                } else if (max == Integer.MAX_VALUE) {
                    message = "at least 1 argument is required";
                    break message;
                }
            } else if (min == max) {
                message = "exactly " + min + " arguments are required";
                break message;
            } else if (max == Integer.MAX_VALUE) {
                message = "at least " + min + " arguments are required";
                break message;
            }

            message = min + " to " + max + " arguments are required";
        }

        reason.accept(message);
        return false;
    }

    /**
     * Validate that the function acceps the given arguments, and returns a new applier
     * instance. If any arguments should be converted, directly replace elements of the
     * argTypes array with the desired type.
     *
     * @param argTypes non-null array of unnamed argument types
     * @param namedArgTypes non-null map of named argument types
     * @param reasons if validation fails, optionally provide reasons
     * @return the new applier, or else null if validation fails
     */
    public abstract FunctionApplier validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                                             Consumer<String> reasons);

    /**
     * Returns the type that the function returns, or else null if this applier isn't
     * validated.
     */
    public final Type type() {
        return mType;
    }

    /**
     * Returns true if the applied function is plain and yields the same result when given the
     * same arguments.
     */
    public abstract boolean isPureFunction();

    /**
     * Returns true if the function needs a projection group.
     */
    public abstract boolean isGrouping();

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
         * Override and return false if the function isn't pure.
         */
        @Override
        public boolean isPureFunction() {
            return true;
        }

        /**
         * Override and return true if the function requires a group number or group row number.
         */
        @Override
        public boolean isGrouping() {
            return false;
        }
    }

    public static interface GroupContext extends FunctionContext {
        /**
         * Defines a new field with the given type. Should only be called by a begin method.
         */
        default Field newWorkField(Class<?> type) {
            return newWorkField(type, false, null);
        }

        /**
         * Defines a new field with the given type. Should only be called by a begin method.
         *
         * @param final true if the field should be final
         * @param init optional code to initialize the field in the constructor
         */
        default Field newWorkField(Class<?> type, boolean final_, Consumer<Field> init) {
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
        public final boolean isPureFunction() {
            return false;
        }

        @Override
        public final boolean isGrouping() {
            return true;
        }

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
         * Generate code for the first row in the group. The given context can be called to
         * make and initialize all of the necessary work fields, and the applier instance needs
         * to maintain references to their names.
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
         * Generate code which branches to an appropriate label depending on whether the step
         * method is ready or not.
         */
        public abstract void check(GroupContext context, Label ready, Label notReady);

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
         * Generate code for the first row in the group. The given context can be called to
         * make and initialize all of the necessary work fields, and the applier instance needs
         * to maintain references to their names.
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
        protected String mWorkFieldName;

        protected BasicAggregated(Type type) {
            super(type);
        }

        @Override
        public void begin(GroupContext context) {
            Variable arg = eval(context.args().get(0));
            mWorkFieldName = context.newWorkField(type().clazz()).set(arg).name();
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
    }

    /**
     * Defines an aggregate function which computes against a simple work field.
     */
    public abstract static class ComputeAggregated extends BasicAggregated {
        protected ComputeAggregated(Type type) {
            super(type);
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

            Variable right = eval(context.args().get(0));

            if (type.isNullable()) {
                Label notNull = context.methodMaker().label();
                right.ifNe(null, notNull);
                workField.set(null);
                done.goto_();
                notNull.here();
            }

            workField.set(compute(type, left, right));

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
        protected NumericalAggregated(Type type) {
            super(type);
        }

        @Override
        public NumericalAggregated validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                                            Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, argTypes.length, reason)) {
                return null;
            }

            Type type = argTypes[0];

            if (!type.isNumber()) {
                reason.accept("argument must be a number");
                return null;
            }

            return validate(type, reason);
        }

        protected abstract NumericalAggregated validate(Type type, Consumer<String> reason);
    }

    /**
     * Defines an aggregate function which performs numerical computation against a simple work
     * field, skipping over null values.
     */
    public abstract static class NullSkipNumericalAggregated extends NumericalAggregated {
        // Used to count the number of non-null values.
        private String mCountFieldName;

        protected NullSkipNumericalAggregated(Type type) {
            super(type);
        }

        @Override
        public void begin(GroupContext context) {
            if (!nullSkip()) {
                super.begin(context);
                return;
            }

            Field countField;
            if (requireCountField()) {
                countField = context.newWorkField(long.class);
                mCountFieldName = countField.name();
            } else {
                countField = null;
            }

            Variable arg = eval(context.args().get(0));
            mWorkFieldName = context.newWorkField(type().clazz()).set(arg).name();

            if (countField != null) {
                MethodMaker mm = context.methodMaker();
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
        public void accumulate(GroupContext context) {
            if (!nullSkip()) {
                super.accumulate(context);
                return;
            }

            var workField = workField(context);
            Variable left = workField.get();
            Variable right = eval(context.args().get(0));

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
        protected Variable eval(LazyValue arg) {
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

        protected NullZeroNumericalAggregated(Type type) {
            super(type);
        }

        @Override
        public void begin(GroupContext context) {
            if (!nullAsZero()) {
                super.begin(context);
                return;
            }

            Field countField;
            if (requireCountField()) {
                countField = context.newWorkField(long.class);
                mCountFieldName = countField.name();
            } else {
                countField = null;
            }

            Type type = type();
            Field workField = context.newWorkField(type.clazz());
            mWorkFieldName = workField.name();

            Variable arg = eval(context.args().get(0));

            MethodMaker mm = context.methodMaker();
            Label notNull = mm.label();
            arg.ifNe(null, notNull);
            Arithmetic.zero(workField);
            if (countField != null) {
                countField.set(0L);
            }
            Label done = mm.label().goto_();
            notNull.here();
            workField.set(arg);
            if (countField != null) {
                countField.set(1L);
            }
            done.here();
        }

        @Override
        public void accumulate(GroupContext context) {
            if (!nullAsZero()) {
                super.accumulate(context);
                return;
            }

            var workField = workField(context);
            Variable arg = eval(context.args().get(0));

            MethodMaker mm = context.methodMaker();
            Label done = mm.label();
            arg.ifEq(null, done);
            workField.set(compute(type(), workField, arg));
            if (mCountFieldName != null) {
                context.methodMaker().field(mCountFieldName).inc(1L);
            }
            done.here();
        }

        @Override
        protected Variable eval(LazyValue arg) {
            return !nullAsZero() ? super.eval(arg) : arg.eval(true);
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

        private boolean nullAsZero() {
            Type type = type();
            return type.isNullable() && Arithmetic.canZero(type.clazz());
        }
    }
}
