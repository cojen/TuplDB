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
         * Generate code which branches to the given label if the step method isn't ready to
         * produce a result.
         */
        public abstract void check(GroupContext context, Label notReady);

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
     * Defines an aggregate function which needs one work field.
     */
    public abstract static class BasicAggregated extends Aggregated {
        private String mFieldName;

        protected BasicAggregated(Type type) {
            super(type);
        }

        @Override
        public void begin(GroupContext context) {
            LazyValue arg = context.args().get(0);
            mFieldName = context.newWorkField(type().clazz()).set(eval(arg)).name();
        }

        @Override
        public Variable finish(GroupContext context) {
            return workField(context);
        }

        protected final Field workField(GroupContext context) {
            return context.methodMaker().field(mFieldName);
        }

        protected Variable eval(LazyValue arg) {
            return arg.eval(true);
        }
    }

    /**
     * Defines an aggregate function which computes against one work field.
     */
    public abstract static class ComputeAggregated extends BasicAggregated {
        protected ComputeAggregated(Type type) {
            super(type);
        }

        @Override
        public void accumulate(GroupContext context) {
            Field field = workField(context);
            Type type = type();
            Label done = null;

            Variable left = field;

            if (type.isNullable()) {
                left = left.get();
                done = context.methodMaker().label();
                left.ifEq(null, done);
            }

            Variable right = eval(context.args().get(0));

            if (type.isNullable()) {
                Label notNull = context.methodMaker().label();
                right.ifNe(null, notNull);
                field.set(null);
                done.goto_();
                notNull.here();
            }

            field.set(compute(type, left, right));

            if (done != null) {
                done.here();
            }
        }

        /**
         * Generate code to compute an incremental result against two non-null values.
         */
        protected abstract Variable compute(Type type, Variable left, Variable right);
    }

    /**
     * Defines an aggregate function which performs numerical computation against one work
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
}
