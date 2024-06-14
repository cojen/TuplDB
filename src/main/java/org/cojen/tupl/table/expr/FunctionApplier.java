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

import java.util.function.Consumer;
import java.util.function.Function;

import org.cojen.maker.Field;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Generates code to invoke a function.
 *
 * @author Brian S. O'Neill
 * @see FunctionFinder
 */
public abstract class FunctionApplier {
    private FunctionApplier() {
    }

    /**
     * Returns this instance or a new instance if the applier requires unsharable state when
     * generating code.
     */
    public FunctionApplier prepare() {
        return this;
    }

    /**
     * Return true if the function needs a row number value, which spans the entire query. Row
     * numbers begin at one.
     */
    public boolean requireRowNum() {
        return false;
    }

    /**
     * Return true if the function needs a group number value, which spans the entire query.
     * Group numbers begin at one.
     */
    public boolean requireGroupNum() {
        return false;
    }

    /**
     * Return true if the function needs a group row number value. Group row numbers begin at
     * one, and they reset back to one for each new group.
     */
    public boolean requireGroupRowNum() {
        return false;
    }

    /**
     * Returns true if the applied function is pure with respect to the current row, producing
     * the same result each time it's applied.
     */
    public boolean isPureFunction() {
        return true;
    }

    /**
     * Validate that the function acceps the given arguments, and returns the type of the
     * function. If any arguments should be converted, directly replace elements of the
     * argTypes array with the desired type.
     *
     * @param argTypes non-null array of argument types
     * @param argNames non-null array of optional argument names; same length as argTypes
     * @param reasons if validation fails, optionally provide reasons
     * @return the function return type or null if validation fails
     */
    public abstract Type validate(Type[] argTypes, String[] argNames, Consumer<String> reasons);

    abstract void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args,
                        Variable rowNum, Variable groupNum, Variable groupRowNum);

    abstract void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args);

    /**
     * A plain function is a plain function.
     */
    public abstract static class Plain extends FunctionApplier {
        @Override
        public Plain prepare() {
            return this;
        }

        /**
         * Called to apply the function. The given arguments are either Variables or constant
         * values.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the last group row number (>= 1L)
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args,
                          Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            apply(retType, retVar, argTypes, args);
        }

        /**
         * Called to apply the function. The given arguments are either Variables or constant
         * values.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }
    }

    abstract static class ByGroup extends FunctionApplier {
        private ByGroup() {
        }

        abstract void prepare(Function<Class, Field> fieldDefiner);

        abstract void reset();
    }

    /**
     * A rolling function operates over groups of rows, producing one output row for each input
     * row, without requiring that the entire group of values be provided up front.
     */
    public abstract static class Rolling extends ByGroup {
        @Override
        public Rolling prepare() {
            return this;
        }

        /**
         * Called when code generation begins. The given definer can be called to produce all
         * of the necessary work fields, and if necessary, the prepare method should initialize
         * them.
         */
        @Override
        public abstract void prepare(Function<Class, Field> fieldDefiner);

        /**
         * Called to reset the work fields before a group begins.
         */
        @Override
        public abstract void reset();

        /**
         * Called to apply the function. The given arguments are either Variables or constant
         * values.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the last group row number (>= 1L)
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args,
                          Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            apply(retType, retVar, argTypes, args);
        }

        /**
         * Called to apply the function. The given arguments are either Variables or constant
         * values.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }
    }

    abstract static class Accumulator extends ByGroup {
        private Accumulator() {
        }

        abstract void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args,
                                 Variable rowNum, Variable groupNum, Variable groupRowNum);

        abstract void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args);
    }

    /**
     * A grouped function operates over groups of rows, producing one output row for each input
     * row, while also requiring that the entire group of values be provided up front.
     */
    public abstract static class Grouped extends Accumulator {
        @Override
        public Grouped prepare() {
            return this;
        }

        /**
         * Called when code generation begins. The given definer can be called to produce all
         * of the necessary work fields, and if necessary, the prepare method should initialize
         * them.
         */
        @Override
        public abstract void prepare(Function<Class, Field> fieldDefiner);

        /**
         * Called to reset the work fields before a group begins.
         */
        @Override
        public abstract void reset();

        /**
         * Called for each row in the group. The given arguments are either Variables or
         * constant values.
         *
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the current group row number (>= 1L)
         */
        @Override
        public void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args,
                               Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            accumulate(mm, argTypes, args);
        }

        /**
         * Called for each row in the group. The given arguments are either Variables or
         * constant values.
         *
         * @param args Variables or constants
         */
        @Override
        public void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }

        /**
         * Called for the last row in the group, and produces the first result.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the current group row number (>= 1L)
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args,
                          Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            apply(retType, retVar, argTypes, args);
        }

        /**
         * Called for the last row in the group, and produces the first result.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }

        /**
         * Called to produce each remaining result.
         *
         * @return a Variable or a constant
         */
        public abstract Object next();
    }

    /**
     * An aggregate function computes an aggregate over a group of rows. One output row is
     * produced for a group of input rows.
     */
    public abstract static class Aggregated extends Accumulator {
        @Override
        public Aggregated prepare() {
            return this;
        }

        /**
         * Called when code generation begins. The given definer can be called to produce all
         * of the necessary work fields, and if necessary, the prepare method should initialize
         * them.
         */
        @Override
        public abstract void prepare(Function<Class, Field> fieldDefiner);

        /**
         * Called to reset the work fields before a group begins.
         */
        @Override
        public abstract void reset();

        /**
         * Called for each row in the group. The given arguments are either Variables or
         * constant values.
         *
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the current group row number (>= 1L)
         */
        @Override
        public void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args,
                               Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            accumulate(mm, argTypes, args);
        }

        /**
         * Called for each row in the group. The given arguments are either Variables or
         * constant values.
         *
         * @param args Variables or constants
         */
        @Override
        public void accumulate(MethodMaker mm, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }

        /**
         * Called for the last row in the group, and produces an aggregate result.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @param rowNum if required, represents the current row number (>= 1L)
         * @param groupNum if required, represents the current group number (>= 1L)
         * @param groupRowNum if required, represents the current group row number (>= 1L)
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args,
                          Variable rowNum, Variable groupNum, Variable groupRowNum)
        {
            apply(retType, retVar, argTypes, args);
        }

        /**
         * Called for the last row in the group, and produces an aggregate result.
         *
         * @param retVar non-null return variable to set
         * @param args Variables or constants
         * @return a Variable or a constant
         */
        @Override
        public void apply(Type retType, Variable retVar, Type[] argTypes, LazyValue[] args) {
            throw new AbstractMethodError();
        }
    }
}
