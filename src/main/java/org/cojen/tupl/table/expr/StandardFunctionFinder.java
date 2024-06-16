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

import java.lang.reflect.Modifier;

import java.math.BigDecimal;

import java.util.function.Consumer;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.Converter;
import org.cojen.tupl.table.SoftCache;

import static org.cojen.tupl.table.expr.Type.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class StandardFunctionFinder extends SoftCache<String, Object, Object>
    implements FunctionFinder
{
    public static final FunctionFinder THE = new StandardFunctionFinder();

    @Override
    public FunctionApplier tryFindFunction(String name,
                                           Type[] argTypes, String[] argNames, Object[] args,
                                           Consumer<String> reason)
    {
        // Note: Only the function name is examined for now. The FunctionApplier.validate
        // method performs additional parameter matching checks.
        return obtain(name, null) instanceof FunctionApplier fa ? fa : null;
    }

    @Override
    protected Object newValue(String name, Object unused) {
        try {
            Class<?> clazz = Class.forName(getClass().getName() + '$' + name);
            if (Modifier.isAbstract(clazz.getModifiers())) {
                // Negative cache.
                return new Object();
            }
            return (FunctionApplier) clazz
                .getDeclaredConstructor(Type.class).newInstance((Type) null);
        } catch (ClassNotFoundException e) {
            // Negative cache.
            return new Object();
        } catch (Exception e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Defines a function which evaluates the first non-null argument.
     */
    private static class coalesce extends FunctionApplier.Plain {
        coalesce(Type type) {
            super(type);
        }

        @Override
        public coalesce validate(Type[] argTypes, String[] argNames, Consumer<String> reason) {
            if (!checkNumArgs(0, Integer.MAX_VALUE, argTypes.length, reason)) {
                return null;
            }

            Type type = argTypes[0];

            for (int i=1; i<argTypes.length; i++) {
                type = type.commonType(argTypes[i], -1);
                if (type == null) {
                    reason.accept("no common type");
                    return null;
                }
            }

            for (int i=0; i<argTypes.length; i++) {
                argTypes[i] = type;
            }

            return new coalesce(type);
        }

        @Override
        public void apply(Context context, LazyValue[] args, Variable resultVar) {
            if (resultVar.classType().isPrimitive()) {
                // No arguments will be null, so just return the first one.
                resultVar.set(args[0].eval(true));
                return;
            }

            MethodMaker mm = resultVar.clear().methodMaker();
            Label done = mm.label();

            for (int i=0; i<args.length; i++) {
                LazyValue arg = args[i];

                if (!arg.isConstant()) {
                    var argVar = arg.eval(i == 0);
                    Label next = mm.label();
                    argVar.ifEq(null, next);
                    resultVar.set(argVar);
                    done.goto_();
                    next.here();
                } else if (arg.constantValue() != null) {
                    resultVar.set(arg.eval(i == 0));
                    // Once a constant non-null value is reached, skip the rest.
                    break;
                }
            }

            done.here();
        }
    }

    /**
     * Defines an aggregated function which evaluates the argument for only the first row in a
     * group.
     */
    static class first extends FunctionApplier.BasicAggregated {
        first(Type type) {
            super(type);
        }

        @Override
        public first validate(Type[] argTypes, String[] argNames, Consumer<String> reason) {
            if (!checkNumArgs(1, 1, argTypes.length, reason)) {
                return null;
            }
            return new first(argTypes[0]);
        }

        @Override
        public void accumulate(Context context, LazyValue[] args) {
        }
    }

    /**
     * Defines an aggregated function which evaluates the argument for all rows in the group,
     * but only the last one is used in the aggregate result.
     */
    private static class last extends FunctionApplier.BasicAggregated {
        last(Type type) {
            super(type);
        }

        @Override
        public last validate(Type[] argTypes, String[] argNames, Consumer<String> reason) {
            if (!checkNumArgs(1, 1, argTypes.length, reason)) {
                return null;
            }
            return new last(argTypes[0]);
        }

        // Note: The Aggregator interface doesn't provide a convenient (or efficient) way of
        // accessing just the last row in the group. Instead, the arg is evaluated for each row
        // and stored in the field. This can be expensive if evaluating the arg is expensive.

        @Override
        public void accumulate(Context context, LazyValue[] args) {
            workField(context).set(args[0].eval(true));
        }
    }

    /**
     * Defines an aggregated function which yields the smallest argument in the group.
     */
    private static class min extends FunctionApplier.NumericalAggregated {
        min(Type type) {
            super(type);
        }

        @Override
        protected min validate(Type type, Consumer<String> reason) {
            return new min(type);
        }

        @Override
        protected Variable compute(Type type, Variable left, Variable right) {
            return Arithmetic.min(type, left, right);
        }
    }

    /**
     * Defines an aggregated function which yields the largest argument in the group.
     */
    private static class max extends FunctionApplier.NumericalAggregated {
        max(Type type) {
            super(type);
        }

        @Override
        protected max validate(Type type, Consumer<String> reason) {
            return new max(type);
        }

        @Override
        protected Variable compute(Type type, Variable left, Variable right) {
            return Arithmetic.max(type, left, right);
        }
    }

    /**
     * Defines an aggregated function which adds together the evaluated argument for all rows
     * in the group.
     */
    private static class sum extends FunctionApplier.NumericalAggregated {
        sum(Type type) {
            super(type);
        }

        @Override
        protected sum validate(Type type, Consumer<String> reason) {
            Class<?> clazz = type.clazz();
            int typeCode = type.plainTypeCode();

            switch (typeCode) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT -> {
                    typeCode = TYPE_ULONG;
                    clazz = type.isNullable() ? Long.class : long.class;
                }
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT -> {
                    typeCode = TYPE_LONG;
                    clazz = type.isNullable() ? Long.class : long.class;
                }
                case TYPE_FLOAT -> {
                    typeCode = TYPE_DOUBLE;
                    clazz = type.isNullable() ? Double.class : double.class;
                }
                case TYPE_ULONG, TYPE_LONG, TYPE_DOUBLE, TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    // big enough
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            typeCode |= type.modifiers();

            return new sum(BasicType.make(clazz, typeCode));
        }

        @Override
        protected Variable compute(Type type, Variable left, Variable right) {
            return Arithmetic.eval(type, Token.T_PLUS, left, right);
        }
    }

    /**
     * Defines an aggregated function computes the average argument value against all rows in
     * the group.
     */
    private static class avg extends FunctionApplier.NumericalAggregated {
        private final Type mOriginalType;

        avg(Type type) {
            this(type, type);
        }

        private avg(Type type, Type originalType) {
            super(type);
            mOriginalType = originalType;
        }

        @Override
        public Variable finish(Context context) {
            Variable sum = super.finish(context);

            Variable count = context.groupRowNum();
            if (type().clazz() == BigDecimal.class) {
                count = count.methodMaker().var(BigDecimal.class).invoke("valueOf", count);
            } else {
                count = count.cast(double.class);
            }

            return Arithmetic.eval(type(), Token.T_DIV, sum, count);
        }

        @Override
        protected Variable eval(LazyValue arg) {
            Variable value = arg.eval(true);
            MethodMaker mm = value.methodMaker();
            var converted = mm.var(type().clazz());
            Converter.convertLossy(mm, mOriginalType, value, type(), converted);
            return converted;
        }

        @Override
        protected avg validate(final Type type, Consumer<String> reason) {
            Class<?> clazz = type.clazz();
            int typeCode = type.plainTypeCode();

            switch (typeCode) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT, TYPE_ULONG,
                     TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
                     TYPE_FLOAT, TYPE_DOUBLE ->
                {
                    typeCode = TYPE_DOUBLE;
                    clazz = type.isNullable() ? Double.class : double.class;
                }
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    typeCode = TYPE_BIG_DECIMAL;
                    clazz = BigDecimal.class;
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            typeCode |= type.modifiers();

            return new avg(BasicType.make(clazz, typeCode), type);
        }

        @Override
        protected Variable compute(Type type, Variable left, Variable right) {
            return Arithmetic.eval(type, Token.T_PLUS, left, right);
        }
    }
}
