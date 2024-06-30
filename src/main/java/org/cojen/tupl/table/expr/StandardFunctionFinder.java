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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ThreadLocalRandom;

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
                                           Type[] argTypes, Map<String, Type> namedArgTypes,
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
                return "";
            }
            return (FunctionApplier) clazz
                .getDeclaredConstructor(Type.class).newInstance((Type) null);
        } catch (ClassNotFoundException e) {
            // Negative cache.
            return "";
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
        public coalesce validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                                 Consumer<String> reason)
        {
            if (!checkNumArgs(1, Integer.MAX_VALUE, argTypes.length, reason)) {
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

            Arrays.fill(argTypes, type);

            return new coalesce(type);
        }

        @Override
        public void apply(FunctionContext context, Variable resultVar) {
            List<LazyValue> args = context.args();

            if (resultVar.classType().isPrimitive()) {
                // No arguments will be null, so just return the first one.
                resultVar.set(args.get(0).eval(true));
                return;
            }

            MethodMaker mm = resultVar.clear().methodMaker();
            Label done = mm.label();

            for (int i=0; i<args.size(); i++) {
                LazyValue arg = args.get(i);

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
     * Defines a function which returns a random number in the range [0.0, 1.0). If one
     * argument is provided, the range is [0, arg), and if two arguments are provided, the
     * range is [arg1, arg2).
     */
    private static class random extends FunctionApplier.Plain {
        random(Type type) {
            super(type);
        }

        @Override
        public boolean isPureFunction() {
            return false;
        }

        @Override
        public random validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                               Consumer<String> reason)
        {
            if (!checkNumArgs(0, 2, argTypes.length, reason)) {
                return null;
            }

            Type type;

            if (argTypes.length == 0) {
                type = BasicType.make(double.class, TYPE_DOUBLE);
            } else {
                for (Type t : argTypes) {
                    if (!checkArg(t, reason)) {
                        return null;
                    }
                }

                type = argTypes[0];

                if (argTypes.length == 2) {
                    type = type.commonType(argTypes[1], -1);
                    if (type == null) {
                        reason.accept("no common type");
                        return null;
                    }
                    if (!checkArg(type, reason)) {
                        return null;
                    }
                    Arrays.fill(argTypes, type);
                }
            }

            return new random(type);
        }

        private static boolean checkArg(Type type, Consumer<String> reason) {
            if (!type.isNumber()) {
                reason.accept("argument must be a number");
                return false;
            }

            if (!type.isPrimitive() || type.plainTypeCode() == TYPE_ULONG) {
                reason.accept("unsupported argument type");
                return false;
            }

            return true;
        }

        @Override
        public void apply(FunctionContext context, Variable resultVar) {
            var rndVar = context.methodMaker().var(ThreadLocalRandom.class).invoke("current");

            List<LazyValue> args = context.args();

            if (args.isEmpty()) {
                resultVar.set(rndVar.invoke("nextDouble"));
                return;
            }

            String methodName = "next" +  switch (type().plainTypeCode()) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_BYTE, TYPE_SHORT, TYPE_INT -> "Int";
                case TYPE_UINT, TYPE_LONG -> "Long";
                case TYPE_FLOAT -> "Float";
                case TYPE_DOUBLE -> "Double";
                default -> throw new AssertionError();
            };

            var val1 = args.get(0).eval(true);

            Variable rndResultVar;
            if (args.size() == 1) {
                rndResultVar = rndVar.invoke(methodName, val1);
            } else {
                var val2 = args.get(1).eval(true);
                rndResultVar = rndVar.invoke(methodName, val1, val2);
            }

            resultVar.set(rndResultVar.cast(resultVar));
        }
    }

    /**
     * Defines an aggregated function which counts all rows in the group, or counts all
     * non-null results for one argument.
     */
    private static class count extends FunctionApplier.Aggregated {
        private String mCountFieldName;

        count(Type type) {
            super(type);
        }

        @Override
        public count validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                              Consumer<String> reason)
        {
            if (!checkNumArgs(0, 1, argTypes.length, reason)) {
                return null;
            }
            return new count(BasicType.make(long.class, Type.TYPE_LONG));
        }

        @Override
        public void begin(GroupContext context) {
            List<LazyValue> args = context.args();

            LazyValue arg;
            if (args.isEmpty() || !(arg = args.get(0)).expr().isNullable()) {
                return;
            }

            Field countField = context.newWorkField(type().clazz());
            mCountFieldName = countField.name();

            MethodMaker mm = countField.methodMaker();
            Label notNull = mm.label();
            arg.eval(true).ifNe(null, notNull);
            countField.set(0L);
            Label done = mm.label().goto_();
            notNull.here();
            countField.set(1L);
            done.here();
        }

        @Override
        public void accumulate(GroupContext context) {
            if (mCountFieldName == null) {
                return;
            }
            var result = context.args().get(0).eval(true);
            MethodMaker mm = result.methodMaker();
            Label isNull = mm.label();
            result.ifEq(null, isNull);
            mm.field(mCountFieldName).inc(1L);
            isNull.here();
        }

        @Override
        public Variable finish(GroupContext context) {
            if (mCountFieldName == null) {
                return context.groupRowNum();
            } else {
                return context.methodMaker().field(mCountFieldName);
            }
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
        public first validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                              Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, argTypes.length, reason)) {
                return null;
            }
            return new first(argTypes[0]);
        }

        @Override
        public void accumulate(GroupContext context) {
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
        public last validate(Type[] argTypes, Map<String, Type> namedArgTypes,
                             Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, argTypes.length, reason)) {
                return null;
            }
            return new last(argTypes[0]);
        }

        // Note: The Aggregator interface doesn't provide a convenient (or efficient) way of
        // accessing just the last row in the group. Instead, the arg is evaluated for each row
        // and stored in the field. This can be expensive if evaluating the arg is expensive.

        @Override
        public void accumulate(GroupContext context) {
            workField(context).set(context.args().get(0).eval(true));
        }
    }

    /**
     * Defines an aggregated function which yields the smallest argument in the group.
     */
    private static class min extends FunctionApplier.NullSkipNumericalAggregated {
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
    private static class max extends FunctionApplier.NullSkipNumericalAggregated {
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
    private static class sum extends FunctionApplier.NullZeroNumericalAggregated {
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
    private static class avg extends FunctionApplier.NullZeroNumericalAggregated {
        private final Type mOriginalType;

        avg(Type type) {
            this(type, type);
        }

        private avg(Type type, Type originalType) {
            super(type);
            mOriginalType = originalType;
        }

        @Override
        protected avg validate(final Type type, Consumer<String> reason) {
            int typeCode = type.plainTypeCode();
            Class<?> clazz;

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
        protected Variable eval(LazyValue arg) {
            Variable value = super.eval(arg);
            MethodMaker mm = value.methodMaker();
            var converted = mm.var(type().clazz());
            Converter.convertLossy(mm, mOriginalType, value, type(), converted);
            return converted;
        }

        @Override
        protected Variable compute(Type type, Variable left, Variable right) {
            return Arithmetic.eval(type, Token.T_PLUS, left, right);
        }

        @Override
        protected boolean requireCountField() {
            return true;
        }

        @Override
        public Variable finish(GroupContext context) {
            Variable sum = super.finish(context);

            Variable count = countField(context);
            if (type().clazz() == BigDecimal.class) {
                count = count.methodMaker().var(BigDecimal.class).invoke("valueOf", count);
            } else {
                count = count.cast(double.class);
            }

            return Arithmetic.eval(type(), Token.T_DIV, sum, count);
        }
    }
}
