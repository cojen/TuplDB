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

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import java.util.concurrent.ThreadLocalRandom;

import java.util.function.Consumer;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

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
                                           List<Expr> args, Map<String, Expr> namedArgs,
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
        public coalesce validate(List<Expr> args, Map<String, Expr> namedArgs,
                                 Consumer<String> reason)
        {
            if (!checkNumArgs(1, Integer.MAX_VALUE, args.size(), reason)) {
                return null;
            }

            Type type = args.get(0).type();

            for (int i=1; i<args.size(); i++) {
                type = type.commonType(args.get(i).type(), -1);
                if (type == null) {
                    reason.accept("no common type");
                    return null;
                }
            }

            ListIterator<Expr> it = args.listIterator();
            while (it.hasNext()) {
                it.set(it.next().asType(type));
            }

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
     * Defines a function which returns the second argument when the first argument is true,
     * else returns the third argument.
     */
    private static class iif extends FunctionApplier.Plain {
        iif(Type type) {
            super(type);
        }

        @Override
        public iif validate(List<Expr> args, Map<String, Expr> namedArgs,
                            Consumer<String> reason)
        {
            if (!checkNumArgs(3, 3, args.size(), reason)) {
                return null;
            }

            Type type = args.get(0).type();

            if (!type.isBoolean()) {
                reason.accept("first argument must be a boolean type");
                return null;
            }

            type = args.get(1).type().commonType(args.get(2).type(), -1);

            if (type == null) {
                reason.accept("no common type");
                return null;
            }

            args.set(1, args.get(1).asType(type));
            args.set(2, args.get(2).asType(type));

            return new iif(type);
        }

        @Override
        public void apply(FunctionContext context, Variable resultVar) {
            List<LazyValue> args = context.args();

            MethodMaker mm = context.methodMaker();
            Label pass = mm.label();
            Label fail = mm.label();
            Label done = mm.label();

            args.get(0).evalFilter(pass, fail);
            pass.here();
            resultVar.set(args.get(1).eval(false));
            done.goto_();
            fail.here();
            resultVar.set(args.get(2).eval(false));
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
        public random validate(List<Expr> args, Map<String, Expr> namedArgs,
                               Consumer<String> reason)
        {
            if (!checkNumArgs(0, 2, args.size(), reason)) {
                return null;
            }

            Type type;

            if (args.size() == 0) {
                type = BasicType.make(double.class, TYPE_DOUBLE);
            } else {
                for (Expr arg : args) {
                    if (!checkArg(arg.type(), reason)) {
                        return null;
                    }
                }

                type = args.get(0).type();

                if (args.size() == 2) {
                    type = type.commonType(args.get(1).type(), -1);
                    if (type == null) {
                        reason.accept("no common type");
                        return null;
                    }
                    if (!checkArg(type, reason)) {
                        return null;
                    }
                    ListIterator<Expr> it = args.listIterator();
                    while (it.hasNext()) {
                        it.set(it.next().asType(type));
                    }
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
        public count validate(List<Expr> args, Map<String, Expr> namedArgs,
                              Consumer<String> reason)
        {
            if (!checkNumArgs(0, 1, args.size(), reason)) {
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
        public first validate(List<Expr> args, Map<String, Expr> namedArgs,
                              Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, args.size(), reason)) {
                return null;
            }
            return new first(args.get(0).type());
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
        public last validate(List<Expr> args, Map<String, Expr> namedArgs,
                             Consumer<String> reason)
        {
            if (!checkNumArgs(1, 1, args.size(), reason)) {
                return null;
            }
            return new last(args.get(0).type());
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
        protected min validate(Type type, Map<String, Expr> namedArgs,
                               Consumer<String> reason)
        {
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
        protected max validate(Type type, Map<String, Expr> namedArgs,
                               Consumer<String> reason)
        {
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
            this(type, type);
        }

        private sum(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        protected sum validate(Type type, Map<String, Expr> namedArgs,
                               Consumer<String> reason)
        {
            Class<?> clazz = type.clazz();
            int typeCode = type.plainTypeCode();

            switch (typeCode) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT -> {
                    typeCode = TYPE_ULONG;
                    clazz = long.class;
                }
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT -> {
                    typeCode = TYPE_LONG;
                    clazz = long.class;
                }
                case TYPE_FLOAT -> {
                    typeCode = TYPE_DOUBLE;
                    clazz = double.class;
                }
                case TYPE_ULONG, TYPE_LONG, TYPE_DOUBLE, TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    // big enough
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            return new sum(BasicType.make(clazz, typeCode), type);
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
        avg(Type type) {
            this(type, type);
        }

        private avg(Type type, Type originalType) {
            super(type, originalType);
        }

        @Override
        public boolean hasNamedParameters() {
            // FIXME: Use something other than hasNamedParameters?
            return true;
        }

        @Override
        protected FunctionApplier validate(final Type type, Map<String, Expr> namedArgs,
                                           Consumer<String> reason)
        {
            int typeCode;
            Class<?> clazz;

            // Note: Result type is nullable when the source is nullable, because the count can
            // be zero. Division by zero with double can be represented as NaN, but BigDecimal
            // has no such representation. For consistency, null is used instead.

            switch (type.plainTypeCode()) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT, TYPE_ULONG,
                     TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
                     TYPE_FLOAT, TYPE_DOUBLE ->
                {
                    typeCode = TYPE_DOUBLE;
                    if (type.isNullable()) {
                        typeCode |= Type.TYPE_NULLABLE;
                        clazz = Double.class;
                    } else {
                        clazz = double.class;
                    }
                }
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    typeCode = TYPE_BIG_DECIMAL;
                    clazz = BigDecimal.class;
                    if (type.isNullable()) {
                        typeCode |= Type.TYPE_NULLABLE;
                    }
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            // FIXME: Common code; only check for specific names; verify types; reject conflicts.
            String rangeMode = "rows";
            Expr rangeExpr = namedArgs.get(rangeMode);

            if (!includesCurrent(rangeExpr)) {
                // Results can be null.
                typeCode |= Type.TYPE_NULLABLE;
                if (clazz == double.class) {
                    clazz = Double.class;
                }
            }

            Type resultType = BasicType.make(clazz, typeCode);

            if (rangeExpr != null) {
                return new StandardWindowFunctions.avg(resultType, type, rangeMode);
            }

            return new avg(resultType, type);
        }

        /**
         * Returns true if the given expression represents a range and is guaranteed to include
         * the current row of a window frame, which is zero.
         */
        private static boolean includesCurrent(Expr expr) {
            return expr instanceof ConstantExpr c
                && c.value() instanceof Range r
                && r.start() <= 0 && 0 <= r.end();
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
            MethodMaker mm = context.methodMaker();

            Variable sum = super.finish(context);
            Variable count = countField(context);
            Variable result = mm.var(type().clazz());

            Label done = null;

            if (originalType().isNullable()) {
                // Handle division by zero.
                count = count.get();
                Label notZero = mm.label();
                count.ifNe(0, notZero);
                result.set(null);
                done = mm.label().goto_();
                notZero.here();
            }

            if (type().clazz() == BigDecimal.class) {
                count = mm.var(BigDecimal.class).invoke("valueOf", count);
            } else {
                count = count.cast(double.class);
            }

            result.set(Arithmetic.eval(type(), Token.T_DIV, sum, count));

            if (done != null) {
                done.here();
            }

            return result;
        }
    }

    /**
     * Simple grouped function which yields the source row. Only used for testing and
     * should be removed or else be hidden and used automatically.
     */
    // FIXME: Must be buffered in case another column isn't ready yet.
    private static class self extends FunctionApplier.Grouped {
        // FIXME: Define a shared context field for this? I think I need a ready field only
        // when the projection doesn't have any grouped functions. Consider these cases:
        // {; }  or  {; a=1}  or  {; a=1, b=grn()}  or  {; a=self(active), b=grn()}
        private String mReadyFieldName;
        private String mValueFieldName;

        self(Type type) {
            super(type);
        }

        @Override
        public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                        Consumer<String> reasons)
        {
            if (!checkNumArgs(1, 1, args.size(), reasons)) {
                return null;
            }

            return new self(args.get(0).type());
        }

        @Override
        public void begin(GroupContext context) {
            mReadyFieldName = context.newWorkField(boolean.class).set(true).name();
            var value = context.args().get(0).eval(true);
            mValueFieldName = context.newWorkField(type().clazz()).set(value).name();
        }

        @Override
        public void accumulate(GroupContext context) {
            context.methodMaker().field(mReadyFieldName).set(true);
            var value = context.args().get(0).eval(true);
            context.methodMaker().field(mValueFieldName).set(value);
        }

        @Override
        public void finished(GroupContext context) {
        }

        @Override
        public void check(GroupContext context) {
            MethodMaker mm = context.methodMaker();
            mm.field(mReadyFieldName).ifFalse(() -> mm.return_(null));
        }

        @Override
        public Variable step(GroupContext context) {
            context.methodMaker().field(mReadyFieldName).set(false);
            return context.methodMaker().field(mValueFieldName);
        }
    }

    /**
     * Simple grouped function that yields the group row number. Only used for testing and
     * should be removed eventually.
     */
    private static class grn extends FunctionApplier.Grouped {
        private String mReadyFieldName;

        grn(Type type) {
            super(type);
        }

        @Override
        public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                        Consumer<String> reasons)
        {
            if (!checkNumArgs(0, 0, args.size(), reasons)) {
                return null;
            }

            return new grn(BasicType.make(long.class, Type.TYPE_LONG));
        }

        @Override
        public void begin(GroupContext context) {
            mReadyFieldName = context.newWorkField(boolean.class).set(true).name();
        }

        @Override
        public void accumulate(GroupContext context) {
            context.methodMaker().field(mReadyFieldName).set(true);
        }

        @Override
        public void finished(GroupContext context) {
        }

        @Override
        public void check(GroupContext context) {
            MethodMaker mm = context.methodMaker();
            mm.field(mReadyFieldName).ifFalse(() -> mm.return_(null));
        }

        @Override
        public Variable step(GroupContext context) {
            context.methodMaker().field(mReadyFieldName).set(false);
            return context.groupRowNum();
        }
    }
}
