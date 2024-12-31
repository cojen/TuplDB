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
import java.math.BigInteger;

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
                                 Map<String, ProjExpr> projectionMap,
                                 Consumer<String> reason)
        {
            if (!checkNumArgs(1, Integer.MAX_VALUE, args.size(), reason)) {
                return null;
            }

            Type type = args.getFirst().type();

            for (int i=1; i<args.size(); i++) {
                type = type.commonTypeLenient(args.get(i).type());
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
                resultVar.set(args.getFirst().eval(true));
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
                            Map<String, ProjExpr> projectionMap,
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

            type = args.get(1).type().commonTypeLenient(args.get(2).type());

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
                               Map<String, ProjExpr> projectionMap,
                               Consumer<String> reason)
        {
            if (!checkNumArgs(0, 2, args.size(), reason)) {
                return null;
            }

            Type type;

            if (args.isEmpty()) {
                type = BasicType.make(double.class, TYPE_DOUBLE);
            } else {
                for (Expr arg : args) {
                    if (!checkArg(arg.type(), reason)) {
                        return null;
                    }
                }

                type = args.get(0).type();

                if (args.size() == 2) {
                    type = type.commonTypeStrict(args.get(1).type());
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
        public boolean hasNamedParameters() {
            return true;
        }

        @Override
        public FunctionApplier validate(List<Expr> args, Map<String, Expr> namedArgs,
                                        Map<String, ProjExpr> projectionMap,
                                        Consumer<String> reason)
        {
            if (!checkNumArgs(0, 1, args.size(), reason)) {
                return null;
            }

            Type resultType = BasicType.make(long.class, Type.TYPE_LONG);

            if (!hasFrame(namedArgs)) {
                return new count(resultType);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            Type originalType = args.isEmpty() ? BasicType.BOOLEAN : args.getFirst().type();
            Type valueType = originalType;

            return new StandardWindowFunctions.count(resultType, valueType, originalType, frame);
        }

        @Override
        public void init(GroupContext context) {
            List<LazyValue> args = context.args();
            if (!args.isEmpty() && args.getFirst().expr().isNullable()) {
                mCountFieldName = context.newWorkField(type().clazz()).name();
            }
        }

        @Override
        public void begin(GroupContext context) {
            if (mCountFieldName == null) {
                return;
            }
            LazyValue arg = context.args().getFirst();
            MethodMaker mm = context.methodMaker();
            Field countField = mm.field(mCountFieldName);
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
            var result = context.args().getFirst().eval(true);
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
            super(type, type);
        }

        @Override
        public boolean hasNamedParameters() {
            return true;
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

            if (!hasFrame(namedArgs)) {
                return new first(type);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            return new StandardWindowFunctions.first(type, type, type, frame);
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
            super(type, type);
        }

        @Override
        public boolean hasNamedParameters() {
            return true;
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

            if (!hasFrame(namedArgs)) {
                return new last(type);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            return new StandardWindowFunctions.last(type, type, type, frame);
        }

        // Note: The Aggregator interface doesn't provide a convenient (or efficient) way of
        // accessing just the last row in the group. Instead, the arg is evaluated for each row
        // and stored in the field. This can be expensive if evaluating the arg is expensive.

        @Override
        public void accumulate(GroupContext context) {
            workField(context).set(context.args().getFirst().eval(true));
        }
    }

    /**
     * Defines an aggregated function which yields the smallest argument in the group.
     */
    private static class min extends FunctionApplier.NullSkipNumericalAggregated {
        min(Type type) {
            super(type, type);
        }

        @Override
        public boolean hasNamedParameters() {
            return true;
        }

        @Override
        protected FunctionApplier validate(Type type,
                                           List<Expr> args, Map<String, Expr> namedArgs,
                                           Map<String, ProjExpr> projectionMap,
                                           Consumer<String> reason)
        {
            if (!hasFrame(namedArgs)) {
                return new min(type);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            Type resultType = type;

            if (!frame.includesCurrent()) {
                // The frame might go out of bounds, and so the affected values should be null.
                resultType = resultType.nullable();
            }

            return new StandardWindowFunctions.min(resultType, type, type, frame);
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
            super(type, type);
        }

        @Override
        public boolean hasNamedParameters() {
            return true;
        }

        @Override
        protected FunctionApplier validate(Type type,
                                           List<Expr> args, Map<String, Expr> namedArgs,
                                           Map<String, ProjExpr> projectionMap,
                                           Consumer<String> reason)
        {
            if (!hasFrame(namedArgs)) {
                return new max(type);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            Type resultType = type;

            if (!frame.includesCurrent()) {
                // The frame might go out of bounds, and so the affected values should be null.
                resultType = resultType.nullable();
            }

            return new StandardWindowFunctions.max(resultType, type, type, frame);
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
        public boolean hasNamedParameters() {
            return true;
        }

        @Override
        protected FunctionApplier validate(Type originalType,
                                           List<Expr> args, Map<String, Expr> namedArgs,
                                           Map<String, ProjExpr> projectionMap,
                                           Consumer<String> reason)
        {
            Class<?> clazz = originalType.clazz();
            int typeCode = originalType.plainTypeCode();

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
                case TYPE_ULONG, TYPE_LONG -> {
                    typeCode = TYPE_BIG_INTEGER;
                    clazz = BigInteger.class;
                }
                case TYPE_DOUBLE, TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    // big enough
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            Type resultType = BasicType.make(clazz, typeCode);

            if (!hasFrame(namedArgs)) {
                return new sum(resultType, originalType);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            Type valueType = resultType;

            if (originalType.isNullable()) {
                // Nulls must be preserved in the value buffer and not be treated as zero.
                valueType = valueType.nullable();
            }

            return new StandardWindowFunctions.sum(resultType, valueType, originalType, frame);
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
            return true;
        }

        @Override
        protected FunctionApplier validate(Type originalType,
                                           List<Expr> args, Map<String, Expr> namedArgs,
                                           Map<String, ProjExpr> projectionMap,
                                           Consumer<String> reason)
        {
            Class<?> clazz;
            int typeCode;

            switch (originalType.plainTypeCode()) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT, TYPE_ULONG,
                     TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
                     TYPE_FLOAT, TYPE_DOUBLE ->
                {
                    clazz = double.class;
                    typeCode = TYPE_DOUBLE;
                }
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> {
                    clazz = BigDecimal.class;
                    typeCode = TYPE_BIG_DECIMAL;
                }
                default -> {
                    reason.accept("unsupported argument type");
                    return null;
                }
            }

            Type resultType = BasicType.make(clazz, typeCode);

            if (originalType.isNullable()) {
                // The result type is nullable when the source is nullable, because the count
                // can be zero. Division by zero with double can be represented as NaN, but
                // BigDecimal has no such representation. Null is used for consistency.
                resultType = resultType.nullable();
            }

            if (!hasFrame(namedArgs)) {
                return new avg(resultType, originalType);
            }

            WindowFunction.Frame frame = accessFrame(args, namedArgs, projectionMap, reason);
            if (frame == null) {
                // The reason should have been provided by the accessFrame method.
                return null;
            }

            Type valueType = resultType;

            if (!frame.includesCurrent()) {
                // Results can be null because the effective frame count can be zero. Note that
                // the value type doesn't need an early conversion to a nullable type.
                resultType = resultType.nullable();
            }

            return new StandardWindowFunctions.avg(resultType, valueType, originalType, frame);
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
}
