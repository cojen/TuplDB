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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;
import java.util.function.BiFunction;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.RowUtils;

/**
 * Defines an expression which calls a function.
 *
 * @author Brian S. O'Neill
 */
public final class CallExpr extends Expr {
    public static CallExpr make(int startPos, int endPos,
                                String name, List<Expr> args, Map<String, Expr> namedArgs,
                                FunctionApplier applier)
    {
        return new CallExpr(startPos, endPos, name, args, namedArgs, applier);
    }

    private final String mName;
    private final List<Expr> mArgs;
    private final Map<String, Expr> mNamedArgs;
    private final FunctionApplier mOriginalApplier, mApplier;

    private CallExpr(int startPos, int endPos,
                     String name, List<Expr> args, Map<String, Expr> namedArgs,
                     FunctionApplier applier)
    {
        super(startPos, endPos);

        mName = name;
        mOriginalApplier = applier;

        Type[] argTypes;
        {
            int num = args.size();
            argTypes = new Type[num];
            int i = 0;
            for (Expr arg : args) {
                argTypes[i++] = arg.type();
            }
            assert i == num;
        }

        Map<String, Type> namedArgTypes;
        {
            if (namedArgs.isEmpty()) {
                namedArgTypes = Map.of();
            } else {
                namedArgTypes = new LinkedHashMap<>(namedArgs.size() << 1);
                namedArgs.forEach((n, e) -> namedArgTypes.put(n, e.type()));
            }
        }

        var reasons = new ArrayList<String>(1);

        if (!applier.hasNamedParameters() && !namedArgs.isEmpty()) {
            reasons.add("it doesn't define any named parameters");
            mApplier = null;
        } else {
            mApplier = applier.validate(argTypes, namedArgTypes, reasons::add);
        }

        if (mApplier != null) {
            // Perform any necessary type conversions to the arguments.
            args = replaceElements(args, 0, (i, arg) -> arg.asType(argTypes[i]));
        }

        mArgs = args;
        mNamedArgs = namedArgs;

        if (applier instanceof FunctionApplier.Aggregated) {
            for (Expr arg : args) {
                if (arg.isAccumulating()) {
                    reasons.add("depends on an expression which accumulates group results");
                    break;
                }
            }
        }

        if (mApplier == null || !reasons.isEmpty()) {
            var b = new StringBuilder().append("Cannot call ");
            RowUtils.appendQuotedString(b, name);
            b.append(" function");

            if (!reasons.isEmpty()) {
                b.append(": ");
                for (int i=0; i<reasons.size(); i++) {
                    if (i > 0) {
                        b.append(", ");
                    }
                    b.append(reasons.get(i));
                }
            }

            throw new QueryException(b.toString(), this);
        }
    }

    @Override
    public Type type() {
        return mApplier.type();
    }

    @Override
    public Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
    }

    @Override
    public int maxArgument() {
        int max = 0;
        for (Expr arg : mArgs) {
            max = Math.max(max, arg.maxArgument());
        }
        return max;
    }

    @Override
    public boolean isPureFunction() {
        if (!mApplier.isPureFunction()) {
            return false;
        }

        for (Expr arg : mArgs) {
            if (!arg.isPureFunction()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isNullable() {
        return type().isNullable();
    }

    @Override
    public boolean isConstant() {
        if (!mApplier.isPureFunction()) {
            return false;
        }

        for (Expr arg : mArgs) {
            if (!arg.isConstant()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isGrouping() {
        if (mApplier.isGrouping()) {
            return true;
        }

        for (Expr arg : mArgs) {
            if (arg.isGrouping()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isAccumulating() {
        if (mApplier instanceof FunctionApplier.Accumulator) {
            return true;
        }

        for (Expr arg : mArgs) {
            if (arg.isAccumulating()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isAggregating() {
        if (mApplier instanceof FunctionApplier.Aggregated) {
            return true;
        }

        for (Expr arg : mArgs) {
            if (arg.isAggregating()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public CallExpr asAggregate(Set<String> group) {
        if (mApplier instanceof FunctionApplier.Aggregated) {
            return this;
        }

        List<Expr> args = replaceElements(mArgs, 0, (i, arg) -> arg.asAggregate(group));

        Map<String, Expr> namedArgs = replaceValues
            (mNamedArgs, (k, arg) -> arg.asAggregate(group));

        return args == mArgs && namedArgs == mNamedArgs ? this
            : new CallExpr(startPos(), endPos(), mName, args, namedArgs, mApplier);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }

        List<Expr> args = replaceElements(mArgs, 0, (i, arg) -> arg.replace(replacements));

        Map<String, Expr> namedArgs = replaceValues
            (mNamedArgs, (k, arg) -> arg.replace(replacements));

        return args == mArgs && namedArgs == mNamedArgs ? this
            : new CallExpr(startPos(), endPos(), mName, args, namedArgs, mApplier);
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
        for (Expr arg : mArgs) {
            arg.gatherEvalColumns(c);
        }
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        if (mApplier instanceof FunctionApplier.Plain plain) {
            return withArgs(context, (ctx, args) -> {
                var resultVar = ctx.methodMaker().var(type().clazz());
                plain.apply(args, resultVar);
                return resultVar;
            });
        }

        if (mApplier instanceof FunctionApplier.Aggregated aggregated) {
            withArgs(context.beginContext(), (ctx, args) -> {
                aggregated.begin(ctx, args);
                return null;
            });

            withArgs(context.accumContext(), (ctx, args) -> {
                aggregated.accumulate(ctx, args);
                return null;
            });

            return aggregated.finish(context);
        }

        // FIXME: Support other function types.
        throw new UnsupportedOperationException();
    }

    /**
     * Prepares LazyValue args and passes it to the applier to use.
     */
    private <V> V withArgs(EvalContext context, BiFunction<EvalContext, LazyValue[], V> applier) {
        LazyValue[] args = lazyArgs(context);

        // Must rollback to a savepoint for lazy/eager evaluation to work properly. Arguments
        // which aren't eagerly evaluated will rollback, forcing the underlying expression to
        // be evaluated again later if used again.
        int savepoint = context.refSavepoint();

        V result = applier.apply(context, args);

        context.refRollback(savepoint);

        return result;
    }

    private LazyValue[] lazyArgs(EvalContext context) {
        return mArgs.stream().map(arg -> arg.lazyValue(context)).toArray(LazyValue[]::new);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeString(mName);
            enc.encodeExprs(mArgs);
            enc.encodeReference(mOriginalApplier);
        }
    }

    @Override
    public int hashCode() {
        int hash = mName.hashCode();
        hash = hash * 31 + mArgs.hashCode();
        hash = hash * 31 + mOriginalApplier.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof CallExpr ce
            && mName.equals(ce.mName)
            && mArgs.equals(ce.mArgs)
            && mOriginalApplier.equals(ce.mOriginalApplier);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        b.append(mName).append('(');

        int i = 0;
        for (Expr arg : mArgs) {
            if (i > 0) {
                b.append(", ");
            }
            arg.appendTo(b);
            i++;
        }

        b.append(')');
    }
}
