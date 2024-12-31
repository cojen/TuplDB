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
import java.util.function.Function;
import java.util.function.Predicate;

import org.cojen.maker.FieldMaker;
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
        return make(startPos, endPos, name, args, namedArgs, applier, null);
    }

    /**
     * @param projectionMap a linked map of all the projection expressions encountered by the
     * Parser so far within the current projection group; is used by WindowFunction for
     * supporting the "range" frame mode which requires an explicit value ordering
     */
    public static CallExpr make(int startPos, int endPos,
                                String name, List<Expr> args, Map<String, Expr> namedArgs,
                                FunctionApplier applier, Map<String, ProjExpr> projectionMap)
    {
        return new CallExpr(startPos, endPos, name, args, namedArgs, applier, projectionMap);
    }

    private final String mName;
    private final List<Expr> mArgs;
    private final Map<String, Expr> mNamedArgs;
    private final FunctionApplier mOriginalApplier, mApplier;

    private CallExpr(int startPos, int endPos,
                     String name, List<Expr> args, Map<String, Expr> namedArgs,
                     FunctionApplier applier, Map<String, ProjExpr> projectionMap)
    {
        super(startPos, endPos);

        mName = name;
        mOriginalApplier = applier;

        var reasons = new ArrayList<String>(1);

        if (!applier.hasNamedParameters() && !namedArgs.isEmpty()) {
            reasons.add("it doesn't define any named parameters");
            mApplier = null;
        } else {
            mApplier = applier.validate(args, namedArgs, projectionMap, reasons::add);
        }

        mArgs = args;
        mNamedArgs = namedArgs;

        if (applier instanceof FunctionApplier.Aggregated) {
            if (isAccumulating(args) || isAccumulating(namedArgs.values())) {
                reasons.add("depends on an expression which accumulates group results");
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

            throw queryException(b.toString());
        }
    }

    private static boolean isAccumulating(Iterable<Expr> args) {
        for (Expr arg : args) {
            if (arg.isAccumulating()) {
                return true;
            }
        }
        return false;
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
        return mApplier.isPureFunction() && isTestF(Expr::isPureFunction);
    }

    @Override
    public boolean isNullable() {
        return type().isNullable();
    }

    @Override
    public boolean isConstant() {
        return mApplier.isPureFunction() && isTestF(Expr::isConstant);
    }

    @Override
    public boolean isOrderDependent() {
        return mApplier.isOrderDependent() || isTestT(Expr::isOrderDependent);
    }

    @Override
    public boolean isGrouping() {
        return mApplier.isGrouping() || isTestT(Expr::isGrouping);
    }

    @Override
    public boolean isAccumulating() {
        return mApplier instanceof FunctionApplier.Accumulator || isTestT(Expr::isAccumulating);
    }

    @Override
    public boolean isAggregating() {
        return mApplier instanceof FunctionApplier.Aggregated || isTestT(Expr::isAggregating);
    }

    /**
     * Tests the predicate against mArgs and mNamedArgs, short-circuiting on true like an 'or'
     * expression.
     */
    private boolean isTestT(Predicate<Expr> pred) {
        for (Expr arg : mArgs) {
            if (pred.test(arg)) {
                return true;
            }
        }

        for (Expr arg : mNamedArgs.values()) {
            if (pred.test(arg)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Tests the predicate against mArgs and mNamedArgs, short-circuiting on false like an
     * 'and' expression.
     */
    private boolean isTestF(Predicate<Expr> pred) {
        for (Expr arg : mArgs) {
            if (!pred.test(arg)) {
                return false;
            }
        }

        for (Expr arg : mNamedArgs.values()) {
            if (!pred.test(arg)) {
                return false;
            }
        }

        return true;
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
            : new CallExpr(startPos(), endPos(), mName, args, namedArgs, mApplier, null);
    }

    @Override
    public CallExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        if (mApplier instanceof FunctionApplier.Grouped) {
            return this;
        }

        List<Expr> args = replaceElements(mArgs, 0, (i, arg) -> arg.asWindow(newAssignments));

        Map<String, Expr> namedArgs = replaceValues
            (mNamedArgs, (k, arg) -> arg.asWindow(newAssignments));

        return args == mArgs && namedArgs == mNamedArgs ? this
            : new CallExpr(startPos(), endPos(), mName, args, namedArgs, mApplier, null);
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
            : new CallExpr(startPos(), endPos(), mName, args, namedArgs, mApplier, null);
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
        for (Expr arg : mArgs) {
            arg.gatherEvalColumns(c);
        }
        for (Expr arg : mNamedArgs.values()) {
            arg.gatherEvalColumns(c);
        }
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        if (mApplier instanceof FunctionApplier.Plain plain) {
            return withArgs(context, ctx -> {
                var resultVar = ctx.methodMaker().var(type().clazz());
                plain.apply(ctx, resultVar);
                return resultVar;
            });
        }

        if (mApplier instanceof FunctionApplier.Aggregated aggregated) {
            withArgs(context.initContext(), ctx -> {
                aggregated.init(ctx);
                return null;
            });

            withArgs(context.beginContext(), ctx -> {
                aggregated.begin(ctx);
                return null;
            });

            withArgs(context.accumContext(), ctx -> {
                aggregated.accumulate(ctx);
                return null;
            });

            return aggregated.finish(context);
        }

        if (mApplier instanceof FunctionApplier.Grouped grouped) {
            withArgs(context.initContext(), ctx -> {
                grouped.init(ctx);
                return null;
            });

            withArgs(context.beginContext(), ctx -> {
                grouped.begin(ctx);
                return null;
            });

            withArgs(context.accumContext(), ctx -> {
                grouped.accumulate(ctx);
                return null;
            });

            grouped.finished(context.finishedContext());

            context.checkContext(grouped::check);

            return grouped.step(context);
        }

        throw new IllegalStateException();
    }

    /**
     * Prepares LazyValue args and passes it to the applier to use.
     */
    private <V> V withArgs(EvalContext context, Function<FunctionApplier.GroupContext, V> applier) {
        List<LazyValue> args = mArgs.stream().map(arg -> arg.lazyValue(context)).toList();

        Map<String, LazyValue> namedArgs;
        if (mNamedArgs.isEmpty()) {
            namedArgs = Map.of();
        } else {
            namedArgs = new LinkedHashMap<>(mNamedArgs.size() << 1);
            mNamedArgs.forEach((name, expr) -> namedArgs.put(name, expr.lazyValue(context)));
        }

        // Must roll back to a savepoint for lazy/eager evaluation to work properly. Arguments
        // which aren't eagerly evaluated will roll back, forcing the underlying expression to
        // be evaluated again later if used again.
        int savepoint = context.refSavepoint();

        V result = applier.apply(new FunctionApplier.GroupContext() {
            @Override
            public List<LazyValue> args() {
                return args;
            }

            @Override
            public Map<String, LazyValue> namedArgs() {
                return namedArgs;
            }

            @Override
            public MethodMaker methodMaker() {
                return context.methodMaker();
            }

            @Override
            public FieldMaker newWorkField(Class<?> type) {
                return context.newWorkField(type);
            }

            @Override
            public Variable rowNum() {
                return context.rowNum();
            }

            @Override
            public Variable groupNum() {
                return context.groupNum();
            }

            @Override
            public Variable groupRowNum() {
                return context.groupRowNum();
            }
        });

        context.refRollback(savepoint);

        return result;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeString(mName);
            enc.encodeExprs(mArgs);

            enc.encodeByte(mNamedArgs.size());
            for (Map.Entry<String, Expr> e : mNamedArgs.entrySet()) {
                enc.encodeString(e.getKey());
                e.getValue().encodeKey(enc);
            }

            enc.encodeReference(mOriginalApplier);
        }
    }

    @Override
    public int hashCode() {
        int hash = mName.hashCode();
        hash = hash * 31 + mArgs.hashCode();
        hash = hash * 31 + mNamedArgs.hashCode();
        hash = hash * 31 + mOriginalApplier.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof CallExpr ce
            && mName.equals(ce.mName)
            && mArgs.equals(ce.mArgs)
            && mNamedArgs.equals(ce.mNamedArgs)
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

        for (Map.Entry<String, Expr> e : mNamedArgs.entrySet()) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(e.getKey()).append(':');
            e.getValue().appendTo(b);
            i++;
        }

        b.append(')');
    }
}
