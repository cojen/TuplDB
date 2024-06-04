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
import java.util.List;

import java.util.function.Consumer;

import org.cojen.maker.Variable;

/**
 * Defines an expression which calls a function.
 *
 * @author Brian S. O'Neill
 */
public final class CallExpr extends Expr {
    public static CallExpr make(int startPos, int endPos,
                                String name, List<Expr> args, FunctionApplier applier)
    {
        return new CallExpr(startPos, endPos, name, args, applier);
    }

    private final String mName;
    private final List<Expr> mArgs;
    private final FunctionApplier mApplier;

    private final Type[] mArgTypes;
    private final Type mReturnType;

    private CallExpr(int startPos, int endPos,
                     String name, List<Expr> args, FunctionApplier applier)
    {
        super(startPos, endPos);

        mName = name;
        mApplier = applier;

        Type[] argTypes;
        String[] argNames;

        {
            int num = args.size();

            argTypes = new Type[num];
            argNames = new String[num];

            int i = 0;
            for (Expr arg : args) {
                argTypes[i] = arg.type();
                // FIXME: support named arguments
                argNames[i] = null;
                i++;
            }

            assert i == num;
        }

        mArgTypes = argTypes;
        var reasons = new ArrayList<String>(1);
        mReturnType = applier.validate(argTypes, argNames, reasons::add);

        if (mReturnType != null) {
            // Apply any necessary type conversions to the arguments.

            boolean copied = false;
            int i = 0;
            for (Expr arg : args) {
                Expr altArg = arg.asType(argTypes[i]);
                if (altArg != arg) {
                    if (!copied) {
                        args = new ArrayList<>(args);
                        copied = true;
                    }
                    args.set(i, altArg);
                }
                i++;
            }

            assert i == argTypes.length;
        }

        mArgs = args;

        if (mReturnType == null) {
            String message = "Cannot call " + name + " function";

            if (!reasons.isEmpty()) {
                var b = new StringBuilder(message).append(": ");
                for (int i=0; i<reasons.size(); i++) {
                    if (i > 0) {
                        b.append(", ");
                    }
                    b.append(reasons.get(i));
                }
                message = b.toString();
            }

            throw new QueryException(message, this);
        }
    }

    @Override
    public Type type() {
        return mReturnType;
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
        return mReturnType.isNullable();
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
    public void gatherEvalColumns(Consumer<Column> c) {
        for (Expr arg : mArgs) {
            arg.gatherEvalColumns(c);
        }
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        FunctionApplier applier = mApplier.prepare();

        // FIXME: requireRowNum, requireGroupNum, requireGroupRowNum; must be in the context

        if (applier instanceof FunctionApplier.Plain plain) {
            LazyArg[] args = lazyArgs(context);

            // Must rollback to a savepoint for lazy/eager evaluation to work properly.
            // Arguments which aren't eagerly evaluated will rollback, forcing the underlying
            // expression to be evaluated again later if used again.
            int savepoint = context.refSavepoint();

            var retVar = context.methodMaker().var(mReturnType.clazz());
            plain.apply(mReturnType, retVar, mArgTypes, args);

            context.refRollback(savepoint);

            return retVar;
        }

        // FIXME: support other function types
        throw new UnsupportedOperationException();
    }

    private LazyArg[] lazyArgs(EvalContext context) {
        return mArgs.stream().map(arg -> arg.lazyArg(context)).toArray(LazyArg[]::new);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeString(mName);
            enc.encodeExprs(mArgs);
            enc.encodeReference(mApplier);
        }
    }

    @Override
    public int hashCode() {
        int hash = mName.hashCode();
        hash = hash * 31 + mArgs.hashCode();
        hash = hash * 31 + mApplier.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof CallExpr ce
            && mName.equals(ce.mName)
            && mArgs.equals(ce.mArgs)
            && mApplier.equals(ce.mApplier);
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
