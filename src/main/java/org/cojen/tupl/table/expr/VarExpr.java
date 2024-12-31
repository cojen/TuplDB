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

import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.maker.Variable;

import org.cojen.tupl.table.RowMethodsMaker;

/**
 * Defines an expression which reads a named local variable.
 *
 * @author Brian S. O'Neill
 */
public final class VarExpr extends Expr implements Attr {
    /**
     * @param assign the expression that created the local variable
     */
    public static VarExpr make(int startPos, int endPos, AssignExpr assign) {
        return new VarExpr(startPos, endPos, assign);
    }

    private final AssignExpr mAssign;

    private VarExpr(int startPos, int endPos, AssignExpr assign) {
        super(startPos, endPos);
        mAssign = assign;
    }

    @Override
    public String name() {
        return mAssign.name();
    }

    @Override
    public Type type() {
        return mAssign.type();
    }

    @Override
    public Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isTrivial() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return type().isNullable();
    }

    @Override
    public boolean isConstant() {
        return mAssign.isConstant();
    }

    @Override
    public boolean isOrderDependent() {
        return mAssign.isOrderDependent();
    }

    @Override
    public boolean isNull() {
        return mAssign.isNull();
    }

    @Override
    public boolean isZero() {
        return mAssign.isZero();
    }

    @Override
    public boolean isOne() {
        return mAssign.isOne();
    }

    @Override
    public ColumnExpr sourceColumn() {
        return mAssign.sourceColumn();
    }

    @Override
    public boolean isGrouping() {
        return mAssign.isGrouping();
    }

    @Override
    public boolean isAccumulating() {
        return mAssign.isAccumulating();
    }

    @Override
    public boolean isAggregating() {
        return mAssign.isAggregating();
    }

    @Override
    public VarExpr asAggregate(Set<String> group) {
        if (!mAssign.isAggregating()) {
            // Cannot change how mAssign behaves, so require that asAggregate has already been
            // called against it.
            throw queryException("Column isn't part of the aggregation group or " +
                                     "wrapped by an aggregation function");
        }
        return this;
    }

    @Override
    public VarExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        AssignExpr assign = mAssign.asWindow(newAssignments);
        if (assign == mAssign) {
            return this;
        }
        return new VarExpr(startPos(), endPos(), assign);
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        Variable localVar = context.findLocalVar(name());
        if (localVar == null) {
            // Usually the assignment should have already been evaluated, but do it now.
            localVar = mAssign.makeEval(context);
            assert context.findLocalVar(name()) != null;
        }
        return localVar.get();
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mAssign.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return mAssign.hashCode() * 1869623477;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof VarExpr ve && mAssign.equals(ve.mAssign);
    }

    @Override
    public String toString() {
        return RowMethodsMaker.unescape(name());
    }
}
