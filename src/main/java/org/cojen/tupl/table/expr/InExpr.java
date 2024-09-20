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

import org.cojen.tupl.QueryException;
import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.InFilter;
import org.cojen.tupl.table.filter.RowFilter;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class InExpr extends Expr {
    public static InExpr make(Expr left, Expr right) {
        return make(left.startPos(), right.endPos(), left, right);
    }

    public static InExpr make(int startPos, int endPos, Expr left, Expr right) {
        return new InExpr(startPos, endPos, left, right, false);
    }

    private final Expr mLeft, mRight;
    private final boolean mNot;

    private InExpr(int startPos, int endPos, Expr left, Expr right, boolean not) {
        super(startPos, endPos);
        mLeft = left;
        // TODO: Perhaps 'in' operator should only work against a set?
        mRight = right.asType(BasicType.make(Object[].class, Type.TYPE_REFERENCE));
        mNot = not;
    }

    @Override
    public Type type() {
        return BasicType.BOOLEAN;
    }

    @Override
    public Expr asType(Type type) {
        if (type.isBoolean()) {
            return this;
        }
        throw new QueryException("Cannot convert " + BasicType.BOOLEAN + " to " + type,
                                 mLeft.startPos(), mRight.endPos());
    }

    @Override
    public InExpr not(int startPos) {
        return new InExpr(startPos, endPos(), mLeft, mRight, !mNot);
    }

    @Override
    public boolean supportsLogicalNot() {
        return true;
    }

    @Override
    public int maxArgument() {
        return Math.max(mLeft.maxArgument(), mRight.maxArgument());
    }

    @Override
    public boolean isPureFunction() {
        return mLeft.isPureFunction() && mRight.isPureFunction();
    }

    @Override
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnExpr> columns) {
        ColumnExpr ce;

        if ((ce = mLeft.sourceColumn()) != null) {
            ColumnInfo ci = ce.tryFindColumn(info);
            if (ci != null && mRight instanceof ParamExpr pe) {
                var filter = new InFilter(ci, pe.ordinal());
                if (mNot) {
                    filter = filter.not();
                }
                return filter;
            }
        }

        return super.toRowFilter(info, columns);
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isConstant() {
        return mLeft.isConstant() && mRight.isConstant();
    }

    @Override
    public boolean isOrderDependent() {
        return mLeft.isOrderDependent() || mRight.isOrderDependent();
    }

    @Override
    public boolean isGrouping() {
        return mLeft.isGrouping() || mRight.isGrouping();
    }

    @Override
    public boolean isAccumulating() {
        return mLeft.isAccumulating() || mRight.isAccumulating();
    }

    @Override
    public boolean isAggregating() {
        return mLeft.isAggregating() || mRight.isAggregating();
    }

    @Override
    public InExpr asAggregate(Set<String> group) {
        Expr left = mLeft.asAggregate(group);
        Expr right = mRight.asAggregate(group);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new InExpr(startPos(), endPos(), left, right, mNot);
    }

    @Override
    public InExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        Expr left = mLeft.asWindow(newAssignments);
        Expr right = mRight.asWindow(newAssignments);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new InExpr(startPos(), endPos(), left, right, mNot);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }
        Expr left = mLeft.replace(replacements);
        Expr right = mRight.replace(replacements);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new InExpr(startPos(), endPos(), left, right, mNot);
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
        mLeft.gatherEvalColumns(c);
        mRight.gatherEvalColumns(c);
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        return doMakeEvalForFilter(context, resultRef);
    }

    @Override
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        if (isPureFunction()) {
            Variable result = context.refFor(this).get();
            if (result != null) {
                result.ifTrue(pass);
                fail.goto_();
                return;
            }
        }

        int op = mNot ? ColumnFilter.OP_NOT_IN : ColumnFilter.OP_IN;

        CompareUtils.compare(context.methodMaker(),
                             mLeft.type(), mLeft.makeEval(context),
                             mRight.type(), mRight.makeEval(context),
                             op, pass, fail);
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mLeft.canThrowRuntimeException() || mRight.canThrowRuntimeException();
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeBoolean(mNot);
            mLeft.encodeKey(enc);
            mRight.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        int hash = mLeft.hashCode() * 31 + mRight.hashCode();
        if (mNot) {
            hash *= 31;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof InExpr ie
            && mLeft.equals(ie.mLeft) && mRight.equals(ie.mRight)
            && mNot == ie.mNot;
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        if (mNot) {
            b.append('!').append('(');
        }

        b.append(mLeft).append(' ').append("in").append(' ').append(mRight);

        if (mNot) {
            b.append(')');
        }
    }
}
