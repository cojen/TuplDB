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

import org.cojen.tupl.QueryException;
import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.RowFilter;

import static org.cojen.tupl.table.expr.Token.*;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class FilterExpr extends BinaryOpExpr {
    private final Expr mOriginalLeft, mOriginalRight;

    /**
     * @param op must be less than or equal to T_LOR
     * @see BinaryOpExpr#make
     */
    FilterExpr(int startPos, int endPos,
               int op, Expr originalLeft, Expr originalRight, Expr left, Expr right)
    {
        super(startPos, endPos, BasicType.BOOLEAN, op, left, right);
        mOriginalLeft = originalLeft;
        mOriginalRight = originalRight;
    }

    @Override
    public FilterExpr asType(Type type) {
        if (type.isBoolean()) {
            return this;
        }
        throw new QueryException("Cannot convert " + mType + " to " + type, startPos(), endPos());
    }

    @Override
    public Expr asAggregate(Set<String> group) {
        Expr left = mOriginalLeft.asAggregate(group);
        Expr right = mOriginalRight.asAggregate(group);
        if (left == mOriginalLeft && right == mOriginalRight) {
            return this;
        }
        return BinaryOpExpr.make(startPos(), endPos(), mOp, left, right);
    }

    @Override
    public Expr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        Expr left = mOriginalLeft.asWindow(newAssignments);
        Expr right = mOriginalRight.asWindow(newAssignments);
        if (left == mOriginalLeft && right == mOriginalRight) {
            return this;
        }
        return BinaryOpExpr.make(startPos(), endPos(), mOp, left, right);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }
        Expr left = mOriginalLeft.replace(replacements);
        Expr right = mOriginalRight.replace(replacements);
        if (left == mOriginalLeft && right == mOriginalRight) {
            return this;
        }
        return BinaryOpExpr.make(startPos(), endPos(), mOp, left, right);
    }

    @Override
    public Expr not(int startPos) {
        int op = mOp;
        if (op < T_LAND) {
            return make(startPos, endPos(), op ^ 1, mOriginalLeft, mOriginalRight);
        } else if (op <= T_LOR) {
            // Apply De Morgan's law.
            return make(startPos, endPos(), op ^ 1, mOriginalLeft.not(), mOriginalRight.not());
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public boolean supportsLogicalNot() {
        int op = mOp;
        if (op < T_LAND) {
            return true;
        } else if (op <= T_LOR) {
            // Would apply De Morgan's law.
            return mOriginalRight.supportsLogicalNot() && mOriginalRight.supportsLogicalNot();
        } else {
            return false;
        }
    }

    @Override
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnExpr> columns) {
        if (mOp == T_LAND) {
            return mLeft.toRowFilter(info, columns).and(mRight.toRowFilter(info, columns));
        } else if (mOp == T_LOR) {
            return mLeft.toRowFilter(info, columns).or(mRight.toRowFilter(info, columns));
        }

        // Attempt to push down the original expressions, not the ones which have been
        // converted to a common type. The lower filtering layer will perform any conversions
        // itself if necessary.

        ColumnExpr leftColumn, rightColumn;

        if ((leftColumn = mOriginalLeft.sourceColumn()) != null) {
            ColumnInfo leftInfo = leftColumn.tryFindColumn(info);
            if (leftInfo != null) {
                if ((rightColumn = mOriginalRight.sourceColumn()) != null) {
                    ColumnInfo rightInfo = rightColumn.tryFindColumn(info);
                    if (rightInfo != null) {
                        var filter = ColumnToColumnFilter.tryMake(leftInfo, mOp, rightInfo);
                        if (filter != null) {
                            columns.putIfAbsent(leftInfo.name, leftColumn);
                            columns.putIfAbsent(rightInfo.name, rightColumn);
                            return filter;
                        }
                    }
                } else if (mOriginalRight instanceof ParamExpr right) {
                    columns.putIfAbsent(leftInfo.name, leftColumn);
                    return new ColumnToArgFilter(leftInfo, mOp, right.ordinal());
                } else if (mOriginalRight instanceof ConstantExpr right) {
                    columns.putIfAbsent(leftInfo.name, leftColumn);
                    return new ColumnToConstantFilter(leftInfo, mOp, right);
                }
            }
        } else if ((rightColumn = mOriginalRight.sourceColumn()) != null) {
            ColumnInfo rightInfo = rightColumn.tryFindColumn(info);
            if (rightInfo != null) {
                if (mOriginalLeft instanceof ParamExpr left) {
                    int op = ColumnFilter.reverseOperator(mOp);
                    columns.putIfAbsent(rightInfo.name, rightColumn);
                    return new ColumnToArgFilter(rightInfo, op, left.ordinal());
                } else if (mOriginalLeft instanceof ConstantExpr left) {
                    int op = ColumnFilter.reverseOperator(mOp);
                    columns.putIfAbsent(rightInfo.name, rightColumn);
                    return new ColumnToConstantFilter(rightInfo, op, left);
                }
            }
        }

        return super.toRowFilter(info, columns);
    }

    @Override
    public boolean isNullable() {
        return false;
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

        switch (mOp) {
        case T_EQ, T_NE, T_GE, T_LT, T_LE, T_GT:
            CompareUtils.compare(context.methodMaker(),
                                 mLeft.type(), mLeft.makeEval(context),
                                 mRight.type(), mRight.makeEval(context),
                                 mOp, pass, fail);
            return;

        case T_LAND: {
            Label andPass = context.methodMaker().label();
            mLeft.makeFilter(context, andPass, fail);
            andPass.here();
            int savepoint = context.refSavepoint();
            mRight.makeFilter(context, pass, fail);
            // Rollback the refs for the right expression, because it doesn't always execute.
            context.refRollback(savepoint);
            return;
        }

        case T_LOR:
            Label orFail = context.methodMaker().label();
            mLeft.makeFilter(context, pass, orFail);
            orFail.here();
            int savepoint = context.refSavepoint();
            mRight.makeFilter(context, pass, fail);
            // Rollback the refs for the right expression, because it doesn't always execute.
            context.refRollback(savepoint);
            return;
        }

        throw new AssertionError();
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mLeft.canThrowRuntimeException() || mRight.canThrowRuntimeException();
    }
}
