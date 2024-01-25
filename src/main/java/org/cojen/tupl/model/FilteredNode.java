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

package org.cojen.tupl.model;

import java.util.Map;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ColumnSet;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.RowFilter;

/**
 * Only used by pure filter operators.
 *
 * @author Brian S. O'Neill
 */
public final class FilteredNode extends BinaryOpNode {
    private final Node mOriginalLeft, mOriginalRight;

    /**
     * @param op must be less than or equal to OP_OR
     * @see BinaryOpNode#make
     */
    FilteredNode(String name, int op,
                 Node originalLeft, Node originalRight, Node left, Node right)
    {
        super(BasicType.BOOLEAN, name, op, left, right);
        mOriginalLeft = originalLeft;
        mOriginalRight = originalRight;
    }

    @Override
    public FilteredNode asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }
        throw new IllegalStateException("Cannot convert " + mType + " to " + type);
    }

    @Override
    public FilteredNode withName(String name) {
        return name.equals(mName) ? this
            : new FilteredNode(name, mOp, mOriginalLeft, mOriginalRight, mLeft, mRight);
    }

    @Override
    public Node not() {
        int op = mOp;
        if (op < OP_AND) {
            return make(ColumnFilter.flipOperator(op), mOriginalLeft, mOriginalRight);
        } else if (op <= OP_OR) {
            // Apply De Morgan's law.
            return make(op ^ 1, mOriginalLeft.not(), mOriginalRight.not());
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnNode> columns) {
        if (mOp == OP_AND) {
            return mLeft.toRowFilter(info, columns).and(mRight.toRowFilter(info, columns));
        } else if (mOp == OP_OR) {
            return mLeft.toRowFilter(info, columns).or(mRight.toRowFilter(info, columns));
        }

        // Attempt to push down the original nodes, not the ones which have been converted
        // to a common type. The lower filtering layer will perform any conversions itself
        // if necessary.

        if (mOriginalLeft instanceof ColumnNode left) {
            ColumnInfo leftCol = tryFindColumn(info, left);
            if (leftCol != null) {
                if (mOriginalRight instanceof ColumnNode right) {
                    ColumnInfo rightCol = tryFindColumn(info, right);
                    if (rightCol != null) {
                        var filter = ColumnToColumnFilter.tryMake(leftCol, mOp, rightCol);
                        if (filter != null) {
                            columns.putIfAbsent(leftCol.name, left);
                            columns.putIfAbsent(rightCol.name, right);
                            return filter;
                        }
                    }
                } else if (mOriginalRight instanceof ParamNode right) {
                    columns.putIfAbsent(leftCol.name, left);
                    return new ColumnToArgFilter(leftCol, mOp, right.ordinal());
                } else if (mOriginalRight instanceof ConstantNode right) {
                    columns.putIfAbsent(leftCol.name, left);
                    return new ColumnToConstantFilter(leftCol, mOp, right.value());
                }
            }
        } else if (mOriginalRight instanceof ColumnNode right) {
            ColumnInfo rightCol = tryFindColumn(info, right);
            if (rightCol != null) {
                if (mOriginalLeft instanceof ParamNode left) {
                    int op = ColumnFilter.reverseOperator(mOp);
                    columns.putIfAbsent(rightCol.name, right);
                    return new ColumnToArgFilter(rightCol, op, left.ordinal());
                } else if (mOriginalLeft instanceof ConstantNode left) {
                    int op = ColumnFilter.reverseOperator(mOp);
                    columns.putIfAbsent(rightCol.name, right);
                    return new ColumnToConstantFilter(rightCol, op, left.value());
                }
            }
        }

        return super.toRowFilter(info, columns);
    }

    private static ColumnInfo tryFindColumn(RowInfo info, ColumnNode node) {
        return ColumnSet.findColumn(info.allColumns, node.column().name());
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public Variable makeEval(EvalContext context) {
        EvalContext.ResultRef resultRef;

        if (isPureFunction()) {
            resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }
        } else {
            resultRef = null;
        }

        MethodMaker mm = context.methodMaker();

        Label pass = mm.label();
        Label fail = mm.label();

        makeFilter(context, pass, fail);

        var result = resultRef == null ? mm.var(boolean.class) : resultRef.toSet(boolean.class);

        fail.here();
        result.set(false);
        Label cont = mm.label().goto_();
        pass.here();
        result.set(true);
        cont.here();

        return result;
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
        case OP_EQ, OP_NE, OP_GE, OP_LT, OP_LE, OP_GT:
            CompareUtils.compare(context.methodMaker(),
                                 mLeft.type(), mLeft.makeEval(context),
                                 mRight.type(), mRight.makeEval(context),
                                 mOp, pass, fail);
            return;

        case OP_AND: {
            Label andPass = context.methodMaker().label();
            mLeft.makeFilter(context, andPass, fail);
            andPass.here();
            int savepoint = context.refSavepoint();
            mRight.makeFilter(context, pass, fail);
            // Rollback the refs for the right node, because it doesn't always execute.
            context.refRollback(savepoint);
            return;
        }

        case OP_OR:
            Label orFail = context.methodMaker().label();
            mLeft.makeFilter(context, pass, orFail);
            orFail.here();
            int savepoint = context.refSavepoint();
            mRight.makeFilter(context, pass, fail);
            // Rollback the refs for the right node, because it doesn't always execute.
            context.refRollback(savepoint);
            return;
        }

        throw new AssertionError();
    }

    @Override
    public Variable makeFilterEval(EvalContext context) {
        return makeEval(context);
    }

    @Override
    public Variable makeFilterEvalRemap(EvalContext context) {
        if (!hasOrderDependentException()) {
            return super.makeFilterEvalRemap(context);
        }

        var leftVar = makeFilterEvalRemap(context, mLeft);
        var rightVar = makeFilterEvalRemap(context, mRight);

        String method = switch(mOp) {
        case OP_AND -> "And";
        case OP_OR -> "Or";
        default -> throw new AssertionError();
        };

        MethodMaker mm = context.methodMaker();

        return mm.var(RemapUtils.class).invoke("check" + method, leftVar, rightVar);
    }

    /**
     * @return an assigned Object variable which references a Boolean or a RuntimeException
     */
    private static Variable makeFilterEvalRemap(EvalContext context, Node node) {
        MethodMaker mm = context.methodMaker();

        Label pass = mm.label();
        Label fail = mm.label();

        Variable result = mm.var(Object.class);

        Label tryStart = null;
        int savepoint = 0;

        if (node.canThrowRuntimeException()) {
            tryStart = mm.label().here();
            savepoint = context.refSavepoint();
        }

        node.makeFilter(context, pass, fail);

        fail.here();
        result.set(false);
        Label cont = mm.label().goto_();
        pass.here();
        result.set(true);

        if (tryStart != null) {
            // Rollback the refs for the node, because it doesn't always fully execute.
            context.refRollback(savepoint);

            mm.catch_(tryStart, RuntimeException.class, exVar -> {
                    result.set(exVar);
                });
        }

        cont.here();

        return result;
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mLeft.canThrowRuntimeException() || mRight.canThrowRuntimeException();
    }

    @Override
    public boolean hasOrderDependentException() {
        return super.hasOrderDependentException()
            || ((mOp == OP_AND || mOp == OP_OR) && mLeft.canThrowRuntimeException());
    }
}

