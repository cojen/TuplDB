/*
 *  Copyright (C) 2023 Cojen.org
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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Objects;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.CompareUtils;
import org.cojen.tupl.rows.ConvertUtils;
import org.cojen.tupl.rows.RowInfo;

import org.cojen.tupl.rows.filter.ColumnFilter;
import org.cojen.tupl.rows.filter.ColumnToArgFilter;
import org.cojen.tupl.rows.filter.ColumnToColumnFilter;
import org.cojen.tupl.rows.filter.ColumnToConstantFilter;
import org.cojen.tupl.rows.filter.RowFilter;

/**
 * Defines a node for a binary operator.
 *
 * @author Brian S. O'Neill
 */
public sealed class BinaryOpNode extends Node {
    // Pure filter operators. The numerical order and grouping of these fields must not change
    // because the make method depends on it.
    public static final int
        OP_EQ = ColumnFilter.OP_EQ, OP_NE = ColumnFilter.OP_NE,
        OP_GE = ColumnFilter.OP_GE, OP_LT = ColumnFilter.OP_LT,
        OP_LE = ColumnFilter.OP_LE, OP_GT = ColumnFilter.OP_GT,
        OP_AND = 6, OP_OR = 7;

    // Arithmetic operators.
    public static final int OP_ADD = 8, OP_SUB = 9, OP_MUL = 10, OP_DIV = 11, OP_REM = 12;

    /*
    // Bitwise arithmetic operators.
    public static final int OP_BAND = 13, OP_BOR = 14, OP_XOR = 15,
        OP_SHL = 16, OP_SHR = 17, OP_USHR = 17;
    */

    /**
     * @param name can be null to automatically assign a name
     */
    public static BinaryOpNode make(String name, int op, Node left, Node right) {
        Type type;

        while (true) {
            Type leftType = left.type();
            Type rightType = right.type();

            ColumnInfo common = ConvertUtils.commonType(leftType, rightType, op);

            if (common == leftType) {
                type = leftType;
                break;
            }

            if (common == rightType) {
                type = rightType;
                break;
            }

            if (common == null) {
                if (left instanceof ParamNode pn) {
                    left = left.asType(rightType);
                    continue;
                }
                if (right instanceof ParamNode pn) {
                    right = right.asType(leftType);
                    continue;
                }

                throw new IllegalStateException
                    ("No common type for: " + left + ' ' + opString(op) + ' ' + right);
            }

            if ((common.type == BigDecimal.class || common.type == BigInteger.class) &&
                leftType.isUnsigned() != rightType.isUnsigned())
            {
                // Try to use a simpler type.
                if (left instanceof ConstantNode cn) {
                    ConstantNode converted = cn.tryConvert(rightType);
                    if (converted != null && !converted.equals(left)) {
                        left = converted;
                        continue;
                    }
                } else if (right instanceof ConstantNode cn) {
                    ConstantNode converted = cn.tryConvert(leftType);
                    if (converted != null && !converted.equals(right)) {
                        right = converted;
                        continue;
                    }
                }
            }

            type = BasicType.make(common.type, common.typeCode);
            break;
        }

        left = left.asType(type);
        right = right.asType(type);

        if (type == BasicType.BOOLEAN && op <= OP_NE
            /*&& left.isPureFilter() && right.isPureFilter()*/)
        {
            /* FIXME: Transform some forms into xor:

               A == B  -->    A ^ B
               A != B  -->  ~(A ^ B)

               Transform using UnaryOpNode with OP_NOT, although OP_NOT could just apply De
               Morgan's law.

               a b  xor  !a  !b  (!a && b)  (a && !b)  ||
               --------------------------------------------
               0 0  0    1   1   0           0         0
               0 1  1    1   0   1           0         1
               1 0  1    0   1   0           1         1
               1 1  0    0   0   0           0         0
            */
        }

        if (op <= OP_OR) {
            // FIXME: If left and right are ConstantNode, return a true/false ConstantNode.
            return new Filtered(name, op, left, right);
        }

        return new BinaryOpNode(type, name, op, left, right);
    }

    protected final Type mType;
    protected String mName;
    protected final int mOp;
    protected final Node mLeft, mRight;

    private BinaryOpNode(Type type, String name, int op, Node left, Node right) {
        mType = type;
        mName = name;
        mOp = op;
        mLeft = left;
        mRight = right;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }

        if (mOp < OP_ADD) {
            throw new IllegalStateException("Cannot convert " + mType + " to " + type);
        }

        // Convert the sources to avoid calculation errors.
        return new BinaryOpNode(type, mName, mOp, mLeft.asType(type), mRight.asType(type));
    }

    @Override
    public String name() {
        if (mName == null) {
            var b = new StringBuilder();
            append(b, mLeft);
            b.append(' ').append(opString()).append(' ');
            append(b, mRight);
            mName = b.toString();
        }
        return mName;
    }

    private static void append(StringBuilder b, Node node) {
        String name = node.name();
        if (node instanceof BinaryOpNode) {
            b.append('(').append(name).append(')');
        } else {
            b.append(name);
        }
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

        var leftVar = mLeft.makeEval(context);
        var rightVar = mRight.makeEval(context);

        Variable result = doMakeEval(context, leftVar, rightVar);

        if (resultRef != null) {
            result = resultRef.set(result);
        }

        return result;
    }

    private Variable doMakeEval(EvalContext context, Variable leftVar, Variable rightVar) {
        switch (mOp) {
            // FIXME: These ops need to work for primitive numbers, BigInteger, and BigDecimal.
            // FIXME: Needs to perform exact arithmetic.
            // FIXME: Needs to support unsigned numbers.

        case OP_ADD:
            // FIXME: Temporary hack.
            return leftVar.add(rightVar);

        case OP_SUB:
            // FIXME: OP_SUB
            throw null;

        case OP_MUL:
            // FIXME: OP_MUL
            throw null;

        case OP_DIV:
            // FIXME: OP_DIV
            throw null;

        case OP_REM:
            // FIXME: OP_REM
            throw null;

        };

        throw new AssertionError();
    }

    @Override
    public int hashCode() {
        int hash = mType.hashCode();
        hash = hash * 31 + mOp;
        hash = hash * 31 + mLeft.hashCode();
        hash = hash * 31 + mRight.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BinaryOpNode bon
            && mType.equals(bon.mType) && mOp == bon.mOp
            && mLeft.equals(bon.mLeft) && mRight.equals(bon.mRight);
    }

    protected String opString() {
        return opString(mOp);
    }

    protected static String opString(int op) {
        return switch (op) {
            case OP_EQ  -> "==";
            case OP_NE  -> "!=";
            case OP_GE  -> ">=";
            case OP_LT  -> "<";
            case OP_LE  -> "<=";
            case OP_GT  -> ">";
            case OP_AND -> "&&";
            case OP_OR  -> "||";

            case OP_ADD -> "+";
            case OP_SUB -> "-";
            case OP_MUL -> "*";
            case OP_DIV -> "/";
            case OP_REM -> "%";

            default -> "?";
        };
    }

    /**
     * Only used by pure filter operators.
     */
    public static final class Filtered extends BinaryOpNode {
        private Filtered(String name, int op, Node left, Node right) {
            super(BasicType.BOOLEAN, name, op, left, right);
        }

        @Override
        public RowFilter toFilter(RowInfo info) {
            if (mOp == OP_AND) {
                return mLeft.toFilter(info).and(mRight.toFilter(info));
            } else if (mOp == OP_OR) {
                return mLeft.toFilter(info).or(mRight.toFilter(info));
            }

            if (mLeft instanceof ColumnNode left) {
                ColumnInfo leftCol = tryFindColumn(info, left);
                if (leftCol != null) {
                    if (mRight instanceof ColumnNode right) {
                        ColumnInfo rightCol = tryFindColumn(info, right);
                        if (rightCol != null) {
                            var filter = ColumnToColumnFilter.tryMake(leftCol, mOp, rightCol);
                            if (filter != null) {
                                return filter;
                            }
                        }
                    } else if (mRight instanceof ParamNode right) {
                        return new ColumnToArgFilter(leftCol, mOp, right.ordinal());
                    } else if (mRight instanceof ConstantNode right) {
                        return new ColumnToConstantFilter(leftCol, mOp, right.value());
                    }
                }
            } else if (mRight instanceof ColumnNode right) {
                ColumnInfo rightCol = tryFindColumn(info, right);
                if (rightCol != null) {
                    if (mLeft instanceof ParamNode left) {
                        int op = ColumnFilter.reverseOperator(mOp);
                        return new ColumnToArgFilter(rightCol, op, left.ordinal());
                    } else if (mLeft instanceof ConstantNode left) {
                        int op = ColumnFilter.reverseOperator(mOp);
                        return new ColumnToConstantFilter(rightCol, op, left.value());
                    }
                }
            }

            return super.toFilter(info);
        }

        private static ColumnInfo tryFindColumn(RowInfo info, ColumnNode node) {
            return info.allColumns.get(node.column().name());
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

            if (true) {
                makeFilter(context, pass, fail);
            }

            var result = resultRef == null ? mm.var(boolean.class) : resultRef.toSet(boolean.class);

            fail.here();
            result.set(false);
            Label cont = mm.label().goto_();
            pass.here();
            result.set(true);
            cont.here();

            return result;
        }

        // FIXME: Break up this method into sub methods to be used by FilterVisitor.
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
    }
}
