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

import java.util.Map;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.ColumnSet;
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

            if (leftType == AnyType.THE) {
                if (rightType == AnyType.THE) {
                    type = leftType;
                    break;
                }
                left = left.asType(rightType);
                continue;
            } else if (rightType == AnyType.THE) {
                right = right.asType(leftType);
                continue;
            }

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
                var b = new StringBuilder("No common type for: ");
                append(b, op, left, right);
                throw new IllegalStateException(b.toString());
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

            type = BasicType.make(common);
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

        if (op > OP_OR) {
            return new BinaryOpNode(type, name, op, left, right);
        }

        // FIXME: If left and right are ConstantNode, return a true/false ConstantNode.

        if (op >= OP_AND) {
            if (left.canThrowRuntimeException() && !right.canThrowRuntimeException()) {
                // Swap the evaluation order such that an exception is less likely to be thrown
                // due to short-circuit logic. The implementation of hasOrderDependentException
                // in the Filtered class assumes that this swap operation has been performed.
                Node temp = left;
                left = right;
                right = temp;
            }
        }

        return new Filtered(name, op, left, right);
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

    private static void append(StringBuilder b, int op, Node left, Node right) {
        append(b, left);
        b.append(' ').append(opString(op)).append(' ');
        append(b, right);
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
    public void evalColumns(Set<String> columns) {
        mLeft.evalColumns(columns);
        mRight.evalColumns(columns);
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
    public boolean canThrowRuntimeException() {
        Class clazz = type().clazz();
        if (clazz.isPrimitive()) {
            return clazz != float.class && clazz != double.class;
        } else if (clazz == BigDecimal.class || clazz == BigInteger.class) {
            return mOp == OP_DIV || mOp == OP_REM;
        }
        return true;
    }

    @Override
    public boolean hasOrderDependentException() {
        return mLeft.hasOrderDependentException() || mRight.hasOrderDependentException();
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
        public RowFilter toRowFilter(RowInfo info, Map<String, ColumnNode> columns) {
            if (mOp == OP_AND) {
                return mLeft.toRowFilter(info, columns).and(mRight.toRowFilter(info, columns));
            } else if (mOp == OP_OR) {
                return mLeft.toRowFilter(info, columns).or(mRight.toRowFilter(info, columns));
            }

            if (mLeft instanceof ColumnNode left) {
                ColumnInfo leftCol = tryFindColumn(info, left);
                if (leftCol != null) {
                    if (mRight instanceof ColumnNode right) {
                        ColumnInfo rightCol = tryFindColumn(info, right);
                        if (rightCol != null) {
                            var filter = ColumnToColumnFilter.tryMake(leftCol, mOp, rightCol);
                            if (filter != null) {
                                columns.putIfAbsent(leftCol.name, left);
                                columns.putIfAbsent(rightCol.name, right);
                                return filter;
                            }
                        }
                    } else if (mRight instanceof ParamNode right) {
                        columns.putIfAbsent(leftCol.name, left);
                        return new ColumnToArgFilter(leftCol, mOp, right.ordinal());
                    } else if (mRight instanceof ConstantNode right) {
                        columns.putIfAbsent(leftCol.name, left);
                        return new ColumnToConstantFilter(leftCol, mOp, right.value());
                    }
                }
            } else if (mRight instanceof ColumnNode right) {
                ColumnInfo rightCol = tryFindColumn(info, right);
                if (rightCol != null) {
                    if (mLeft instanceof ParamNode left) {
                        int op = ColumnFilter.reverseOperator(mOp);
                        columns.putIfAbsent(rightCol.name, right);
                        return new ColumnToArgFilter(rightCol, op, left.ordinal());
                    } else if (mLeft instanceof ConstantNode left) {
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
}
