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

import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.filter.ColumnFilter;

import static org.cojen.tupl.model.Type.*;

/**
 * Defines a node for a binary operator.
 *
 * @author Brian S. O'Neill
 */
public sealed class BinaryOpNode extends Node permits FilteredNode {
    // Pure filter operators. The numerical order and grouping of these fields must not change
    // because the make method depends on it.
    public static final int
        OP_EQ = ColumnFilter.OP_EQ, OP_NE = ColumnFilter.OP_NE,
        OP_GE = ColumnFilter.OP_GE, OP_LT = ColumnFilter.OP_LT,
        OP_LE = ColumnFilter.OP_LE, OP_GT = ColumnFilter.OP_GT,
        OP_AND = 6, OP_OR = 7, OP_XOR = 8;

    // Arithmetic operators.
    public static final int OP_ADD = 9, OP_SUB = 10, OP_MUL = 11, OP_DIV = 12, OP_REM = 13;

    /*
    // Bitwise arithmetic operators.
    public static final int OP_BAND = 14, OP_BOR = 15, OP_XOR = 16,
        OP_SHL = 17, OP_SHR = 18, OP_USHR = 19;
    */

    /**
     * @param name can be null to automatically assign a name
     */
    public static Node make(String name, int op, Node left, Node right) {
        final Node originalLeft = left;
        final Node originalRight = right;

        final Type type = Type.commonType(left, right, op);

        if (type == null) {
            throw fail("No common type", op, left, right);
        }

        if (OP_AND <= op && op <= OP_XOR) {
            if (type != BasicType.BOOLEAN) {
                throw fail("Boolean operation not allowed", op, left, right);
            }
            if (op == OP_XOR) {
                op = OP_NE;
            }
        }

        left = left.asType(type);
        right = right.asType(type);

        if (type == BasicType.BOOLEAN && ColumnFilter.isExact(op)
            && left.isPureFunction() && right.isPureFunction())
        {
            // Transform some forms into xor.
            if (op == OP_NE) {
                // a != b -->   a ^ b  --> (!a && b) || (a && !b)
                return make(null, OP_OR,
                            make(null, OP_AND, left.not(), right),
                            make(null, OP_AND, left, right.not()));
            } else if (op == OP_EQ) {
                // a == b --> !(a ^ b) --> (a || !b) && (!a || b)
                return make(null, OP_AND,
                            make(null, OP_OR, left, right.not()),
                            make(null, OP_OR, left.not(), right));
            }
        }

        if (op >= OP_ADD) {
            // Arithmetic operator.
            return new BinaryOpNode(type, name, op, left, right);
        }

        if (left.isPureFunction() && right.isPureFunction()) constant: {
            // Might be able to just return true or false.

            boolean value;

            if (left.equals(right)) {
                value = true;
            } else {
                if (!(left instanceof ConstantNode && right instanceof ConstantNode)) {
                    break constant;
                }
                value = false;
            }
            
            switch (op) {
            case OP_EQ, OP_GE, OP_LE: break;
            case OP_NE, OP_LT, OP_GT: value = !value; break;
            default: break constant;
            }

            return ConstantNode.make(value);
        }

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

        return new FilteredNode(name, op, originalLeft, originalRight, left, right);
    }

    private static IllegalStateException fail(String message, int op, Node left, Node right) {
        var b = new StringBuilder(message + " for: ");
        append(b, op, left, right);
        throw new IllegalStateException(b.toString());
    }

    protected final Type mType;
    protected String mName;
    protected final int mOp;
    protected final Node mLeft, mRight;

    protected BinaryOpNode(Type type, String name, int op, Node left, Node right) {
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
    public BinaryOpNode asType(Type type) {
        if (mType.equals(type)) {
            return this;
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

    @Override
    public BinaryOpNode withName(String name) {
        return name.equals(mName) ? this : new BinaryOpNode(mType, name, mOp, mLeft, mRight);
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
    public boolean isNullable() {
        return mLeft.isNullable() || mRight.isNullable();
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

        MethodMaker mm = context.methodMaker();
        var resultVar = mm.var(mType.clazz());

        Label ready = null;
        if (mLeft.isNullable()) {
            ready = mm.label();
            Label cont = mm.label();
            leftVar.ifNe(null, cont);
            resultVar.set(null);
            ready.goto_();
            cont.here();
        }

        if (mRight.isNullable()) {
            if (ready == null) {
                ready = mm.label();
            }
            Label cont = mm.label();
            rightVar.ifNe(null, cont);
            resultVar.set(null);
            ready.goto_();
            cont.here();
        }

        resultVar.set(doMakeEval(leftVar, rightVar));

        if (ready != null) {
            ready.here();
        }

        if (resultRef != null) {
            resultVar = resultRef.set(resultVar);
        }

        return resultVar;
    }

    /**
     * @param leftVar not null, same type as mType
     * @param rightVar not null, same type as mType
     */
    private Variable doMakeEval(Variable leftVar, Variable rightVar) {
        int op = mOp;

        Variable resulVar = switch (mType.plainTypeCode()) {
            case TYPE_UBYTE -> Arithmetic.UByte.eval(op, leftVar, rightVar);
            case TYPE_USHORT -> Arithmetic.UShort.eval(op, leftVar, rightVar);
            case TYPE_UINT -> Arithmetic.UInteger.eval(op, leftVar, rightVar);
            case TYPE_ULONG -> Arithmetic.ULong.eval(op, leftVar, rightVar);
            case TYPE_BYTE -> Arithmetic.Byte.eval(op, leftVar, rightVar);
            case TYPE_SHORT -> Arithmetic.Short.eval(op, leftVar, rightVar);
            case TYPE_INT, TYPE_LONG -> Arithmetic.Integer.eval(op, leftVar, rightVar);
            case TYPE_FLOAT, TYPE_DOUBLE -> Arithmetic.Float.eval(op, leftVar, rightVar);
            case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> Arithmetic.Big.eval(op, leftVar, rightVar);
            default -> null;
        };

        if (resulVar != null) {
            return resulVar;
        }

        // FIXME: More detail.
        throw new IllegalStateException("Unsupported operation for type");
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

            // Prefer symbols which don't clash with SQL.
            case OP_AND -> "and";
            case OP_OR  -> "or";

            case OP_ADD -> "+";
            case OP_SUB -> "-";
            case OP_MUL -> "*";
            case OP_DIV -> "/";
            case OP_REM -> "%";

            default -> "?";
        };
    }
}
