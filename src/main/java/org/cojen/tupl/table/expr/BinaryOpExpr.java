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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;

import static org.cojen.tupl.table.expr.Token.*;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.QueryException;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public sealed class BinaryOpExpr extends Expr permits FilterExpr {
    /**
     * @param op token type code
     */
    public static Expr make(int startPos, int endPos, int op, Expr left, Expr right) {
        final Expr originalLeft = left;
        final Expr originalRight = right;

        final Type type = left.type().commonType(right.type(), op);

        if (type == null) {
            throw fail("No common type", op, left, right);
        }

        if (T_LAND <= op && op <= T_LOR) {
            if (type != BasicType.BOOLEAN) {
                throw fail("Boolean operation not allowed", op, left, right);
            }
        } else if (T_AND <= op && op <= T_XOR) {
            if (type.isBoolean()) {
                if (op != T_XOR && left.isPureFunction() && right.isPureFunction()) {
                    // Switch to logical operation which can short-circuit.
                    op -= 2;
                }
            } else if (!type.isInteger()) {
                throw fail("Bitwise operation not allowed", op, left, right);
            }
        }

        left = left.asType(type);
        right = right.asType(type);

        if (type.isBoolean() && (op == T_EQ || op == T_NE || op == T_XOR)
            && left.isPureFunction() && right.isPureFunction()
            && left.supportsLogicalNot() && right.supportsLogicalNot())
        {
            // Transform into expanded xor form.

            if (op == T_NE || op == T_XOR) {
                // a != b -->   a ^ b  --> (!a && b) || (a && !b)
                return make(startPos, endPos, T_LOR,
                            make(startPos, endPos, T_LAND, left.not(), right),
                            make(startPos, endPos, T_LAND, left, right.not()));
            } else if (op == T_EQ) {
                // a == b --> !(a ^ b) --> (a || !b) && (!a || b)
                return make(startPos, endPos, T_LAND,
                            make(startPos, endPos, T_LOR, left, right.not()),
                            make(startPos, endPos, T_LOR, left.not(), right));
            }
        }

        if (T_AND <= op && op <= T_REM) {
            // Arithmetic operator.

            Expr reduced = tryReduce(op, left, right);

            if (reduced != null) {
                return reduced;
            }

            return new BinaryOpExpr(startPos, endPos, type, op, left, right);
        }

        if (left.isPureFunction() && right.isPureFunction()) constant: {
            // Might be able to just return true or false.

            boolean value;

            if (left.equals(right)) {
                value = true;
            } else {
                if ((op == T_EQ || op == T_NE) &&
                    ((left instanceof ConstantExpr && right instanceof ConstantExpr) ||
                     falseNullComparison(originalLeft, originalRight)))
                {
                    value = false;
                } else {
                    break constant;
                }
            }
            
            switch (op) {
            case T_EQ, T_GE, T_LE: break;
            case T_NE, T_LT, T_GT: value = !value; break;
            default: break constant;
            }

            return ConstantExpr.make(startPos, endPos, value);
        }

        if (op >= T_LAND && left.canThrowRuntimeException() && !right.canThrowRuntimeException()) {
            // Swap the evaluation order such that an exception is less likely to be thrown due
            // to short-circuit logic.
            Expr temp = left;
            left = right;
            right = temp;
        }

        return new FilterExpr(startPos, endPos, op, originalLeft, originalRight, left, right);
    }

    /**
     * @return true if either side is comparing to null and is guaranteed to return false
     */
    private static boolean falseNullComparison(Expr left, Expr right) {
        return left.isNull() && !right.isNullable() || right.isNull() && !left.isNullable();
    }

    /**
     * Performs basic constant folding reduction. The left and right expressions must have a
     * common type.
     */
    private static Expr tryReduce(int op, Expr left, Expr right) {
        switch (op) {

        case T_AND:
            if (left.isZero()) {
                return left;
            } else if (right.isZero()) {
                return right;
            }
            break;

        case T_OR: case T_XOR: case T_PLUS:
            if (left.isZero()) {
                return right;
            } else if (right.isZero()) {
                return left;
            }
            break;

        case T_MINUS:
            if (right.isZero()) {
                return left;
            }
            break;

        case T_STAR:
            if (left.isZero() || right.isOne()) {
                return left;
            } else if (right.isZero() || left.isOne()) {
                return right;
            }
            break;

        case T_DIV:
            if (right.isOne()) {
                return left;
            }
            break;

        case T_REM:
            if (right.isOne()) {
                // Make a zero constant.
                return ConstantExpr.make(left.startPos(), right.startPos(), 0).asType(left.type());
            }
        }

        return null;
    }

    private static QueryException fail(String message, int op, Expr left, Expr right) {
        var b = new StringBuilder(message).append(" for: ");
        append(b, op, left, right);
        return new QueryException(b.toString(), left.startPos(), right.endPos());
    }

    protected final Type mType;
    protected final int mOp;
    protected final Expr mLeft, mRight;

    /**
     * @param op token type code
     */
    protected BinaryOpExpr(int startPos, int endPos, Type type, int op, Expr left, Expr right) {
        super(startPos, endPos);
        mType = type;
        mOp = op;
        mLeft = left;
        mRight = right;
    }

    @Override
    public final Type type() {
        return mType;
    }

    @Override
    public BinaryOpExpr asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }
        // Convert the sources to avoid calculation errors.
        // FIXME: Perform a conversion if the new type isn't numerical or is narrowing.
        return new BinaryOpExpr(startPos(), endPos(),
                                type, mOp, mLeft.asType(type), mRight.asType(type));
    }

    @Override
    public final int maxArgument() {
        return Math.max(mLeft.maxArgument(), mRight.maxArgument());
    }

    @Override
    public final boolean isPureFunction() {
        return mLeft.isPureFunction() && mRight.isPureFunction();
    }

    @Override
    public boolean isNullable() {
        return mLeft.isNullable() || mRight.isNullable();
    }

    @Override
    public final boolean isConstant() {
        return mLeft.isConstant() && mRight.isConstant();
    }

    @Override
    public final boolean isOrderDependent() {
        return mLeft.isOrderDependent() || mRight.isOrderDependent();
    }

    @Override
    public final boolean isGrouping() {
        return mLeft.isGrouping() || mRight.isGrouping();
    }

    @Override
    public final boolean isAccumulating() {
        return mLeft.isAccumulating() || mRight.isAccumulating();
    }

    @Override
    public final boolean isAggregating() {
        return mLeft.isAggregating() || mRight.isAggregating();
    }

    @Override
    public Expr asAggregate(Set<String> group) {
        Expr left = mLeft.asAggregate(group);
        Expr right = mRight.asAggregate(group);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new BinaryOpExpr(startPos(), endPos(), mType, mOp, left, right);
    }

    @Override
    public Expr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        Expr left = mLeft.asWindow(newAssignments);
        Expr right = mRight.asWindow(newAssignments);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new BinaryOpExpr(startPos(), endPos(), mType, mOp, left, right);
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
        return new BinaryOpExpr(startPos(), endPos(), mType, mOp, left, right);
    }

    @Override
    public final void gatherEvalColumns(Consumer<Column> c) {
        mLeft.gatherEvalColumns(c);
        mRight.gatherEvalColumns(c);
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
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

        return resultVar;
    }

    /**
     * @param leftVar not null, same type as mType
     * @param rightVar not null, same type as mType
     */
    private Variable doMakeEval(Variable leftVar, Variable rightVar) {
        Variable resulVar = Arithmetic.eval(mType, mOp, leftVar, rightVar);

        if (resulVar != null) {
            return resulVar;
        }

        throw queryException("Unsupported operation for type " + type());
    }

    @Override
    public boolean canThrowRuntimeException() {
        Class clazz = type().clazz();
        if (clazz.isPrimitive()) {
            return clazz != float.class && clazz != double.class;
        } else if (clazz == BigDecimal.class || clazz == BigInteger.class) {
            return mOp == T_DIV || mOp == T_REM;
        }
        return true;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected final void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            assert mOp < 256;
            enc.encodeByte(mOp);
            mLeft.encodeKey(enc);
            mRight.encodeKey(enc);
        }
    }

    @Override
    public final int hashCode() {
        int hash = mType.hashCode();
        hash = hash * 31 + mOp;
        hash = hash * 31 + mLeft.hashCode();
        hash = hash * 31 + mRight.hashCode();
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this ||
            obj instanceof BinaryOpExpr bo
            && mType.equals(bo.mType) && mOp == bo.mOp
            && mLeft.equals(bo.mLeft) && mRight.equals(bo.mRight);
    }

    @Override
    public final String toString() {
        return defaultToString();
    }

    @Override
    public final void appendTo(StringBuilder b) {
        append(b, mLeft);
        b.append(' ').append(opString()).append(' ');
        append(b, mRight);
    }

    private static void append(StringBuilder b, int op, Expr left, Expr right) {
        append(b, left);
        b.append(' ').append(opString(op)).append(' ');
        append(b, right);
    }

    private static void append(StringBuilder b, Expr expr) {
        if (expr instanceof BinaryOpExpr) {
            b.append('(');
            expr.appendTo(b);
            b.append(')');
        } else {
            expr.appendTo(b);
        }
    }

    protected final String opString() {
        return opString(mOp);
    }

    protected static String opString(int op) {
        return switch (op) {
            case T_EQ  -> "==";
            case T_NE  -> "!=";
            case T_GE  -> ">=";
            case T_LT  -> "<";
            case T_LE  -> "<=";
            case T_GT  -> ">";

            case T_LAND -> "&&";
            case T_LOR  -> "||";

            case T_AND -> "&";
            case T_OR  -> "|";
            case T_XOR  -> "^";

            case T_PLUS  -> "+";
            case T_MINUS -> "-";
            case T_STAR  -> "*";
            case T_DIV   -> "/";
            case T_REM   -> "%";

            default -> "?";
        };
    }
}
