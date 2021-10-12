/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.math.BigDecimal;

import java.util.Arrays;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.filter.ColumnFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CompareUtils {
    /**
     * Generates code which compares a column to an argument and branches to a pass or fail
     * target. One of the variables can be widened if necessary, but not both of them. For
     * example, int cannot be compared to float. Such a comparison requires that both be
     * widened to double.
     *
     * When applicable, null is considered to be higher than non-null.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    static void compare(MethodMaker mm,
                        ColumnInfo colInfo, Variable colVar,
                        ColumnInfo argInfo, Variable argVar,
                        int op, Label pass, Label fail)
    {
        if (ColumnFilter.isIn(op)) {
            if (colInfo.isPrimitive() && !colInfo.isNullable() &&
                argInfo.isPrimitive() && !argInfo.isNullable())
            {
                // If the column is boxed, it will be unboxed once before the loop.
                comparePrimitives(mm, colInfo, colVar, argInfo, argVar, op, pass, fail);
            } else {
                compareIn(mm, colInfo, colVar, argInfo, argVar, op, pass, fail);
            }
            return;
        }

        if (colInfo.isNullable() && !colVar.classType().isPrimitive()) {
            if (argInfo.isNullable() && !argVar.classType().isPrimitive()) {
                Label argNotNull = mm.label();
                argVar.ifNe(null, argNotNull);
                Label match = selectNullColumnToNullArg(op, pass, fail);
                Label mismatch = selectColumnToNullArg(op, pass, fail);
                if (match != mismatch) {
                    colVar.ifEq(null, match);
                }
                mm.goto_(mismatch);
                argNotNull.here();
            }
            colVar.ifEq(null, selectNullColumnToArg(op, pass, fail));
        } else if (argInfo.isNullable() && !argVar.classType().isPrimitive()) {
            argVar.ifEq(null, selectColumnToNullArg(op, pass, fail));
        }

        // At this point, neither variable is null. Note that a column which is primitive can
        // still be boxed.

        if (colInfo.isPrimitive()) {
            if (!argInfo.isPrimitive()) {
                throw new IllegalArgumentException("Incomparable types");
            }
            comparePrimitives(mm, colInfo, colVar, argInfo, argVar, op, pass, fail);
            return;
        }

        if (argInfo.isPrimitive()) {
            throw new IllegalArgumentException("Incomparable types");
        }

        // At this point, both variables are non-null objects.

        if (colInfo.isArray() && argInfo.isArray()) {
            compareArrays(mm, colInfo.isUnsigned() && argInfo.isUnsigned(),
                          colVar, 0, colVar.alength(), argVar, 0, argVar.alength(),
                          op, pass, fail);
            return;
        }

        if (ColumnFilter.isExact(op)) {
            if (colVar.classType() != BigDecimal.class && argVar.classType() != BigDecimal.class) {
                var result = colVar.invoke("equals", argVar);
                if (op == ColumnFilter.OP_EQ) {
                    result.ifTrue(pass);
                } else {
                    result.ifFalse(pass);
                }
            } else {
                // BigDecimal.equals is too strict and would lead to much confusion. For
                // example, 0 isn't considered equal to 0.0. Note that Float.equals is
                // similarly fuzzy with respect to how it compares NaN values.
                var result = mm.var(BigDecimalUtils.class).invoke("matches", colVar, argVar);
                if (op == ColumnFilter.OP_EQ) {
                    result.ifEq(0, pass);
                } else {
                    result.ifNe(0, pass);
                }
            }
        } else {
            // Assume both variables are Comparable.
            var result = colVar.invoke("compareTo", argVar);
            switch (op) {
            case ColumnFilter.OP_GE: result.ifGe(0, pass); break;
            case ColumnFilter.OP_LT: result.ifLt(0, pass); break;
            case ColumnFilter.OP_LE: result.ifLe(0, pass); break;
            case ColumnFilter.OP_GT: result.ifGt(0, pass); break;
            default: throw new AssertionError();
            }
        }

        mm.goto_(fail);
    }

    /**
     * Generates code which compares a column to an argument and branches to a pass or fail
     * target. Both must be non-null. One of the variables can be widened if necessary, but not
     * both of them. For example, int cannot be compared to float. Such a comparison requires
     * that both be widened to double.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    static void comparePrimitives(MethodMaker mm,
                                  ColumnInfo colInfo, Variable colVar,
                                  ColumnInfo argInfo, Variable argVar,
                                  int op, Label pass, Label fail)
    {
        Class<?> colType = colVar.classType();
        if (!colType.isPrimitive()) {
            colVar = colVar.unbox();
            colType = colVar.classType();
        }

        if (ColumnFilter.isIn(op)) {
            compareIn(mm, colInfo, colVar, argInfo, argVar, op, pass, fail);
            return;
        }

        Class<?> argType = argVar.classType();
        if (!argType.isPrimitive()) {
            argVar = argVar.unbox();
            argType = argVar.classType();
        }

        special: if (!ColumnFilter.isExact(op)) {
            // Special handling is required for non-exact comparison of unsigned types.

            if (colInfo.isUnsignedInteger()) {
                if (argInfo.isUnsignedInteger()) {
                    colVar = toLong(colInfo, colVar).add(Long.MIN_VALUE);
                    argVar = toLong(argInfo, argVar).add(Long.MIN_VALUE);
                    break special;
                }
            } else if (!argInfo.isUnsignedInteger()) {
                // Both are signed. If floating point, must perform bits comparison for finding
                // NaN in a way which is consistent with the Float.compare method.

                if (colType == float.class) {
                    if (argType != float.class) {
                        if (argType != double.class) {
                            break special;
                        }
                        colVar = colVar.cast(double.class);
                    }
                } else if (colType == double.class) {
                    if (argType == float.class) {
                        argVar = argVar.cast(double.class);
                    } else if (argType != double.class) {
                        break special;
                    }
                } else {
                    break special;
                }

                var rowUtils = mm.var(RowUtils.class);
                colVar = rowUtils.invoke("floatToBitsCompare", colVar);
                argVar = rowUtils.invoke("floatToBitsCompare", argVar);
            }

            // Mixed signed/unsigned comparison.

            colVar = toLong(colInfo, colVar);
            argVar = toLong(argInfo, argVar);

            if (colInfo.plainTypeCode() == TYPE_ULONG) {
                signMismatch(colVar, argVar, ColumnFilter.flipOperator(op), pass, fail);
            } else if (argInfo.plainTypeCode() == TYPE_ULONG) {
                signMismatch(colVar, argVar, op, pass, fail);
            }
        } else {
            // If floating point, must perform bits comparison for finding NaN. Note that the
            // "raw" bits form isn't used, and thus all NaN forms can be found. This is
            // consistent with the Float.equals method.
            // FIXME: When searching against a floating point key, must use a range for
            // consistency. That code won't go here, however.

            if (colType == float.class) {
                if (argType == float.class) {
                    colVar = colVar.invoke("floatToIntBits", colVar);
                    argVar = argVar.invoke("floatToIntBits", argVar);
                    break special;
                }
                if (argType != double.class) {
                    break special;
                }
                colVar = colVar.cast(double.class);
            } else if (colType == double.class) {
                if (argType == float.class) {
                    argVar = argVar.cast(double.class);
                } else if (argType != double.class) {
                    break special;
                }
            } else {
                // Special handling is required for exact comparison of unsigned types.
                if (colInfo.isUnsignedInteger() || argInfo.isUnsignedInteger()) {
                    colVar = toLong(colInfo, colVar);
                    argVar = toLong(argInfo, argVar);
                    if ((colInfo.isUnsigned() ^ argInfo.isUnsigned()) &&
                        (colInfo.plainTypeCode() == TYPE_ULONG ||
                         argInfo.plainTypeCode() == TYPE_ULONG))
                    {
                        signMismatch(colVar, argVar, op, pass, fail);
                    }
                }

                break special;
            }

            colVar = colVar.invoke("doubleToLongBits", colVar);
            argVar = argVar.invoke("doubleToLongBits", argVar);
        }

        switch (op) {
        case ColumnFilter.OP_EQ: colVar.ifEq(argVar, pass); break;
        case ColumnFilter.OP_NE: colVar.ifNe(argVar, pass); break;
        case ColumnFilter.OP_GE: colVar.ifGe(argVar, pass); break;
        case ColumnFilter.OP_LT: colVar.ifLt(argVar, pass); break;
        case ColumnFilter.OP_LE: colVar.ifLe(argVar, pass); break;
        case ColumnFilter.OP_GT: colVar.ifGt(argVar, pass); break;
        default: throw new AssertionError();
        }

        mm.goto_(fail);
    }

    @FunctionalInterface
    static interface CompareArg {
        void apply(Variable argVar, Label pass, Label fail);
    }

    /**
     * Generates code for "in" and "not in" filters, which are expected to operate over an
     * argument array.
     *
     * @param argVar expected to be an array
     * @param op OP_IN or OP_NOT_IN
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     * @param compareArg called for each element
     */
    static void compareIn(MethodMaker mm, Variable argVar, int op, Label pass, Label fail,
                          CompareArg compareArg)
    {
        // FIXME: Use binary search if large enough. Must have already been sorted.

        if (op == ColumnFilter.OP_NOT_IN) {
            Label tmp = pass;
            pass = fail;
            fail = tmp;
        }

        // Basic for-loop over the array.

        var lengthVar = argVar.alength();
        var ixVar = mm.var(int.class).set(0);
        Label start = mm.label().here();
        ixVar.ifGe(lengthVar, fail);
        Label next = mm.label();
        compareArg.apply(argVar.aget(ixVar), pass, next);
        next.here();
        ixVar.inc(1);
        mm.goto_(start);
    }

    /**
     * Generates code for "in" and "not in" filters, which are expected to operate over an
     * argument array.
     *
     * @param argVar expected to be an array
     * @param op OP_IN or OP_NOT_IN
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    private static void compareIn(MethodMaker mm,
                                  ColumnInfo colInfo, Variable colVar,
                                  ColumnInfo argInfo, Variable argVar,
                                  int op, Label pass, Label fail)
    {
        compareIn(mm, argVar, op, pass, fail, (a, p, f) -> {
            compare(mm, colInfo, colVar, argInfo, a, ColumnFilter.OP_EQ, p, f);
        });
    }

    /**
     * Convert signed or unsigned value to long.
     */
    private static Variable toLong(ColumnInfo info, Variable var) {
        if (var.classType() != long.class) {
            var = var.cast(long.class);
            doMask: {
                long mask;
                switch (info.plainTypeCode()) {
                case TYPE_UBYTE:  mask = 0xffL;        break;
                case TYPE_USHORT: mask = 0xffffL;      break;
                case TYPE_UINT:   mask = 0xffff_ffffL; break;
                default: break doMask;
                }
                var = var.and(mask);
            }
        }

        return var;
    }

    /**
     * Short-circuit branches to pass or fail only if the value ranges don't overlap. One of
     * the variables must be signed and the other must be unsigned.
     */
    private static void signMismatch(Variable a, Variable b, int op, Label pass, Label fail) {
        switch (op) {
        case ColumnFilter.OP_LT: case ColumnFilter.OP_LE: case ColumnFilter.OP_NE:
            a.ifLt(0, pass);
            b.ifLt(0, pass);
            break;
        case ColumnFilter.OP_GE: case ColumnFilter.OP_GT: case ColumnFilter.OP_EQ:
            a.ifLt(0, fail);
            b.ifLt(0, fail);
            break;
        default: throw new AssertionError();
        }
    }

    /**
     * Selects a target label when comparing a null column value to a non-null filter argument.
     * Null is considered to be higher than non-null.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     * @return pass or fail
     */
    static Label selectNullColumnToArg(int op, Label pass, Label fail) {
        switch (op) {
        case ColumnFilter.OP_EQ: return fail; // null == !null? false
        case ColumnFilter.OP_NE: return pass; // null != !null? true
        case ColumnFilter.OP_GE: return pass; // null >= !null? true
        case ColumnFilter.OP_LT: return fail; // null <  !null? false
        case ColumnFilter.OP_LE: return fail; // null <= !null? false
        case ColumnFilter.OP_GT: return pass; // null >  !null? true
        default: throw new AssertionError();
        }
    }

    /**
     * Selects a target label when comparing a non-null column value to a null filter argument.
     * Null is considered to be higher than non-null.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     * @return pass or fail
     */
    static Label selectColumnToNullArg(int op, Label pass, Label fail) {
        switch (op) {
        case ColumnFilter.OP_EQ: return fail; // !null == null? false
        case ColumnFilter.OP_NE: return pass; // !null != null? true
        case ColumnFilter.OP_GE: return fail; // !null >= null? false
        case ColumnFilter.OP_LT: return pass; // !null <  null? true
        case ColumnFilter.OP_LE: return pass; // !null <= null? true
        case ColumnFilter.OP_GT: return fail; // !null >  null? false

        // Treat a null "in" array as if it was empty.
        case ColumnFilter.OP_IN: return fail;
        case ColumnFilter.OP_NOT_IN: return pass;

        default: throw new AssertionError();
        }
    }

    /**
     * Selects a target label when comparing a null column value to a null filter argument.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     * @return pass or fail
     */
    static Label selectNullColumnToNullArg(int op, Label pass, Label fail) {
        switch (op) {
        case ColumnFilter.OP_EQ: return pass; // null == null? true
        case ColumnFilter.OP_NE: return fail; // null != null? false
        case ColumnFilter.OP_GE: return pass; // null >= null? true
        case ColumnFilter.OP_LT: return fail; // null <  null? false
        case ColumnFilter.OP_LE: return pass; // null <= null? true
        case ColumnFilter.OP_GT: return fail; // null >  null? false

        // Treat a null "in" array as if it was empty.
        case ColumnFilter.OP_IN: return fail;
        case ColumnFilter.OP_NOT_IN: return pass;

        default: throw new AssertionError();
        }
    }

    /**
     * Makes code which compares two arrays of the same type.
     *
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    static void compareArrays(MethodMaker mm, boolean unsigned,
                              Object a, Object aFrom, Object aTo,
                              Object b, Object bFrom, Object bTo,
                              int op, Label pass, Label fail)
    {
        var arraysVar = mm.var(Arrays.class);

        switch (op) {
        case ColumnFilter.OP_EQ: case ColumnFilter.OP_NE: {
            var resultVar = arraysVar.invoke("equals", a, aFrom, aTo, b, bFrom, bTo);
            if (op == ColumnFilter.OP_EQ) {
                resultVar.ifTrue(pass);
            } else {
                resultVar.ifFalse(pass);
            }
            break;
        }
        default:
            String method = "compare";
            if (unsigned) {
                method += "Unsigned";
            }
            var resultVar = arraysVar.invoke(method, a, aFrom, aTo, b, bFrom, bTo);
            switch (op) {
            case ColumnFilter.OP_GE: resultVar.ifGe(0, pass); break;
            case ColumnFilter.OP_LT: resultVar.ifLt(0, pass); break;
            case ColumnFilter.OP_LE: resultVar.ifLe(0, pass); break;
            case ColumnFilter.OP_GT: resultVar.ifGt(0, pass); break;
            default: throw new AssertionError();
            }
        }

        mm.goto_(fail);
    }
}
