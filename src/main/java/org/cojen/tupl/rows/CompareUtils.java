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
                        ColumnInfo columnInfo, Variable columnVar,
                        ColumnInfo argInfo, Variable argVar,
                        int op, Label pass, Label fail)
    {
        if (columnInfo.isNullable() && !columnVar.classType().isPrimitive()) {
            if (argInfo.isNullable() && !argVar.classType().isPrimitive()) {
                Label argNotNull = mm.label();
                argVar.ifNe(null, argNotNull);
                columnVar.ifEq(null, CompareUtils.selectNullColumnToNullArg(op, pass, fail));
                mm.goto_(CompareUtils.selectColumnToNullArg(op, pass, fail));
                argNotNull.here();
            }
            columnVar.ifEq(null, selectNullColumnToArg(op, pass, fail));
        } else if (argInfo.isNullable() && !argVar.classType().isPrimitive()) {
            argVar.ifEq(null, selectColumnToNullArg(op, pass, fail));
        }

        // At this point, neither variable is null.

        if (columnInfo.isPrimitive()) {
            if (!argInfo.isPrimitive()) {
                throw new IllegalArgumentException("Incomparable types");
            }
            comparePrimitives(mm, columnInfo, columnVar, argInfo, argVar, op, pass, fail);
            return;
        }

        if (argInfo.isPrimitive()) {
            throw new IllegalArgumentException("Incomparable types");
        }

        // At this point, both variables are non-null objects.

        if (columnInfo.isArray() || argInfo.isArray()) {
            // FIXME
            throw null;
        }

        if (ColumnFilter.isExact(op)) {
            if (columnVar.classType() != BigDecimal.class
                && argVar.classType() != BigDecimal.class)
            {
                var result = columnVar.invoke("equals", argVar);
                if (op == ColumnFilter.OP_EQ) {
                    result.ifTrue(pass);
                } else {
                    result.ifFalse(pass);
                }
            } else {
                // BigDecimal.equals is too strict and would lead to much confusion. For
                // example, 0 isn't considered equal to 0.0.
                var result = columnVar.invoke("compareTo", argVar);
                if (op == ColumnFilter.OP_EQ) {
                    result.ifEq(0, pass);
                } else {
                    result.ifNe(0, pass);
                }
            }
        } else {
            // Assume both variables are Comparable.
            var result = columnVar.invoke("compareTo", argVar);
            switch (op) {
            case ColumnFilter.OP_LT: result.ifLt(0, pass); break;
            case ColumnFilter.OP_GE: result.ifGe(0, pass); break;
            case ColumnFilter.OP_GT: result.ifGt(0, pass); break;
            case ColumnFilter.OP_LE: result.ifLe(0, pass); break;
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
        if (colInfo.isUnsignedInteger() || argInfo.isUnsignedInteger()) {
            // FIXME: If possible, widen to int or long. If ulong, need special method. If
            // an "exact" comparison and both are unsigned, nothing special is needed.
            throw null;
        }

        Class<?> colType = colVar.classType();
        if (!colType.isPrimitive()) {
            colVar = colVar.unbox();
            colType = colVar.classType();
        }

        Class<?> argType = argVar.classType();
        if (!argType.isPrimitive()) {
            argVar = argVar.unbox();
            argType = argVar.classType();
        }

        raw: if (ColumnFilter.isExact(op)) {
            // If floating point, must perform raw comparison for finding NaN.

            if (colType == float.class) {
                if (argType == float.class) {
                    colVar = colVar.invoke("floatToRawIntBits", colVar);
                    argVar = argVar.invoke("floatToRawIntBits", argVar);
                    break raw;
                }
                if (argType != double.class) {
                    break raw;
                }
                colVar = colVar.cast(double.class);
            } else if (colType == double.class) {
                if (argType == float.class) {
                    argVar = argVar.cast(double.class);
                } else if (argType != double.class) {
                    break raw;
                }
            } else {
                break raw;
            }
            
            colVar = colVar.invoke("doubleToRawLongBits", colVar);
            argVar = argVar.invoke("doubleToRawLongBits", argVar);
        }

        switch (op) {
        case ColumnFilter.OP_EQ: colVar.ifEq(argVar, pass); break;
        case ColumnFilter.OP_NE: colVar.ifNe(argVar, pass); break;
        case ColumnFilter.OP_LT: colVar.ifLt(argVar, pass); break;
        case ColumnFilter.OP_GE: colVar.ifGe(argVar, pass); break;
        case ColumnFilter.OP_GT: colVar.ifGt(argVar, pass); break;
        case ColumnFilter.OP_LE: colVar.ifLe(argVar, pass); break;
        default: throw new AssertionError();
        }

        mm.goto_(fail);
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
        case ColumnFilter.OP_LT: return fail; // null <  !null? false
        case ColumnFilter.OP_GE: return pass; // null >= !null? true
        case ColumnFilter.OP_GT: return pass; // null >  !null? true
        case ColumnFilter.OP_LE: return fail; // null <= !null? false
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
        case ColumnFilter.OP_LT: return pass; // !null <  null? true
        case ColumnFilter.OP_GE: return fail; // !null >= null? false
        case ColumnFilter.OP_GT: return fail; // !null >  null? false
        case ColumnFilter.OP_LE: return pass; // !null <= null? true
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
        case ColumnFilter.OP_LT: return fail; // null <  null? false
        case ColumnFilter.OP_GE: return pass; // null >= null? true
        case ColumnFilter.OP_GT: return fail; // null >  null? false
        case ColumnFilter.OP_LE: return pass; // null <= null? true
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
    static void compareArrays(MethodMaker mm,
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
            var resultVar = arraysVar.invoke("compare", a, aFrom, aTo, b, bFrom, bTo);
            switch (op) {
            case ColumnFilter.OP_LT: resultVar.ifLt(0, pass); break;
            case ColumnFilter.OP_GE: resultVar.ifGe(0, pass); break;
            case ColumnFilter.OP_GT: resultVar.ifGt(0, pass); break;
            case ColumnFilter.OP_LE: resultVar.ifLe(0, pass); break;
            default: throw new AssertionError();
            }
        }

        mm.goto_(fail);
    }
}
