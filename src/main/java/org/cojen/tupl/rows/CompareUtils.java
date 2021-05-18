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
        if (columnInfo.isNullable()) {
            if (argInfo.isNullable()) {
                Label argNotNull = mm.label();
                argVar.ifNe(null, argNotNull);
                columnVar.ifEq(null, CompareUtils.selectNullColumnToNullArg(op, pass, fail));
                mm.goto_(CompareUtils.selectColumnToNullArg(op, pass, fail));
                argNotNull.here();
            }
            columnVar.ifEq(null, selectNullColumnToArg(op, pass, fail));
        } else if (argInfo.isNullable()) {
            argVar.ifEq(null, selectColumnToNullArg(op, pass, fail));
        }

        // At this point, neither variable is null.

        if (columnInfo.isPrimitive()) {
            if (!argInfo.isPrimitive()) {
                throw new IllegalArgumentException("Incomparable types");
            }

            if (columnInfo.isUnsigned() || argInfo.isUnsigned()) {
                // FIXME: If possible, widen to int or long. If ulong, need special method. If
                // an "exact" comparison and both are unsigned, nothing special is needed.
                throw null;
            }

            switch (op) {
            case ColumnFilter.OP_EQ: columnVar.ifEq(argVar, pass); break;
            case ColumnFilter.OP_NE: columnVar.ifNe(argVar, pass); break;
            case ColumnFilter.OP_LT: columnVar.ifLt(argVar, pass); break;
            case ColumnFilter.OP_GE: columnVar.ifGe(argVar, pass); break;
            case ColumnFilter.OP_GT: columnVar.ifGt(argVar, pass); break;
            case ColumnFilter.OP_LE: columnVar.ifLe(argVar, pass); break;
            default: throw new AssertionError();
            }

            mm.goto_(fail);
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
            var result = columnVar.invoke("equals", argVar);
            if (op == ColumnFilter.OP_EQ) {
                result.ifTrue(pass);
            } else {
                result.ifFalse(pass);
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
}
