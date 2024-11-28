/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table.filter;

import java.util.Map;

import java.util.function.Function;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ColumnSet;

import static java.lang.Integer.MIN_VALUE;
import static java.lang.Integer.MAX_VALUE;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract sealed class ColumnFilter extends TermFilter
    permits ColumnToArgFilter, ColumnToColumnFilter, ColumnToConstantFilter
{
    public static final int OP_EQ = 0, OP_NE = 1, OP_GE = 2, OP_LT = 3, OP_LE = 4, OP_GT = 5;

    // Used by InFilter.
    public static final int OP_IN = 6, OP_NOT_IN = 7;

    /**
     * Returns the operator for 'not' matching.
     */
    public static int flipOperator(int op) {
        return op ^ 1;
    }

    /**
     * Returns the operator with reverse ordering.
     */
    public static int reverseOperator(int op) {
        return switch (op) {
            case OP_GE -> OP_LE;
            case OP_LT -> OP_GT;
            case OP_LE -> OP_GE;
            case OP_GT -> OP_LT;
            default -> op;
        };
    }

    public static boolean hasEqualComponent(int op) {
        return (op & 1) == 0;
    }

    public static int removeEqualComponent(int op) {
        return switch (op) {
            case OP_EQ -> OP_NE;
            case OP_GE -> OP_GT;
            case OP_LE -> OP_LT;
            default -> op;
        };
    }

    final ColumnInfo mColumn;
    final int mOperator;

    int mMatchHashCode;

    ColumnFilter(int hash, ColumnInfo column, int op) {
        super(hash);
        mColumn = column;
        mOperator = op;
    }

    @Override
    final RowFilter expandOperators(boolean force) {
        if (!force) {
            return this;
        } else {
            int op1;
            switch (mOperator) {
            default: return this;
            case OP_GE: op1 = OP_GT; break;
            case OP_LE: op1 = OP_LT; break;
            }
            return new OrFilter(withOperator(op1), withOperator(OP_EQ));
        }
    }

    @Override
    public ColumnFilter not() {
        return withOperator(flipOperator(mOperator));
    }

    @Override
    protected final RowFilter trySplit(Function<ColumnFilter, RowFilter> check) {
        return check.apply(this);
    }

    protected abstract boolean canSplit(Map<String, ? extends ColumnInfo> columns);

    protected boolean columnExists(Map<String, ? extends ColumnInfo> columns, ColumnInfo column) {
        return ColumnSet.findColumn(columns, column.name) != null;
    }

    public final ColumnInfo column() {
        return mColumn;
    }

    public final int operator() {
        return mOperator;
    }

    public final String operatorString() {
        return switch (mOperator) {
        case OP_EQ -> "==";
        case OP_NE -> "!=";
        case OP_GE -> ">=";
        case OP_LT -> "<";
        case OP_LE -> "<=";
        case OP_GT -> ">";
        case OP_IN -> "in";
        case OP_NOT_IN -> "!in";
        default -> "?";
        };
    }

    /**
     * @return true if operator is OP_EQ or OP_NE
     */
    public final boolean isExact() {
        return isExact(mOperator);
    }

    /**
     * @return true if operator is OP_EQ or OP_NE
     */
    public static boolean isExact(int op) {
        return op == OP_EQ || op == OP_NE;
    }

    /**
     * @return true if operator is OP_IN or OP_NOT_IN
     */
    public final boolean isIn() {
        return isIn(mOperator);
    }

    /**
     * @return true if operator is OP_IN or OP_NOT_IN
     */
    public static boolean isIn(int op) {
        return op >= OP_IN;
    }

    /**
     * Returns:
     *
     * - MIN_VALUE if not reducible
     * - MAX_VALUE if complementation (entire group goes away)
     * - operator if idempotence (one of the operators is redundant)
     * - ~operator if elimination (operators are merged into a different operator)
     */
    final int reduceOperatorForAnd(ColumnFilter other) {
        if (!isReducible(other)) {
            return MIN_VALUE;
        }
        return reduceOperatorForAnd(mOperator, other.mOperator);
    }

    static int reduceOperatorForAnd(int op1, int op2) {
        if (op1 < OP_IN && op2 < OP_IN) {
            return REDUCE_AND[op1 * 6 + op2];
        }
        return MIN_VALUE;
    }

    /**
     * Returns:
     *
     * - MIN_VALUE if not reducible
     * - MAX_VALUE if complementation (entire group goes away)
     * - operator if idempotence (one of the operators is redundant)
     * - ~operator if elimination (operators are merged into a different operator)
     */
    final int reduceOperatorForOr(ColumnFilter other) {
        if (!isReducible(other)) {
            return MIN_VALUE;
        }
        return reduceOperatorForOr(mOperator, other.mOperator);
    }

    static int reduceOperatorForOr(int op1, int op2) {
        if (op1 < OP_IN && op2 < OP_IN) {
            return REDUCE_OR[op1 * 6 + op2];
        }
        return MIN_VALUE;
    }

    private boolean isReducible(ColumnFilter other) {
        return mColumn.equals(other.mColumn) && equalRhs(other);
    }

    private static final int[] REDUCE_AND = {
         OP_EQ,     MAX_VALUE,  OP_EQ,     MAX_VALUE,  OP_EQ,     MAX_VALUE, 
         MAX_VALUE,  OP_NE,    ~OP_GT,     OP_LT,     ~OP_LT,     OP_GT, 
         OP_EQ,     ~OP_GT,     OP_GE,     MAX_VALUE, ~OP_EQ,     OP_GT, 
         MAX_VALUE,  OP_LT,     MAX_VALUE, OP_LT,      OP_LT,     MAX_VALUE, 
         OP_EQ,     ~OP_LT,    ~OP_EQ,     OP_LT,      OP_LE,     MAX_VALUE, 
         MAX_VALUE,  OP_GT,     OP_GT,     MAX_VALUE, MAX_VALUE,  OP_GT, 
    };

    private static final int[] REDUCE_OR = {
         OP_EQ,     MAX_VALUE, OP_GE,     ~OP_LE,     OP_LE,     ~OP_GE, 
         MAX_VALUE, OP_NE,     MAX_VALUE,  OP_NE,     MAX_VALUE,  OP_NE, 
         OP_GE,     MAX_VALUE, OP_GE,      MAX_VALUE, MAX_VALUE,  OP_GE, 
        ~OP_LE,     OP_NE,     MAX_VALUE,  OP_LT,     OP_LE,     ~OP_NE, 
         OP_LE,     MAX_VALUE, MAX_VALUE,  OP_LE,     OP_LE,      MAX_VALUE, 
        ~OP_GE,     OP_NE,     OP_GE,     ~OP_NE,     MAX_VALUE,  OP_GT,
    };

    /**
     * Equal right-hand-side.
     */
    abstract boolean equalRhs(ColumnFilter other);

    /**
     * Returns a new instance with a different operator.
     */
    abstract ColumnFilter withOperator(int op);

    @Override
    public void appendTo(StringBuilder b) {
        b.append(mColumn.name).append(' ').append(operatorString()).append(' ');
    }
}
