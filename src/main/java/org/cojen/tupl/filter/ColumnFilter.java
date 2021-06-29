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

package org.cojen.tupl.filter;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class ColumnFilter extends RowFilter {
    public static final int OP_EQ = 0, OP_NE = 1, OP_LT = 2, OP_GE = 3, OP_GT = 4, OP_LE = 5;

    // Used by InFilter.
    public static final int OP_IN = 6, OP_NOT_IN = 7;

    public static int flipOperator(int op) {
        return op ^ 1;
    }

    public static int descendingOperator(int op) {
        switch (op) {
        case OP_LT: return OP_GT;
        case OP_GE: return OP_LE;
        case OP_GT: return OP_LT;
        case OP_LE: return OP_GE;
        default: return op;
        }
    }

    final ColumnInfo mColumn;
    final int mOperator;

    ColumnFilter(int hash, ColumnInfo column, int op) {
        super(hash);
        mColumn = column;
        mOperator = op;
    }

    public ColumnInfo column() {
        return mColumn;
    }

    public int operator() {
        return mOperator;
    }

    /**
     * @return true if operator is OP_EQ or OP_NE
     */
    public boolean isExact() {
        return isExact(mOperator);
    }

    /**
     * @return true if operator is OP_EQ or OP_NE
     */
    public static boolean isExact(int op) {
        return op <= OP_NE;
    }

    /**
     * @return true if operator is OP_IN or OP_NOT_IN
     */
    public boolean isIn() {
        return isIn(mOperator);
    }

    /**
     * @return true if operator is OP_IN or OP_NOT_IN
     */
    public static boolean isIn(int op) {
        return op >= OP_IN;
    }

    @Override
    void appendTo(StringBuilder b) {
        b.append(mColumn.name()).append(' ');

        String opStr;
        switch (mOperator) {
        case OP_EQ: opStr = "=="; break;
        case OP_NE: opStr = "!="; break;
        case OP_LT: opStr = "<";  break;
        case OP_GE: opStr = ">="; break;
        case OP_GT: opStr = ">";  break;
        case OP_LE: opStr = "<="; break;
        default:    opStr = "?";  break;
        }

        b.append(opStr).append(' ');
    }
}
