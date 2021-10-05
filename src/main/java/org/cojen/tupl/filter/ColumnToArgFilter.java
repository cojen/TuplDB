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
public class ColumnToArgFilter extends ColumnFilter {
    final int mArgNum;

    ColumnToArgFilter(ColumnInfo column, int op, int arg) {
        super(hash(column, op, arg), column, op);
        mArgNum = arg;
    }

    private static int hash(ColumnInfo column, int op, int arg) {
        int hash = column.hashCode();
        hash = hash * 31 + op;
        hash = hash * 31 + arg;
        return hash;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int isMatch(RowFilter filter) {
        if (filter == this) {
            return 1; // equal
        }
        if (filter instanceof ColumnToArgFilter) {
            var other = (ColumnToArgFilter) filter;
            if (mArgNum == other.mArgNum && mColumn.equals(other.mColumn)) {
                if (mOperator == other.mOperator) {
                    return 1; // equal
                } else if (mOperator == flipOperator(other.mOperator)) {
                    return -1; // inverse is equal
                }
            }
        }
        return 0; // doesn't match
    }

    @Override
    public int matchHashCode() {
        int hash = mMatchHashCode;
        if (hash == 0) {
            hash = mColumn.hashCode();
            hash = hash * 31 + (mOperator & ~1); // exclude the bit used to flip the operator
            hash = hash * 31 + mArgNum;
            mMatchHashCode = hash;
        }
        return hash;
    }

    @Override
    public ColumnToArgFilter not() {
        return new ColumnToArgFilter(mColumn, flipOperator(mOperator), mArgNum);
    }

    @Override
    public RowFilter[] rangeExtract(boolean reverse, ColumnInfo... keyColumns) {
        RowFilter remaining, low, high;

        match: {
            if (keyColumns[0].equals(mColumn)) {
                remaining = null;
                switch (mOperator) {
                case OP_EQ:
                    low = this.withOperator(OP_GE);
                    high = this.withOperator(OP_LE);
                    break match;
                case OP_GT: case OP_GE:
                    low = this;
                    high = null;
                    break match;
                case OP_LT: case OP_LE:
                    low = null;
                    high = this;
                    break match;
                }
            }
            remaining = this;
            low = null;
            high = null;
        }

        return new RowFilter[] {remaining, low, high};
    }

    public int argument() {
        return mArgNum;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnToArgFilter) {
            var other = (ColumnToArgFilter) obj;
            return mColumn.equals(other.mColumn) && mOperator == other.mOperator
                && mArgNum == other.mArgNum;
        }
        return false;
    }

    @Override
    boolean equalRhs(ColumnFilter other) {
        if (other instanceof ColumnToArgFilter) {
            return mArgNum == ((ColumnToArgFilter) other).mArgNum;
        }
        return false;
    }

    @Override
    ColumnToArgFilter withOperator(int op) {
        return new ColumnToArgFilter(mColumn, op, mArgNum);
    }

    @Override
    void appendTo(StringBuilder b) {
        super.appendTo(b);
        b.append('?').append(mArgNum);
    }
}
