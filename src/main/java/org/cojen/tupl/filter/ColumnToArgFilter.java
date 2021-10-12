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

import java.math.BigDecimal;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ColumnToArgFilter extends ColumnFilter {
    final int mArgNum;
    final boolean mPlusUlp;

    ColumnToArgFilter(ColumnInfo column, int op, int arg) {
        this(column, op, arg, false);
    }

    private ColumnToArgFilter(ColumnInfo column, int op, int arg, boolean plusUlp) {
        super(hash(column, op, arg), column, op);
        mArgNum = arg;
        mPlusUlp = plusUlp;
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
            if (mArgNum == other.mArgNum && mColumn.equals(other.mColumn)
                && mPlusUlp == other.mPlusUlp)
            {
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
    public RowFilter[] rangeExtract(ColumnInfo... keyColumns) {
        RowFilter remaining, low, high;

        match: {
            if (keyColumns[0].equals(mColumn)) {
                remaining = null;
                switch (mOperator) {
                case OP_EQ:
                    if (mColumn.type != BigDecimal.class) {
                        low = this.withOperator(OP_GE);
                        high = this.withOperator(OP_LE);
                    } else {
                        remaining = this;
                        low = this.withOperator(OP_GE);
                        high = this.withOperatorPlusUlp(OP_LT);
                    }
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

    /**
     * When true, filter evaluator must add one ulp to the argument before making a comparison.
     * This is only expected for BigDecimal columns, when part of a high range bound.
     */
    public boolean plusUlp() {
        return mPlusUlp;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnToArgFilter) {
            var other = (ColumnToArgFilter) obj;
            return mColumn.equals(other.mColumn) && mOperator == other.mOperator
                && mArgNum == other.mArgNum && mPlusUlp == other.mPlusUlp;
        }
        return false;
    }

    @Override
    public int compareTo(RowFilter filter) {
        if (!(filter instanceof ColumnToArgFilter)) {
            return super.compareTo(filter);
        }
        var other = (ColumnToArgFilter) filter;
        int cmp = mColumn.name.compareTo(other.mColumn.name);
        if (cmp == 0) {
            cmp = Integer.compare(mOperator, other.mOperator);
            if (cmp == 0) {
                cmp = Integer.compare(mArgNum, other.mArgNum);
            }
        }
        return cmp;
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
        return new ColumnToArgFilter(mColumn, op, mArgNum, mPlusUlp);
    }

    ColumnToArgFilter withOperatorPlusUlp(int op) {
        return new ColumnToArgFilter(mColumn, op, mArgNum, true);
    }

    @Override
    void appendTo(StringBuilder b) {
        super.appendTo(b);
        if (mPlusUlp) {
            b.append('+');
        }
        b.append('?').append(mArgNum);
    }
}
