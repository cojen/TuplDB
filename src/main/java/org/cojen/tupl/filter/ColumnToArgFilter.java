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

import java.util.Map;

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
        if (filter instanceof ColumnToArgFilter other) {
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
    public boolean isSufficient(Map<String, ColumnInfo> columns) {
        return columns.containsKey(mColumn.name);
    }

    @Override
    public RowFilter retain(Map<String, ColumnInfo> columns, boolean strict, RowFilter undecided) {
        return columns.containsKey(mColumn.name) ? this : undecided;
    }

    @Override
    public RowFilter[] rangeExtract(ColumnInfo... keyColumns) {
        ColumnToArgFilter low, high, remaining;

        match: {
            if (keyColumns[0].name.equals(mColumn.name)) {
                remaining = null;
                switch (mOperator) {
                case OP_EQ:
                    if (mColumn.type != BigDecimal.class) {
                        low = this.withOperator(OP_GE);
                        high = this.withOperator(OP_LE);
                    } else {
                        remaining = this;
                        low = this;
                        high = this;
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

        if (keyColumns[0].isDescending()) {
            var newLow = descending(high);
            high = descending(low);
            low = newLow;
        }

        return new RowFilter[] {low, high, remaining, null};
    }

    static ColumnToArgFilter descending(ColumnToArgFilter filter) {
        if (filter != null) {
            int op;
            switch (filter.mOperator) {
            case OP_GE: op = OP_LE; break;
            case OP_LT: op = OP_GT; break;
            case OP_LE: op = OP_GE; break;
            case OP_GT: op = OP_LT; break;
            default: return filter;
            }
            filter = filter.withOperator(op);
        }
        return filter;
    }

    @Override
    public boolean matchesOne(RowFilter high, ColumnInfo... keyColumns) {
        return (high instanceof ColumnToArgFilter highCol) &&
            mOperator == OP_GE && highCol.mOperator == OP_LE &&
            mArgNum == highCol.mArgNum && mColumn.name.equals(highCol.mColumn.name) &&
            keyColumns.length == 1 && keyColumns[0].name.equals(mColumn.name);
    }

    @Override
    public boolean uniqueColumn(String columnName) {
        return mOperator == OP_EQ
            && mColumn.type != BigDecimal.class
            && mColumn.name.equals(columnName);
    }

    public int argument() {
        return mArgNum;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this || obj instanceof ColumnToArgFilter other
            && mColumn.equals(other.mColumn) && mOperator == other.mOperator
            && mArgNum == other.mArgNum;
    }

    @Override
    public int compareTo(RowFilter filter) {
        if (!(filter instanceof ColumnToArgFilter other)) {
            return super.compareTo(filter);
        }
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
        return other instanceof ColumnToArgFilter ctaf && mArgNum == ctaf.mArgNum;
    }

    @Override
    public ColumnToArgFilter withOperator(int op) {
        return new ColumnToArgFilter(mColumn, op, mArgNum);
    }

    @Override
    void appendTo(StringBuilder b) {
        super.appendTo(b);
        b.append('?').append(mArgNum);
    }
}
