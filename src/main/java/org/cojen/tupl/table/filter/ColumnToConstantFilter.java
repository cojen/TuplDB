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

package org.cojen.tupl.table.filter;

import java.util.Map;
import java.util.Objects;

import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class ColumnToConstantFilter extends ColumnFilter {
    private final Object mConstant;

    public ColumnToConstantFilter(ColumnInfo column, int op, Object constant) {
        super(hash(column, op, constant), column, op);
        mConstant = constant;
    }

    private static int hash(ColumnInfo column, int op, Object constant) {
        int hash = column.hashCode();
        hash = hash * 31 + op;
        hash = hash * 31 + Objects.hashCode(constant);
        return hash;
    }

    public Object constant() {
        return mConstant;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected int maxArgument(int max) {
        return max;
    }

    @Override
    public int isMatch(RowFilter filter) {
        if (filter == this) {
            return 1; // equal
        }
        if (filter instanceof ColumnToConstantFilter other) {
            if (mColumn.equals(other.mColumn) && Objects.equals(mConstant, other.mConstant)) {
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
            hash = hash * 31 + Objects.hashCode(mConstant);
            mMatchHashCode = hash;
        }
        return hash;
    }

    @Override
    public RowFilter replaceArguments(IntUnaryOperator function) {
        return this;
    }

    @Override
    public RowFilter argumentAsNull(int argNum) {
        return this;
    }

    @Override
    public RowFilter constantsToArguments(ToIntFunction<ColumnToConstantFilter> function) {
        int argNum = function.applyAsInt(this);
        if (argNum == 0) {
            return this;
        } else {
            return new ColumnToArgFilter(mColumn, mOperator, argNum);
        }
    }

    @Override
    public RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided) {
        return pred.test(mColumn.name) ? this : undecided;
    }

    @Override
    protected boolean canSplit(Map<String, ? extends ColumnInfo> columns) {
        return columnExists(columns, mColumn);
    }

    @Override
    public boolean uniqueColumn(String columnName) {
        return false;
    }

    @Override
    boolean equalRhs(ColumnFilter other) {
        return other instanceof ColumnToConstantFilter ctcf
            && Objects.equals(mConstant, ctcf.mConstant);
    }

    @Override
    ColumnToConstantFilter withOperator(int op) {
        return new ColumnToConstantFilter(mColumn, op, mConstant);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof ColumnToConstantFilter other
            && mColumn.equals(other.mColumn) && mOperator == other.mOperator
            && Objects.equals(mConstant, other.mConstant);
    }

    @Override
    public int compareTo(RowFilter filter) {
        if (!(filter instanceof ColumnToConstantFilter other)) {
            return super.compareTo(filter);
        }
        int cmp = mColumn.name.compareTo(other.mColumn.name);
        if (cmp == 0) {
            cmp = Integer.compare(mOperator, other.mOperator);
        }
        return cmp;
    }

    @Override
    public void appendTo(StringBuilder b) {
        super.appendTo(b);

        if (mConstant instanceof String s) {
            RowUtils.appendQuotedString(b, s);
        } else if (mConstant instanceof Character c) {
            RowUtils.appendQuotedString(b, c);
        } else {
            b.append(mConstant);
        }
    }
}
