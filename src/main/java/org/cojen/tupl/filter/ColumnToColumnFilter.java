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
public final class ColumnToColumnFilter extends ColumnFilter {
    final ColumnInfo mMatchColumn;

    ColumnToColumnFilter(ColumnInfo column, int op, ColumnInfo match) {
        super(hash(column, op, match), column, op);
        mMatchColumn = match;
    }

    private static int hash(ColumnInfo column, int op, ColumnInfo match) {
        int hash = column.hashCode();
        hash = hash * 31 + op;
        hash = hash * 31 + match.hashCode();
        return hash;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public ColumnToColumnFilter not() {
        return new ColumnToColumnFilter(mColumn, flipOperator(mOperator), mMatchColumn);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnToColumnFilter) {
            var other = (ColumnToColumnFilter) obj;
            return mColumn.equals(other.mColumn) && mOperator == other.mOperator
                && mMatchColumn.equals(other.mMatchColumn);
        }
        return false;
    }

    @Override
    void appendTo(StringBuilder b) {
        super.appendTo(b);
        b.append(mMatchColumn.name());
    }
}
