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
    public ColumnToArgFilter not() {
        return new ColumnToArgFilter(mColumn, flipOperator(mOperator), mArgNum);
    }

    public int argument() {
        return mArgNum;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        // Note: Not using instanceof because InFilter is a subclass.
        if (getClass() == obj.getClass()) {
            var other = (ColumnToArgFilter) obj;
            return mColumn.equals(other.mColumn) && mOperator == other.mOperator
                && mArgNum == other.mArgNum;
        }
        return false;
    }

    @Override
    void appendTo(StringBuilder b) {
        super.appendTo(b);
        b.append('?').append(mArgNum);
    }
}
