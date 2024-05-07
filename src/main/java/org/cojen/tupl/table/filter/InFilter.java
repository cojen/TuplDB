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

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class InFilter extends ColumnToArgFilter {
    public InFilter(ColumnInfo column, int arg) {
        this(column, OP_IN, arg);
    }

    private InFilter(ColumnInfo column, int op, int arg) {
        super(column, op, arg);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void appendTo(StringBuilder b) {
        if (mOperator == OP_NOT_IN) {
            b.append('!').append('(');
        }

        b.append(mColumn.name).append(' ').append("in").append(' ').append('?').append(mArgNum);

        if (mOperator == OP_NOT_IN) {
            b.append(')');
        }
    }

    @Override
    public InFilter not() {
        return new InFilter(mColumn, flipOperator(mOperator), mArgNum);
    }
}
