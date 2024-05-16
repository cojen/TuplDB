/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import java.util.Map;
import java.util.Objects;

import org.cojen.tupl.table.ColumnInfo;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.ExprFilter;
import org.cojen.tupl.table.filter.GroupFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Converts a RowFilter into an Expr, but without any source position information.
 *
 * @author Brian S. O'Neill
 */
final class ToExprVisitor implements Visitor {
    private final Map<String, ColumnExpr> mColumns;

    /**
     * @param columns map which was filled in by the Expr.toRowFilter method.
     */
    ToExprVisitor(Map<String, ColumnExpr> columns) {
        mColumns = columns;
    }

    Expr apply(RowFilter filter) {
        filter.accept(this);
        return mCurrent;
    }

    private Expr mCurrent;

    @Override
    public void visit(OrFilter filter) {
        visit(filter, Token.T_LOR);
    }

    @Override
    public void visit(AndFilter filter) {
        visit(filter, Token.T_LAND);
    }

    private void visit(GroupFilter filter, int op) {
        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mCurrent = ConstantExpr.make(-1, -1, op == Token.T_LAND);
            return;
        }

        subFilters[0].accept(this);
        Expr expr = mCurrent;

        for (int i=1; i<subFilters.length; i++) {
            subFilters[i].accept(this);
            expr = BinaryOpExpr.make(-1, -1, op, expr, mCurrent);
        }

        mCurrent = expr;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        finish(filter, ParamExpr.make(-1, -1, filter.argument()));
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        finish(filter, toColumnExpr(filter.otherColumn()));
    }

    @Override
    public void visit(ColumnToConstantFilter filter) {
        finish(filter, (ConstantExpr) filter.constant());
    }

    private void finish(ColumnFilter filter, Expr right) {
        mCurrent = BinaryOpExpr.make(-1, 1, filter.operator(),
                                     toColumnExpr(filter.column()), right);
    }

    private ColumnExpr toColumnExpr(ColumnInfo info) {
        return Objects.requireNonNull(mColumns.get(info.name));
    }

    @Override
    public void visit(ExprFilter filter) {
        mCurrent = filter.expression();
    }
}


