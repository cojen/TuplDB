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

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.cojen.tupl.table.expr.Expr;

/**
 * Defines a filter term which references an expression that cannot be deeply analyzed.
 *
 * @author Brian S. O'Neill
 */
public final class ExprFilter extends TermFilter {
    private final Expr mExpr;

    public ExprFilter(Expr expr) {
        super(expr.hashCode());
        mExpr = expr;
    }

    public Expr expression() {
        return mExpr;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected int maxArgument(int max) {
        return Math.max(max, mExpr.maxArgument());
    }

    @Override
    RowFilter expandOperators(boolean force) {
        return this;
    }

    @Override
    public int isMatch(RowFilter filter) {
        return equals(filter) ? 1 : 0;
    }

    @Override
    public int matchHashCode() {
        return hashCode();
    }

    @Override
    public RowFilter not() {
        return new ExprFilter(mExpr.not());
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
        return this;
    }

    @Override
    public RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided) {
        return undecided;
    }

    @Override
    protected RowFilter trySplit(Function<ColumnFilter, RowFilter> check) {
        return null;
    }

    @Override
    public boolean uniqueColumn(String columnName) {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof ExprFilter other && mExpr.equals(other.mExpr);
    }

    @Override
    public void appendTo(StringBuilder b) {
        b.append('(');
        mExpr.appendTo(b);
        b.append(')');
    }
}
