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

import java.util.function.Consumer;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
abstract sealed class WrappedExpr extends Expr
    permits AssignExpr, ConversionExpr, NotExpr, ProjExpr
{
    protected final Expr mExpr;

    protected WrappedExpr(int startPos, int endPos, Expr expr) {
        super(startPos, endPos);
        mExpr = expr;
    }

    public final Expr wrapped() {
        return mExpr;
    }

    @Override
    public Type type() {
        return mExpr.type();
    }

    @Override
    public final int maxArgument() {
        return mExpr.maxArgument();
    }

    @Override
    public final boolean isPureFunction() {
        return mExpr.isPureFunction();
    }

    @Override
    public boolean isNullable() {
        return mExpr.isNullable();
    }

    @Override
    public final boolean isConstant() {
        return mExpr.isConstant();
    }

    @Override
    public final boolean isOrderDependent() {
        return mExpr.isOrderDependent();
    }

    @Override
    public final boolean isGrouping() {
        return mExpr.isGrouping();
    }

    @Override
    public final boolean isAccumulating() {
        return mExpr.isAccumulating();
    }

    @Override
    public final boolean isAggregating() {
        return mExpr.isAggregating();
    }

    @Override
    public final void gatherEvalColumns(Consumer<Column> c) {
        mExpr.gatherEvalColumns(c);
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mExpr.canThrowRuntimeException();
    }
}
