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

package org.cojen.tupl.model;

import org.cojen.maker.Label;

import org.cojen.tupl.rows.filter.AndFilter;
import org.cojen.tupl.rows.filter.ColumnToArgFilter;
import org.cojen.tupl.rows.filter.ColumnToColumnFilter;
import org.cojen.tupl.rows.filter.ColumnToConstantFilter;
import org.cojen.tupl.rows.filter.OpaqueFilter;
import org.cojen.tupl.rows.filter.OrFilter;
import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.Visitor;

/**
 * Generates filtering code from a RowFilter.
 *
 * @author Brian S. O'Neill
 * @see Node#makeFilter
 */
final class FilterVisitor implements Visitor {
    private final EvalContext mContext;
    private final Label mPass, mFail;

    FilterVisitor(EvalContext context, Label pass, Label fail) {
        mContext = context;
        mPass = pass;
        mFail = fail;
    }

    void apply(RowFilter filter) {
        filter.accept(this);
    }

    @Override
    public void visit(OrFilter filter) {
        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mFail.goto_();
            return;
        }

        System.out.println("visit: " + filter);
        // FIXME: See BinaryOpNode
        throw null;
    }

    @Override
    public void visit(AndFilter filter) {
        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mPass.goto_();
            return;
        }

        System.out.println("visit: " + filter);
        // FIXME: See BinaryOpNode
        throw null;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        // FIXME: access mContext.rowVar and mContext.argsVar
        // FIXME: This line is from ParamNode. Do I try to keep the a ref to arg value?
        //        I can make a ParamNode instance as a cache key.
        //ConvertCallSite.make(context.methodMaker(), boolean.class, makeEval(context)).ifTrue(pass);
        //fail.goto_();
        throw null;
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        // FIXME: access mContext.rowVar twice
        throw null;
    }

    @Override
    public void visit(ColumnToConstantFilter filter) {
        // FIXME: access mContext.rowVar
        throw null;
    }

    @Override
    public void visit(OpaqueFilter filter) {
        ((Node) filter.attachment()).makeFilter(mContext, mPass, mFail);
    }
}
