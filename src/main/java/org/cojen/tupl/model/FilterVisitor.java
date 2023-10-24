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
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.CompareUtils;

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
    private Label mPass, mFail;

    FilterVisitor(EvalContext context, Label pass, Label fail) {
        mContext = context;
        mPass = pass;
        mFail = fail;
    }

    void apply(RowFilter filter) {
        filter.accept(this);
    }

    // Note that the OrFilter and AndFilter logic is similar to how DecodeVisitor does it.

    @Override
    public void visit(OrFilter filter) {
        final Label originalFail = mFail;

        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            originalFail.goto_();
            return;
        }

        MethodMaker mm = mContext.methodMaker();

        mFail = mm.label();
        subFilters[0].accept(this);
        mFail.here();

        int savepoint = mContext.refSavepoint();

        for (int i=1; i<subFilters.length; i++) {
            mFail = mm.label();
            subFilters[i].accept(this);
            mFail.here();
        }

        // Rollback the refs for all nodes but the first, because they don't always execute.
        mContext.refRollback(savepoint);

        originalFail.goto_();
        mFail = originalFail;
    }

    @Override
    public void visit(AndFilter filter) {
        final Label originalPass = mPass;

        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            originalPass.goto_();
            return;
        }

        MethodMaker mm = mContext.methodMaker();

        mPass = mm.label();
        subFilters[0].accept(this);
        mPass.here();

        int savepoint = mContext.refSavepoint();

        for (int i=1; i<subFilters.length; i++) {
            mPass = mm.label();
            subFilters[i].accept(this);
            mPass.here();
        }

        // Rollback the refs for all nodes but the first, because they don't always execute.
        mContext.refRollback(savepoint);

        originalPass.goto_();
        mPass = originalPass;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        // The parameter type must be converted to be the same type as the column. Although
        // ConvertCallSite could be called directly, using a ParamNode allows for reference
        // caching in the EvalContext.
        ColumnInfo info = filter.column();
        ParamNode pn = ParamNode.make(null, BasicType.make(info), filter.argument());

        CompareUtils.compare(mContext.methodMaker(),
                             info, accessColumn(filter.column()),
                             info, pn.makeEval(mContext),
                             filter.operator(), mPass, mFail);
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        CompareUtils.compare(mContext.methodMaker(),
                             filter.column(), accessColumn(filter.column()),
                             filter.otherColumn(), accessColumn(filter.otherColumn()),
                             filter.operator(), mPass, mFail);
    }

    @Override
    public void visit(ColumnToConstantFilter filter) {
        // Assume that BinaryOpNode has already ensured that the left and right sides are of
        // the same type, and so the ColumnInfo can be used for both sides.
        ColumnInfo info = filter.column();

        CompareUtils.compare(mContext.methodMaker(),
                             info, accessColumn(filter.column()),
                             info, ConstantNode.makeEval(mContext, info.type, filter.constant()),
                             filter.operator(), mPass, mFail);
    }

    private Variable accessColumn(ColumnInfo info) {
        return mContext.rowVar.invoke(info.name);
    }

    @Override
    public void visit(OpaqueFilter filter) {
        ((Node) filter.attachment()).makeFilter(mContext, mPass, mFail);
    }
}
