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

import org.cojen.maker.Variable;

/**
 * @author Brian S. O'Neill
 * @see FunctionApplier
 */
public class LazyValue {
    private final EvalContext mContext;
    private final Expr mExpr;

    private Variable mEvaluated;

    LazyValue(EvalContext context, Expr expr) {
        mContext = context;
        mExpr = expr;
    }

    public Expr expr() {
        return mExpr;
    }

    /**
     * Returns true if the value represents a constant.
     */
    public boolean isConstant() {
        return false;
    }

    /**
     * Returns a constant value, which is only applicable if isConstant returns true.
     */
    public Object constantValue() {
        return null;
    }

    /**
     * Evaluates the value.
     *
     * @param eager when true, the value is guaranteed to be evaluated for all execution paths
     */
    public final Variable eval(boolean eager) {
        Variable var = mEvaluated;

        if (var == null) {
            EvalContext context = mContext;
            if (!eager) {
                var = mExpr.makeEval(context);
            } else {
                int savepoint = context.refSavepoint();
                var = mExpr.makeEval(context);
                context.refCommit(savepoint);
            }
            mEvaluated = var;
        }

        return var;
    }
}
