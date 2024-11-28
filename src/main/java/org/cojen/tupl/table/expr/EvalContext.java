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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import java.util.function.Consumer;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a code generation context.
 *
 * @author Brian S. O'Neill
 */
class EvalContext implements FunctionApplier.GroupContext {
    private final Variable mArgsVar, mSourceRowVar;

    private Map<Object, ResultRef> mEvaluated;
    private ResultRef[] mUndoLog;
    private int mUndoSize;

    private Map<String, Variable> mLocalVars;

    EvalContext(Variable argsVar, Variable sourceRowVar) {
        mArgsVar = argsVar;
        mSourceRowVar = sourceRowVar;
    }

    @Override
    public MethodMaker methodMaker() {
        return mSourceRowVar.methodMaker();
    }

    /**
     * Returns a context to use for the FunctionApplier Aggregated/Grouped init method. Throws
     * IllegalStateException when not supported.
     */
    EvalContext initContext() {
        throw new IllegalStateException();
    }

    /**
     * Returns a context to use for the FunctionApplier Aggregated/Grouped begin method. Throws
     * IllegalStateException when not supported.
     */
    EvalContext beginContext() {
        throw new IllegalStateException();
    }

    /**
     * Returns a context to use for the FunctionApplier Aggregated/Grouped accumulate method.
     * Throws IllegalStateException when not supported.
     */
    EvalContext accumContext() {
        throw new IllegalStateException();
    }

    /**
     * Returns a context to use for the FunctionApplier Grouped finished method. Throws
     * IllegalStateException when not supported.
     */
    EvalContext finishedContext() {
        throw new IllegalStateException();
    }

    /**
     * Passes a context to use for the FunctionApplier Grouped check method. Throws
     * IllegalStateException when not supported.
     */
    void checkContext(Consumer<EvalContext> body) {
        throw new IllegalStateException();
    }

    /**
     * Returns an object array of arguments, used by ParamExpr. Is null if no arguments.
     */
    final Variable argsVar() {
        return mArgsVar;
    }

    /**
     * Returns the source row with columns, used by ColumnExpr.
     */
    final Variable sourceRowVar() {
        return mSourceRowVar;
    }

    /**
     * Returns a reference to an already evaluated result for the given expression. Should only
     * be used for non-trivial expressions which are pure functions. There's no point in
     * stashing references to trivial expressions because evaluating them again is cheap.
     */
    final ResultRef refFor(Object expr) {
        Map<Object, ResultRef> evaluated = mEvaluated;
        if (evaluated == null) {
            mEvaluated = evaluated = new HashMap<>();
        }
        return evaluated.computeIfAbsent(expr, n -> new ResultRef());
    }

    /**
     * Returns a savepoint which can be used to rollback ref assignments.
     *
     * @return a non-negative int
     */
    final int refSavepoint() {
        if (mUndoLog == null) {
            mUndoLog = new ResultRef[10];
            return 0;
        } else {
            return mUndoSize;
        }
    }

    /**
     * Rollback to a savepoint, invalidating all ref assignments.
     */
    final void refRollback(int savepoint) {
        ResultRef[] undo = mUndoLog;
        for (int i=mUndoSize; --i>= savepoint; ) {
            undo[i].invalidate();
        }
        mUndoSize = savepoint;
    }

    /**
     * Commit all ref assignments up to a savepoint.
     */
    final void refCommit(int savepoint) {
        mUndoSize = savepoint;
    }

    private void undoPush(ResultRef ref) {
        ResultRef[] undo = mUndoLog;
        if (undo != null) {
            int size = mUndoSize;
            if (size >= undo.length) {
                mUndoLog = undo = Arrays.copyOf(undo, size << 1);
            }
            undo[size] = ref;
            mUndoSize = size + 1;
        }
    }

    /**
     * @param type only used if the variable needs to be declared
     */
    final Variable findOrDeclareLocalVar(Type type, String name) {
        if (mLocalVars == null) {
            mLocalVars = new HashMap<>();
        }
        return mLocalVars.computeIfAbsent(name, k -> methodMaker().var(type.clazz()));
    }

    /**
     * @return null if not found
     */
    final Variable findLocalVar(String name) {
        return mLocalVars == null ? null : mLocalVars.get(name);
    }

    class ResultRef {
        private Variable mVar;
        private boolean mIsValid;

        Object attachment;

        /**
         * @return null if no valid result is available
         */
        Variable get() {
            return mIsValid ? mVar : null;
        }

        /**
         * @param var must not be null
         * @return the actual variable that should be used
         */
        Variable set(Variable var) {
            return toSet(var.classType()).set(var);
        }

        /**
         * @return a validated variable that must be set
         */
        Variable toSet(Class type) {
            if (mVar == null || mVar.classType() != type) {
                mVar = methodMaker().var(type);
            }
            mIsValid = true;
            undoPush(this);
            return mVar;
        }

        void invalidate() {
            mIsValid = false;
        }
    }
}
