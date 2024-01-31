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

package org.cojen.tupl.model;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a node for supporting "case" expressions.
 *
 * @author Brian S. O'Neill
 */
public final class CaseNode extends Node {
    /**
     * @param conditions required; each is expected to evaluate to a boolean
     * @param results required
     * @param elseResult optional; is ConstantNode.NULL when not provided
     * @throw IllegalArgumentException if number of conditions doesn't match number of results
     */
    public static CaseNode make(Node[] conditions, Node[] results, Node elseResult) {
        if (conditions.length != results.length) {
            throw new IllegalArgumentException();
        }
        if (elseResult == null) {
            elseResult = ConstantNode.NULL;
        }

        conditions = conditions.clone();
        for (int i=0; i<conditions.length; i++) {
            conditions[i] = conditions[i].asType(BasicType.BOOLEAN);
        }

        Type type = elseResult.type();

        if (elseResult.isNullable()) {
            type = type.nullable();
        }

        for (Node result : results) {
            type = type.commonType(result, -1);
            if (type == null) {
                type = AnyType.THE;
                break;
            }
        }

        elseResult = elseResult.asType(type);

        results = results.clone();
        for (int i=0; i<results.length; i++) {
            results[i] = results[i].asType(type);
        }

        return new CaseNode(type, null, conditions, results, elseResult);
    }

    private final Type mType;
    private String mName;
    private final Node[] mConditions;
    private final Node[] mResults;
    private final Node mElseResult;

    private CaseNode(Type type, String name, Node[] conditions, Node[] results, Node elseResult) {
        mType = type;
        mName = name;
        mConditions = conditions;
        mResults = results;
        mElseResult = elseResult;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        return ConversionNode.make(this, type);
    }

    @Override
    public Node negate() {
        if (!mType.isSignedNumber()) {
            return super.negate();
        }
        Node elseResult = mElseResult.negate();
        Node[] results = mResults.clone();
        for (int i=0; i<results.length; i++) {
            results[i] = results[i].negate();
        }
        return new CaseNode(mType, mName, mConditions, results, elseResult);
    }

    @Override
    public Node not() {
        if (mType != BasicType.BOOLEAN) {
            return super.not();
        }
        Node elseResult = mElseResult.not();
        Node[] results = mResults.clone();
        for (int i=0; i<results.length; i++) {
            results[i] = results[i].not();
        }
        return new CaseNode(mType, mName, mConditions, results, elseResult);
    }

    @Override
    public String name() {
        if (mName == null) {
            mName = "case";
        }
        return mName;
    }

    @Override
    public Node withName(String name) {
        return name.equals(mName) ? this
            : new CaseNode(mType, name, mConditions, mResults, mElseResult);
    }

    @Override
    public int maxArgument() {
        int max = mElseResult.maxArgument();
        for (Node n : mConditions) {
            max = Math.max(max, n.maxArgument());
        }
        for (Node n : mResults) {
            max = Math.max(max, n.maxArgument());
        }
        return max;
    }

    @Override
    public boolean isPureFunction() {
        if (!mElseResult.isPureFunction()) {
            return false;
        }
        for (Node n : mConditions) {
            if (!n.isPureFunction()) {
                return false;
            }
        }
        for (Node n : mResults) {
            if (!n.isPureFunction()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isNullable() {
        return mType.isNullable();
    }

    @Override
    public void evalColumns(Set<String> columns) {
        mElseResult.evalColumns(columns);
        for (Node n : mConditions) {
            n.evalColumns(columns);
        }
        for (Node n : mResults) {
            n.evalColumns(columns);
        }
    }

    @Override
    public Variable makeEval(EvalContext context) {
        EvalContext.ResultRef resultRef;

        if (isPureFunction()) {
            resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }
        } else {
            resultRef = null;
        }

        MethodMaker mm = context.methodMaker();
        var resultVar = mm.var(mType.clazz());
        Label done = mm.label();

        // TODO: Generate a switch statement if possible.

        int savepoint = -1;

        for (int i=0; i<mConditions.length; i++) {
            Label next = mm.label();
            mConditions[i].makeEval(context).ifFalse(next);
            if (i == 0) {
                // Only the first condition is guaranteed to execute.
                savepoint = context.refSavepoint();
            }
            resultVar.set(mResults[i].makeEval(context));
            context.refRollback(savepoint);
            done.goto_();
            next.here();
        }

        resultVar.set(mElseResult.makeEval(context));

        if (savepoint < 0) {
            context.refRollback(savepoint);
        }

        done.here();

        if (resultRef != null) {
            resultVar = resultRef.set(resultVar);
        }

        return resultVar;
    }

    @Override
    public boolean canThrowRuntimeException() {
        if (mElseResult.canThrowRuntimeException()) {
            return true;
        }
        for (Node n : mConditions) {
            if (n.canThrowRuntimeException()) {
                return true;
            }
        }
        for (Node n : mResults) {
            if (n.canThrowRuntimeException()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public CaseNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        Node[] conditions = Node.replaceConstants(mConditions, map, prefix);
        Node[] results = Node.replaceConstants(mResults, map, prefix);
        Node elseResult = mElseResult.replaceConstants(map, prefix);
        if (conditions == mConditions && results == mResults && elseResult == mElseResult) {
            return this;
        }
        return new CaseNode(mType, mName, conditions, results, elseResult);
    }

    @Override
    public int hashCode() {
        int hash = mElseResult.hashCode();
        for (Node n : mConditions) {
            hash = hash * 31 + n.hashCode();
        }
        for (Node n : mResults) {
            hash = hash * 31 + n.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CaseNode cn && mElseResult.equals(cn.mElseResult)
            && Arrays.equals(mConditions, cn.mConditions)
            && Arrays.equals(mResults, cn.mResults);
    }
}
