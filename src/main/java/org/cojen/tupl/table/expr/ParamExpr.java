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
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.tupl.table.ConvertCallSite;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

/**
 * Defines a parameter which is passed into a query at runtime.
 *
 * @author Brian S. O'Neill
 */
public final class ParamExpr extends Expr {
    public static ParamExpr make(int startPos, int endPos, int ordinal) {
        return make(startPos, endPos, AnyType.THE, ordinal);
    }

    public static ParamExpr make(int startPos, int endPos, Type type, int ordinal) {
        return new ParamExpr(startPos, endPos, type, ordinal);
    }

    private final Type mType;
    private final int mOrdinal;

    private ParamExpr(int startPos, int endPos, Type type, int ordinal) {
        super(startPos, endPos);
        mType = type;
        mOrdinal = ordinal;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public ParamExpr asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }
        return new ParamExpr(startPos(), endPos(), type, mOrdinal);
    }

    @Override
    public int maxArgument() {
        return mOrdinal;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return mType.isNullable();
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isOrderDependent() {
        return false;
    }

    @Override
    public boolean isGrouping() {
        return false;
    }

    @Override
    public boolean isAccumulating() {
        return false;
    }

    @Override
    public boolean isAggregating() {
        return false;
    }

    @Override
    public ParamExpr asAggregate(Set<String> group) {
        return this;
    }

    @Override
    public ParamExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        return this;
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        var value = context.argsVar().aget(mOrdinal - 1);
        Class<?> clazz = mType.clazz();
        if (clazz != Object.class) {
            value = ConvertCallSite.make(context.methodMaker(), clazz, value);
        }
        return value;
    }

    @Override
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        ConvertCallSite.make(context.methodMaker(), boolean.class, makeEval(context)).ifTrue(pass);
        fail.goto_();
    }

    @Override
    public boolean canThrowRuntimeException() {
        // Can throw an exception due to a conversion error.
        return mType.clazz() != Object.class;
    }

    public int ordinal() {
        return mOrdinal;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeUnsignedVarInt(mOrdinal);
        }
    }

    @Override
    public int hashCode() {
        return mOrdinal * 970840757;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof ParamExpr pe && mOrdinal == pe.mOrdinal;
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        b.append('?').append(mOrdinal);
    }
}
