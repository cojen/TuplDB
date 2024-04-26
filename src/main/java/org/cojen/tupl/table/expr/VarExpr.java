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

import org.cojen.maker.Variable;

/**
 * Defines an expression which reads a named local variable.
 *
 * @author Brian S. O'Neill
 */
public final class VarExpr extends Expr implements Named {
    public static VarExpr make(int startPos, int endPos, Type type, String name) {
        return new VarExpr(startPos, endPos, type, name);
    }

    private final Type mType;
    private final String mName;

    private VarExpr(int startPos, int endPos, Type type, String name) {
        super(startPos, endPos);
        mType = type;
        mName = name;
    }

    @Override
    public String name() {
        return mName;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
    }

    @Override
    public int maxArgument() {
        return 0;
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
    public void gatherEvalColumns(Consumer<Column> c) {
    }

    @Override
    public Variable makeEval(EvalContext context) {
        return context.findLocalVar(mName).get();
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mType.encodeKey(enc);
            enc.encodeString(mName);
        }
    }

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + mName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof VarExpr ve && mType.equals(ve.mType) && mName.equals(ve.mName);
    }

    @Override
    public String toString() {
        return mName;
    }
}
