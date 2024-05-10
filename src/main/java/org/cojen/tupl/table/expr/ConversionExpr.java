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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.Converter;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class ConversionExpr extends WrappedExpr {
    /**
     * Returns a ConversionExpr if the given type differs from the expression's current type.
     */
    public static Expr make(int startPos, int endPos, Expr expr, Type type) {
        if (expr.type().equals(type)) {
            return expr;
        }

        if (type == BasicType.BOOLEAN) {
            throw new QueryException("Cannot convert " + expr.type() + " to " + type, expr);
        }

        return new ConversionExpr(startPos, endPos, expr, type);
    }

    private final Type mType;

    private ConversionExpr(int startPos, int endPos, Expr expr, Type type) {
        super(startPos, endPos, expr);
        mType = type;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Expr asType(Type type) {
        return make(startPos(), endPos(), this, type);
    }

    @Override
    public Variable makeEval(EvalContext context) {
        MethodMaker mm = context.methodMaker();
        var dstVar = mm.var(mType.clazz());
        String name = mExpr instanceof Named n ? n.name() : null;
        Converter.convertExact(mm, name, mExpr.type(), mExpr.makeEval(context), mType, dstVar);
        return dstVar;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mType.encodeKey(enc);
            mExpr.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + mExpr.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof ConversionExpr ce
            && mType.equals(ce.mType) && mExpr.equals(ce.mExpr);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b) {
        b.append('(');
        mType.appendTo(b, false);
        b.append(')').append('(');
        mExpr.appendTo(b);
        b.append(')');
    }
}
