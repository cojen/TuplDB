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

import org.cojen.maker.Variable;

import java.math.BigInteger;

/**
 * Represents a bitwise integer not operation (ones' complement).
 *
 * @author Brian S. O'Neill
 */
final class NotExpr extends WrappedExpr {
    public static Expr make(int startPos, int endPos, Expr expr) {
        if (expr instanceof NotExpr ne) {
            return ne.wrapped();
        }
        return new NotExpr(startPos, endPos, expr);
    }

    private NotExpr(int startPos, int endPos, Expr expr) {
        super(startPos, endPos, expr);
    }

    @Override
    public Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
    }

    @Override
    public NotExpr asAggregate(Set<String> group) {
        Expr expr = mExpr.asAggregate(group);
        return expr == mExpr ? this : new NotExpr(startPos(), endPos(), expr);
    }

    @Override
    public NotExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        Expr expr = mExpr.asWindow(newAssignments);
        return expr == mExpr ? this : new NotExpr(startPos(), endPos(), expr);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }
        replaced = mExpr.replace(replacements);
        return replaced == mExpr ? this : new NotExpr(startPos(), endPos(), replaced);
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        var v = mExpr.makeEval(context);
        if (BigInteger.class.isAssignableFrom(v.classType())) {
            v = v.invoke("not");
        } else {
            v = v.com();
        }
        return v;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mExpr.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return -1745513034 ^ mExpr.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof NotExpr ne && mExpr.equals(ne.mExpr);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        b.append('!').append('(');
        mExpr.appendTo(b);
        b.append(')');
    }
}
