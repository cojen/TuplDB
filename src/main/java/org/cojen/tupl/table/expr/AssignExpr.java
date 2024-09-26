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

import org.cojen.tupl.table.RowMethodsMaker;

/**
 * Defines an expression which evaluates an expression, stores it to a local variable, and then
 * returns the variable.
 *
 * @author Brian S. O'Neill
 */
public final class AssignExpr extends WrappedExpr implements Attr {
    public static AssignExpr make(int startPos, int endPos, String name, Expr expr) {
        return new AssignExpr(startPos, endPos, name, expr);
    }

    private final String mName;

    private AssignExpr(int startPos, int endPos, String name, Expr expr) {
        super(startPos, endPos, expr);
        mName = name;
    }

    @Override
    public String name() {
        return mName;
    }

    @Override
    public Expr asType(Type type) {
        return type.equals(type()) ? this
            : new AssignExpr(startPos(), endPos(), mName, mExpr.asType(type));
    }

    @Override
    public AssignExpr asAggregate(Set<String> group) {
        Expr expr = mExpr.asAggregate(group);
        return expr == mExpr ? this : new AssignExpr(startPos(), endPos(), mName, expr);
    }

    @Override
    public AssignExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        Expr expr = mExpr.asWindow(newAssignments);
        return expr == mExpr ? this : new AssignExpr(startPos(), endPos(), mName, expr);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }
        replaced = mExpr.replace(replacements);
        return replaced == mExpr ? this : new AssignExpr(startPos(), endPos(), mName, replaced);
    }

    @Override
    public Expr negate(int startPos, boolean widen) {
        return new AssignExpr(startPos, endPos(), mName, mExpr.negate(widen));
    }

    @Override
    public Expr not(int startPos) {
        return new AssignExpr(startPos, endPos(), mName, mExpr.not());
    }

    @Override
    public boolean supportsLogicalNot() {
        return mExpr.supportsLogicalNot();
    }

    @Override
    public boolean isTrivial() {
        return mExpr.isTrivial();
    }

    @Override
    public boolean isNull() {
        return mExpr.isNull();
    }

    @Override
    public boolean isZero() {
        return mExpr.isZero();
    }

    @Override
    public boolean isOne() {
        return mExpr.isOne();
    }

    @Override
    public ColumnExpr sourceColumn() {
        return mExpr.sourceColumn();
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        Variable dest = context.findOrDeclareLocalVar(type(), name());
        setAny(dest, mExpr.makeEval(context));
        return dest;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeString(mName);
            mExpr.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return mName.hashCode() * 31 + mExpr.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof AssignExpr ae && mName.equals(ae.mName) && mExpr.equals(ae.mExpr);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        b.append(RowMethodsMaker.unescape(mName)).append(" = ").append(mExpr);
    }
}
