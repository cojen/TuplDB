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

import org.cojen.tupl.table.RowMethodsMaker;

/**
 * Defines a projection term.
 *
 * @author Brian S. O'Neill
 */
public final class ProjExpr extends WrappedExpr implements Named {
    // Basic flags.
    public static int F_EXCLUDE = 1, F_ORDER_BY = 2;

    // These flags are only applicable when F_ORDER_BY is set.
    public static int F_DESCENDING = Type.TYPE_DESCENDING, F_NULL_LOW = Type.TYPE_NULL_LOW;

    /**
     * @param expr expected to be AssignExpr, ColumnExpr, or VarExpr
     */
    public static ProjExpr make(int startPos, int endPos, Expr expr, int flags) {
        if (!(expr instanceof Named)) {
            throw new IllegalArgumentException();
        }
        return new ProjExpr(startPos, endPos, expr, flags);
    }

    private final int mFlags;

    /**
     * @param expr must also implement Named
     */
    private ProjExpr(int startPos, int endPos, Expr expr, int flags) {
        super(startPos, endPos, expr);
        mFlags = flags;
    }

    public boolean hasExclude() {
        return (mFlags & F_EXCLUDE) != 0;
    }

    public boolean hasOrderBy() {
        return (mFlags & F_ORDER_BY) != 0;
    }

    /**
     * Is only applicable when orderBy returns true.
     */
    public boolean hasDescending() {
        return (mFlags & F_DESCENDING) != 0;
    }

    /**
     * Is only applicable when orderBy returns true.
     */
    public boolean hasNullLow() {
        return (mFlags & F_NULL_LOW) != 0;
    }

    public ProjExpr withExclude() {
        int flags = mFlags;
        if ((flags & F_EXCLUDE) != 0) {
            return this;
        }
        return new ProjExpr(startPos(), endPos(), mExpr, flags | F_EXCLUDE);
    }

    /**
     * Is only applicable when orderBy returns true.
     *
     * @param typeCode ColumnInfo type code to modify
     * @return modified typeCode
     */
    public int applyOrderBy(int typeCode) {
        int mask = F_DESCENDING | F_NULL_LOW;
        return (typeCode & ~mask) | (mFlags & mask);
    }

    @Override
    public String name() {
        return ((Named) mExpr).name();
    }

    @Override
    public Expr asType(Type type) {
        return type.equals(type()) ? this
            : new ProjExpr(startPos(), endPos(), mExpr.asType(type), mFlags);
    }

    @Override
    public Expr negate(int startPos, boolean widen) {
        return new ProjExpr(startPos, endPos(), mExpr.negate(widen), mFlags);
    }

    @Override
    public Expr not(int startPos) {
        return new ProjExpr(startPos, endPos(), mExpr.not(), mFlags);
    }

    @Override
    public boolean supportsLogicalNot() {
        return mExpr.supportsLogicalNot();
    }

    @Override
    public ColumnExpr sourceColumn() {
        return mExpr.sourceColumn();
    }

    /**
     * Returns the sourceColumn (if any) wrapped as a new ProjExpr.
     */
    public ProjExpr sourceProjColumn() {
        ColumnExpr ce = sourceColumn();
        return ce == null ? null : make(ce.startPos(), ce.endPos(), ce, mFlags);
    }

    @Override
    public Variable makeEval(EvalContext context) {
        return mExpr.makeEval(context);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            assert mFlags < 65536;
            enc.encodeShort(mFlags);
            mExpr.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return mExpr.hashCode() + mFlags;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof ProjExpr pe && mExpr.equals(pe.mExpr) && mFlags == pe.mFlags;
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b) {
        if (hasExclude()) {
            b.append('~');
        }

        if (hasOrderBy()) {
            b.append(hasDescending() ? '-' : '+');
            if (hasNullLow()) {
                b.append('!');
            }
        }

        if (mExpr instanceof AssignExpr) {
            mExpr.appendTo(b);
        } else {
            b.append(RowMethodsMaker.unescape(name()));
        }
    }
}
