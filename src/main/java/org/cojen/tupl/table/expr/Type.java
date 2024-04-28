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

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ConvertUtils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class Type extends ColumnInfo
    permits AnyType, NullType, BasicType, TupleType, RelationType
{
    /**
     * @param typeCode see ColumnInfo
     */
    protected Type(Class clazz, int typeCode) {
        this.type = clazz;
        this.typeCode = typeCode;
    }

    public final Class<?> clazz() {
        return type;
    }

    public final int typeCode() {
        return typeCode;
    }

    /**
     * Returns a Type instance which is nullable.
     */
    public abstract Type nullable();

    /**
     * Returns a cache key instance by calling encodeKey.
     */
    protected final TupleKey makeKey() {
        var enc = new KeyEncoder();
        encodeKey(enc);
        return enc.finish();
    }

    /**
     * @see Expr#encodeKey
     */
    protected abstract void encodeKey(KeyEncoder enc);

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    protected void appendTo(StringBuilder b) {
        b.append(toString());
    }

    protected final String defaultToString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }

    /**
     * Finds a common type which two expressions can be converted to without loss or abiguity.
     *
     * @param op defined in ColumnFilter; pass -1 if not performing a comparison operation
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public static Type commonType(Expr left, Expr right, int op) {
        Type leftType = left.type();
        Type rightType = right.type();
        if (left.isNullable() || right.isNullable()) {
            leftType = leftType.nullable();
            rightType = rightType.nullable();
        }
        return leftType.commonType(rightType, op);
    }

    /**
     * Finds a common type which can be converted to without loss or abiguity.
     *
     * @param op defined in ColumnFilter; pass -1 if not performing a comparison operation
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public Type commonType(Expr expr, int op) {
        return (expr.isNullable() ? nullable() : this).commonType(expr.type(), op);
    }

    /**
     * Finds a common type which can be converted to without loss or abiguity.
     *
     * @param op defined in ColumnFilter; pass -1 if not performing a comparison operation
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public Type commonType(Type type, int op) {
        if (type == AnyType.THE) {
            return type;
        }
        if (type == NullType.THE) {
            return this.nullable();
        }

        // Try finding a common type using a widening conversion.

        ColumnInfo common = ConvertUtils.commonType(this, type, op);

        if (common == this) {
            return this;
        } else if (common == type) {
            return type;
        }

        return common == null ? null : BasicType.make(common);
    }
}
