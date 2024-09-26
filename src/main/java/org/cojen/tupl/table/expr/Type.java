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

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.ConvertUtils;

import org.cojen.tupl.table.filter.ColumnFilter;

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
     * Returns true if this type represents a boolean.
     */
    public boolean isBoolean() {
        return false;
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

    /**
     * @param simple when true, omit full class names
     */
    protected void appendTo(StringBuilder b, boolean simple) {
        b.append(this);
    }

    protected final String defaultToString() {
        var b = new StringBuilder();
        appendTo(b, false);
        return b.toString();
    }

    /**
     * Returns true if this type can be used to represent the given type, possibly requiring a
     * safe conversion.
     *
     * @param exact when true, the column types must exactly match
     */
    public final boolean canRepresent(Type type, boolean exact) {
        return this.equals(exact ? type : commonTypeLenient(type));
    }

    /**
     * Finds a common type which can be converted to without loss or ambiguity. The common type
     * might end up being a string, following lenient rules.
     *
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public final Type commonTypeLenient(Type type) {
        return commonType(type, ColumnFilter.OP_EQ);
    }

    /**
     * Finds a common type which can be converted to without loss or ambiguity. If one type is
     * numerical, the common type must also be numerical.
     *
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public final Type commonTypeStrict(Type type) {
        return commonType(type, -1);
    }

    /**
     * Finds a common type which can be converted to without loss or ambiguity.
     *
     * @param op defined in ColumnFilter; pass -1 if not performing a comparison operation and
     * the common type must obey strict rules
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public Type commonType(Type type, int op) {
        if (type == AnyType.THE || type == NullType.THE) {
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
