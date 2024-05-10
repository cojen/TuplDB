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

import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class RelationType extends Type {
    public static RelationType make(TupleType type, Cardinality cardinality) {
        return new RelationType(type, cardinality);
    }

    private final TupleType mRowType;
    private final Cardinality mCardinality;

    private RelationType(TupleType rowType, Cardinality cardinality) {
        super(Table.class, TYPE_REFERENCE);
        mRowType = rowType;
        mCardinality = cardinality;
    }

    public TupleType rowType() {
        return mRowType;
    }

    public Cardinality cardinality() {
        return mCardinality;
    }

    @Override
    public RelationType nullable() {
        return isNullable() ? this : new RelationType(mRowType.nullable(), mCardinality);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mRowType.encodeKey(enc);
            int ordinal = mCardinality.ordinal();
            assert ordinal < 256;
            enc.encodeByte(ordinal);
        }
    }

    @Override
    public int hashCode() {
        return mRowType.hashCode() * 31 + mCardinality.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof RelationType rt
            && mRowType.equals(rt.mRowType) && mCardinality == rt.mCardinality;
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b) {
        b.append('(');
        mRowType.appendTo(b);
        b.append(", cardinality=").append(mCardinality).append(')');
    }
}