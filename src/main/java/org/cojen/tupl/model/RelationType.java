/*
 *  Copyright (C) 2023 Cojen.org
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

import org.cojen.tupl.Table;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class RelationType extends Type {
    public static RelationType make(TupleType type, Cardinality cardinality) {
        return new RelationType(type, cardinality);
    }

    private final TupleType mType;
    private final Cardinality mCardinality;

    private RelationType(TupleType type, Cardinality cardinality) {
        super(Table.class, type.typeCode());
        mType = type;
        mCardinality = cardinality;
    }

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + mCardinality.hashCode();
    }

    @Override
    public RelationType nullable() {
        return isNullable() ? this : new RelationType(mType.nullable(), mCardinality);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RelationType rt
            && mType.equals(rt.mType) && mCardinality == rt.mCardinality;
    }

    public TupleType tupleType() {
        return mType;
    }

    public Cardinality cardinality() {
        return mCardinality;
    }

    @Override
    public String toString() {
        return '(' + mType.toString() + ", cardinality=" + mCardinality + ')';
    }
}
