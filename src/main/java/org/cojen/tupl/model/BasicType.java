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

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class BasicType extends Type {
    public static final BasicType
        BOOLEAN = new BasicType(boolean.class, TYPE_BOOLEAN);

    public static BasicType make(ColumnInfo info) {
        return make(info.type, info.typeCode);
    }

    public static BasicType make(Class clazz, int typeCode) {
        if (clazz == boolean.class && !isNullable(typeCode)) {
            return BOOLEAN;
        }
        // FIXME: Use more singleton types, lazily initialized. Or use Canonicalizer.
        return new BasicType(clazz, typeCode);
    }

    private BasicType(Class clazz, int typeCode) {
        super(clazz, typeCode);
    }

    @Override
    public BasicType nullable() {
        if (isNullable()) {
            return this;
        }

        Class clazz = clazz();
        if (clazz.isPrimitive() && unboxedType() == clazz) {
            clazz = boxedType();
        }

        return make(clazz, typeCode | TYPE_NULLABLE);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeInt(typeCode);
        }
    }

    @Override
    public int hashCode() {
        return clazz().hashCode() * 31 + typeCode;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BasicType bt
            && clazz() == bt.clazz() && typeCode == bt.typeCode;
    }

    @Override
    public String toString() {
        String str = clazz().getCanonicalName();
        if (isUnsigned() && this != BOOLEAN) {
            str = "unsigned " + str;
        }
        return str;
    }
}
