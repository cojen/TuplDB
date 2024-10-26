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

import org.cojen.tupl.table.RowMethodsMaker;

/**
 * Describes the name and type of a tuple column.
 *
 * @author Brian S. O'Neill
 * @see TupleType
 */
public final class Column implements Attr {
    private final Type mType;
    private final String mName;
    private final boolean mHidden;

    /**
     * @param name column name
     */
    public static Column make(Type type, String name, boolean hidden) {
        return new Column(type, name, hidden);
    }

    private Column(Type type, String name, boolean hidden) {
        mType = type;
        mName = name;
        mHidden = hidden;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public String name() {
        return mName;
    }

    public boolean isHidden() {
        return mHidden;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            doEncodeKey(enc);
        }
    }

    void doEncodeKey(KeyEncoder enc) {
        mType.encodeKey(enc);
        enc.encodeString(mName);
    }

    @Override
    public int hashCode() {
        int hash = mType.hashCode();
        hash = hash * 31 + mName.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof Column c
            && mType.equals(c.mType)
            && mName.equals(c.mName)
            && mHidden == c.mHidden;
    }

    @Override
    public String toString() {
        return '{' + "type=" + mType + ", " + "name=" + RowMethodsMaker.unescape(mName) + '}';
    }
}
