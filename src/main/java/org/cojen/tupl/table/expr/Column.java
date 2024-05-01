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

import java.util.ArrayList;
import java.util.List;

/**
 * Describes the name and type of a tuple column.
 *
 * @author Brian S. O'Neill
 * @see TupleType
 */
public sealed class Column implements Named {
    private final Type mType;
    private final String mName;

    private List<String> mSubNames;

    /**
     * @param name column name (can be fully qualified)
     */
    public static Column make(Type type, String name, boolean hidden) {
        return hidden ? new Hidden(type, name) : new Column(type, name);
    }

    private Column(Type type, String name) {
        mType = type;
        mName = name;
    }

    public Type type() {
        return mType;
    }

    public boolean isHidden() {
        return false;
    }

    @Override
    public String name() {
        return mName;
    }

    /**
     * Returns the name split by '.' characters.
     *
     * @return a list whose size is at least one
     */
    public List<String> subNames() {
        List<String> subNames = mSubNames;
        if (subNames == null) {
            mSubNames = subNames = buildSubNames(mName);
        }
        return subNames;
    }

    private static List<String> buildSubNames(String name) {
        int ix2 = name.indexOf('.');

        if (ix2 < 0) {
            return List.of(name);
        }

        int ix1 = 0;
        var list = new ArrayList<String>(2);

        while (true) {
            list.add(name.substring(ix1, ix2));
            ix1 = ix2 + 1;
            ix2 = name.indexOf('.', ix1);
            if (ix2 < 0) {
                list.add(name.substring(ix1));
                break;
            }
        }

        return list;
    }

    public Column withName(String name) {
        return name.equals(mName) ? this : new Column(mType, name);
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
            && isHidden() == c.isHidden();
    }

    @Override
    public String toString() {
        return '{' + "type=" + mType + ", " + "name=" + mName + '}';
    }

    private static final class Hidden extends Column {
        private Hidden(Type type, String name) {
            super(type, name);
        }

        @Override
        public boolean isHidden() {
            return true;
        }

        @Override
        public Hidden withName(String name) {
            return name.equals(name()) ? this : new Hidden(type(), name);
        }

        private static final byte K_TYPE = KeyEncoder.allocType();

        @Override
        void encodeKey(KeyEncoder enc) {
            if (enc.encode(this, K_TYPE)) {
                doEncodeKey(enc);
            }
        }
    }
}
