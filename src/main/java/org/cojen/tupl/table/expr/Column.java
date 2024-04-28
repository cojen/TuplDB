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
    private final String mName, mFieldName;

    private List<String> mSubNames, mSubFieldNames;

    /**
     * @param name visible column name (can be fully qualified)
     * @param fieldName actual name used by the row class (can be fully qualified)
     */
    public static Column make(Type type, String name, String fieldName, boolean hidden) {
        return hidden ? new Hidden(type, name, fieldName) : new Column(type, name, fieldName);
    }

    private Column(Type type, String name, String fieldName) {
        mType = type;
        mName = name;
        mFieldName = name;
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

    public String fieldName() {
        return mFieldName;
    }

    /**
     * Returns the name split by '.' characters.
     *
     * @return a list whose size is at least one
     */
    public List<String> subNames() {
        List<String> subNames = mSubNames;

        if (subNames == null) {
            if (mName.equals(mFieldName) && mSubFieldNames != null) {
                subNames = mSubFieldNames;
            } else {
                subNames = buildSubNames(mName);
            }
            mSubNames = subNames;
        }

        return subNames;
    }

    /**
     * Returns the field name split by '.' characters.
     *
     * @return a list whose size is at least one
     */
    public List<String> subFieldNames() {
        List<String> subFieldNames = mSubFieldNames;

        if (subFieldNames == null) {
            if (mFieldName.equals(mName) && mSubNames != null) {
                subFieldNames = mSubNames;
            } else {
                subFieldNames = buildSubNames(mFieldName);
            }
            mSubFieldNames = subFieldNames;
        }

        return subFieldNames;
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

    public Column withName(String name, String fieldName) {
        return name.equals(mName) && fieldName.equals(mFieldName) ? this
            : new Column(mType, name, fieldName);
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
        enc.encodeString(mFieldName);
    }

    @Override
    public int hashCode() {
        int hash = mType.hashCode();
        hash = hash * 31 + mName.hashCode();
        hash = hash * 31 + mFieldName.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof Column c
            && mType.equals(c.mType)
            && mName.equals(c.mName)
            && mFieldName.equals(c.mFieldName)
            && isHidden() == c.isHidden();
    }

    @Override
    public String toString() {
        return '{' + "type=" + mType + ", " + "name=" + mName + ", " +
            "fieldName=" + mFieldName + '}';
    }

    private static final class Hidden extends Column {
        private Hidden(Type type, String name, String fieldName) {
            super(type, name, fieldName);
        }

        @Override
        public boolean isHidden() {
            return true;
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
