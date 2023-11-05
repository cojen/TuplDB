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

import java.util.ArrayList;
import java.util.List;

/**
 * Describes the name and type of a tuple column.
 *
 * @author Brian S. O'Neill
 * @see TupleType
 */
public final class Column {
    private Type mType;
    private String mName;
    private boolean mKey;

    private List<String> mSubNames;

    /**
     * @param key is true of column is part of a primary key
     */
    public static Column make(Type type, String name, boolean key) {
        return new Column(type, name, key);
    }

    private Column(Type type, String name, boolean key) {
        mType = type;
        mName = name;
        mKey = key;
    }

    public Type type() {
        return mType;
    }

    public String name() {
        return mName;
    }

    public boolean key() {
        return mKey;
    }

    /**
     * Returns the name split by '.' characters.
     *
     * @return a list whose size is at least one
     */
    public List<String> subNames() {
        List<String> subNames = mSubNames;
        if (subNames == null) {
            mSubNames = subNames = buildSubNames();
        }
        return subNames;
    }

    private List<String> buildSubNames() {
        String name = mName;
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

    public Column withName(String newName) {
        return newName.equals(mName) ? this : new Column(mType, newName, mKey);
    }

    @Override
    public int hashCode() {
        int hash = mType.hashCode();
        hash = hash * 31 + mName.hashCode();
        hash = hash * 31 + Boolean.hashCode(mKey);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Column c
            && mType.equals(c.mType) && mName.equals(c.mName) && mKey == c.mKey;
    }

    @Override
    public String toString() {
        return '{' + "type=" + mType + ", " + "name=" + mName + ", " + "key=" + mKey + '}';
    }
}
