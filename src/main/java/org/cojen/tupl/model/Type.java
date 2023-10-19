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

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract class Type {
    protected final Class mClazz;
    protected final int mTypeCode;

    /**
     * @param typeCode see ColumnInfo
     */
    protected Type(Class clazz, int typeCode) {
        mClazz = clazz;
        mTypeCode = typeCode;
    }

    public final Class<?> clazz() {
        return mClazz;
    }

    public final int typeCode() {
        return mTypeCode;
    }

    /**
     * Returns a new unnamed ColumnInfo instance.
     */
    public final ColumnInfo asColumnInfo() {
        var info = new ColumnInfo();
        info.type = mClazz;
        info.typeCode = mTypeCode;
        return info;
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
