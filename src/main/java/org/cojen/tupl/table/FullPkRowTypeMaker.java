/*
 *  Copyright (C) 2025 Cojen.org
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

package org.cojen.tupl.table;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.PrimaryKey;

/**
 * Makes a row subtype whose primary key references all of its columns.
 *
 * @author Brian S. O'Neill
 */
@SuppressWarnings("unchecked")
public final class FullPkRowTypeMaker {
    private static final WeakCache<Class, Class, Object> cCache = new WeakCache<>() {
        @Override
        public Class newValue(Class rowType, Object unused) {
            return doMakeFor(rowType);
        }
    };

    /**
     * Returns a subtype or the original type, if the primary key already references all of the
     * columns.
     */
    public static <R> Class<R> makeFor(Class<R> rowType) {
        return cCache.obtain(rowType, null);
    }

    private static <R> Class<R> doMakeFor(Class<R> rowType) {
        RowInfo info = RowInfo.find(rowType);

        if (info.valueColumns.isEmpty()) {
            return rowType;
        }

        ClassMaker cm = RowGen.beginClassMakerForRowType
            (rowType.getPackageName(), rowType.getName());

        cm.public_().interface_().implement(rowType);

        cm.sourceFile(FullPkRowTypeMaker.class.getSimpleName());

        var pk = new String[info.allColumns.size()];

        int i = 0;
        for (ColumnInfo col : info.keyColumns.values()) {
            String name = col.name;
            if (col.isDescending()) {
                name = '-' + name;
            }
            pk[i++] = name;
        }

        for (String name : info.valueColumns.keySet()) {
            pk[i++] = name;
        }

        assert i == pk.length;

        cm.addAnnotation(PrimaryKey.class, true).put("value", pk);

        return (Class<R>) cm.finish();
    }
}
