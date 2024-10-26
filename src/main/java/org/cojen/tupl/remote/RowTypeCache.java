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

package org.cojen.tupl.remote;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.Row;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.MultiCache;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowStore;
import org.cojen.tupl.table.Unpersisted;

/**
 * Cache of row type interfaces keyed by descriptor.
 *
 * @author Brian S. O'Neill
 */
final class RowTypeCache extends MultiCache<TupleKey, Class<?>, byte[], RuntimeException> {
    private static final RowTypeCache THE = new RowTypeCache();

    /**
     * @param descriptor see RowStore#primaryRowInfo
     */
    static Class<?> findPlain(byte[] descriptor) {
        return THE.cacheObtain(TYPE_1, TupleKey.make.with(descriptor), descriptor);
    }

    /**
     * @param descriptor see RowStore#primaryRowInfo
     */
    @SuppressWarnings("unchecked")
    static Class<Row> findRow(byte[] descriptor) {
        return (Class<Row>) THE.cacheObtain(TYPE_2, TupleKey.make.with(descriptor), descriptor);
    }

    private RowTypeCache() {
    }

    @Override
    protected Class<?> cacheNewValue(Type type, TupleKey key, byte[] descriptor) {
        ClassMaker cm = RowGen.beginClassMakerForRowType(getClass().getPackageName(), "Row");

        cm.addAnnotation(Unpersisted.class, true);

        if (type == TYPE_2) {
            cm.implement(Row.class);
        }

        return RowStore.primaryRowInfo(cm.name(), descriptor).makeRowType(cm);
    }
}
