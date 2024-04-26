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

import java.util.function.Supplier;

import org.cojen.tupl.table.WeakCache;

/**
 * Maintains a weak cache to objects which have dynamically generated classes.
 *
 * @author Brian S. O'Neill
 */
final class CodeCache {
    private static final WeakCache<Object, ?, Supplier<?>> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public Object newValue(Object key, Supplier<?> maker) {
                return maker.get();
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <C> C obtain(Object key, Supplier<C> maker) {
        return (C) cCache.obtain(key, maker);
    }
}
