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

package org.cojen.tupl.remote;

import java.util.function.Function;

import org.cojen.tupl.table.WeakCache;

/**
 * Cache of sharable client-side remote object wrappers.
 *
 * @author Brian S O'Neill
 */
final class ClientCache extends WeakCache<Object, Object, Function<Object, Object>> {
    private static final ClientCache THE = new ClientCache();

    /**
     * Get or create an object of type C, found by key K. The given factory is called to create
     * an instance of C if necessary.
     */
    @SuppressWarnings("unchecked")
    public static <K, C> C get(K key, Function<K, C> factory) {
        return (C) THE.obtain(key, (Function) factory);
    }

    /**
     * Remove all references to the given client object.
     */
    public static void remove(Object clientObj) {
        THE.removeValues(c -> c == clientObj);
    }

    private ClientCache() {
    }

    @Override
    public Object newValue(Object key, Function<Object, Object> factory) {
        return factory.apply(key);
    }
}
