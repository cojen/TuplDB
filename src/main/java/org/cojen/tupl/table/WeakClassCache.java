/*
 *  Copyright (C) 2022 Cojen.org
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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Simple cache of weakly referenced Class keys to weakly referenced values.
 *
 * @author Brian S O'Neill
 */
public class WeakClassCache<V> extends WeakCache<Class<?>, V, Object> {
    public WeakClassCache() {
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected Entry newEntry(Class<?> key, V value, int hash) {
        return new ClassEntry<>(key, value, hash, this);
    }

    private static final class ClassEntry<V> extends Entry<Object, V> {
        private ClassEntry(Class<?> key, V value, int hash, ReferenceQueue<Object> queue) {
            super(new WeakReference<>(key), value, hash, queue);
        }

        @Override
        protected boolean matches(Object key) {
            return ((WeakReference) mKey).get() == key;
        }
    }
}
