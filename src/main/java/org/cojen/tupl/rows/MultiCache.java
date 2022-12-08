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

package org.cojen.tupl.rows;

import java.util.function.IntFunction;

/**
 * A cache of numerically indexed caches.
 *
 * @author Brian S O'Neill
 */
final class MultiCache<K, V, H> {
    /**
     * @param typeMap maps requested types to effective types
     * @param factory called to make missing entries on demand
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V, H> MultiCache<K, V, H> newSoftCache(int[] typeMap,
                                                             Factory<K, V, H> factory)
    {
        return new MultiCache(typeMap, type -> new SoftCache<K, V, H>() {
            @Override
            protected V newValue(K key, H helper) {
                return factory.newValue(type, key, helper);
            }
        });
    }

    /**
     * @param typeMap maps requested types to effective types
     * @param factory called to make missing entries on demand
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V, H> MultiCache<K, V, H> newWeakCache(int[] typeMap,
                                                             Factory<K, V, H> factory)
    {
        return new MultiCache(typeMap, type -> new WeakCache<K, V, H>() {
            @Override
            protected V newValue(K key, H helper) {
                return factory.newValue(type, key, helper);
            }
        });
    }

    private final RefCache<K, V, H>[] mCaches;

    @SuppressWarnings({"unchecked"})
    private MultiCache(int[] typeMap, IntFunction<RefCache<K, V, H>> f) {
        var caches = new RefCache[typeMap.length];

        for (int i=0; i<caches.length; i++) {
            int type = typeMap[i];
            if (type == i) {
                caches[i] = f.apply(type);
            }
        }

        for (int i=0; i<caches.length; i++) {
            int type = typeMap[i];
            if (type != i) {
                if ((caches[i] = caches[type]) == null) {
                    throw new IllegalArgumentException();
                }
            }
        }

        mCaches = caches;
    }

    public void clear() {
        for (var cache : mCaches) {
            cache.clear();
        }
    }

    /**
     * Generates missing entries on demand, using an entry latch to ensure that only one thread
     * does the work.
     */
    public V obtain(int type, K key, H helper) {
        return mCaches[type].obtain(key, helper);
    }

    @FunctionalInterface
    public static interface Factory<K, V, H> {
        V newValue(int type, K key, H helper);
    }
}
