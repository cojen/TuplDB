/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.function.BiFunction;
import java.util.function.IntFunction;

/**
 * Simple cache of weakly referenced values. The keys must not strongly reference the values,
 * or else they won't get GC'd.
 *
 * @author Brian S O'Neill
 */
class WeakCache<K, V> extends ReferenceQueue<Object> {
    private Entry<K, V>[] mEntries;
    private int mSize;

    @SuppressWarnings({"unchecked"})
    public WeakCache() {
        // Initial capacity must be a power of 2.
        mEntries = new Entry[2];
    }

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    public V get(K key) {
        WeakReference<V> ref = getRef(key);
        return ref == null ? null : ref.get();
    }

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    public WeakReference<V> getRef(K key) {
        Object obj = poll();
        if (obj != null) {
            synchronized (this) {
                cleanup(obj);
            }
        }

        var entries = mEntries;
        for (var e = entries[key.hashCode() & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.mKey.equals(key)) {
                return e;
            }
        }

        return null;
    }

    /**
     * @return a new weak reference to the value
     */
    @SuppressWarnings({"unchecked"})
    public synchronized WeakReference<V> put(K key, V value) {
        Object obj = poll();
        if (obj != null) {
            cleanup(obj);
        }

        var entries = mEntries;
        int hash = key.hashCode();
        int index = hash & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.mKey.equals(key)) {
                V replaced = e.get();
                e.clear();
                var newEntry = new Entry<K, V>(key, value, hash, this);
                if (prev == null) {
                    newEntry.mNext = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                }
                VarHandle.storeStoreFence(); // ensure that entry value is safely visible
                entries[index] = newEntry;
                return newEntry;
            } else {
                prev = e;
            }
        }

        if (mSize >= mEntries.length) {
            // Rehash.
            var newEntries = new Entry[entries.length << 1];
            int size = 0;
            for (int i=entries.length; --i>=0 ;) {
                for (var existing = entries[i]; existing != null; ) {
                    var e = existing;
                    existing = existing.mNext;
                    if (e.get() != null) {
                        size++;
                        index = e.mHash & (newEntries.length - 1);
                        e.mNext = newEntries[index];
                        newEntries[index] = e;
                    }
                }
            }
            mEntries = entries = newEntries;
            mSize = size;
            index = hash & (entries.length - 1);
        }

        var newEntry = new Entry<K, V>(key, value, hash, this);
        newEntry.mNext = entries[index];
        VarHandle.storeStoreFence(); // ensure that entry value is safely visible
        entries[index] = newEntry;
        mSize++;

        return newEntry;
    }

    @SuppressWarnings({"unchecked"})
    synchronized K[] copyKeys(IntFunction<K[]> generator) {
        Object obj = poll();
        if (obj != null) {
            cleanup(obj);
        }

        K[] keys = generator.apply(mSize);

        var entries = mEntries;
        for (int i=0, k=0; i<entries.length; i++) {
            for (Entry<K, V> e = entries[i]; e != null; e = e.mNext) {
                keys[k++] = e.mKey;
            }
        }

        return keys;
    }

    /**
     * @return null if no values
     */
    List<V> copyValues() {
        return findValues(null, (list, value) -> {
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(value);
            return list;
        });
    }

    /**
     * @param collection passed to the function (can be anything or even null)
     * @param fun accepts a the collection and a value, and returns a collection
     * @return the updated collection
     */
    synchronized <C> C findValues(C collection, BiFunction<C, V, C> fun) {
        var entries = mEntries;
        for (int i=0, k=0; i<entries.length; i++) {
            for (Entry<K, V> e = entries[i]; e != null; e = e.mNext) {
                V value = e.get();
                if (value != null) {
                    collection = fun.apply(collection, value);
                }
            }
        }

        Object obj = poll();
        if (obj != null) {
            cleanup(obj);
        }

        return collection;
    }

    /**
     * Caller must be synchronized.
     *
     * @param obj not null
     */
    @SuppressWarnings({"unchecked"})
    private void cleanup(Object obj) {
        var entries = mEntries;
        do {
            var cleared = (Entry<K, V>) obj;
            int ix = cleared.mHash & (entries.length - 1);
            for (Entry<K, V> e = entries[ix], prev = null; e != null; e = e.mNext) {
                if (e == cleared) {
                    if (prev == null) {
                        entries[ix] = e.mNext;
                    } else {
                        prev.mNext = e.mNext;
                    }
                    mSize--;
                    break;
                } else {
                    prev = e;
                }
            }
        } while ((obj = poll()) != null);
    }

    private static final class Entry<K, V> extends WeakReference<V> {
        final K mKey;
        final int mHash;

        Entry<K, V> mNext;

        Entry(K key, V value, int hash, WeakCache<K, V> cache) {
            super(value, cache);
            mKey = key;
            mHash = hash;
        }
    }
}
