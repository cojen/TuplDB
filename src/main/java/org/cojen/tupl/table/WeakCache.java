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

package org.cojen.tupl.table;

import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;

import java.util.ArrayList;
import java.util.List;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.cojen.tupl.util.Latch;

/**
 * Simple cache of weakly referenced values. The keys must not strongly reference the values,
 * or else they won't get GC'd.
 *
 * @author Brian S O'Neill
 * @see WeakClassCache
 * @see SoftCache
 */
public class WeakCache<K, V, H> extends RefCache<K, V, H> {
    private Entry<K, V>[] mEntries;
    private int mSize;

    @SuppressWarnings({"unchecked"})
    public WeakCache() {
        // Initial capacity must be a power of 2.
        mEntries = new Entry[2];
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public synchronized void clear() {
        if (mSize != 0 || mEntries.length != 2) {
            mEntries = new Entry[2];
            mSize = 0;
        }
    }

    @Override
    public void clear(Consumer<V> c) {
        Entry<K, V>[] entries;
        int size;

        synchronized (this) {
            entries = mEntries;
            size = mSize;
            clear();
        }

        if (size > 0) {
            traverse(entries, c);
        }
    }

    @Override
    public synchronized void traverse(Consumer<V> c) {
        if (mSize > 0) {
            traverse(mEntries, c);
        }
    }

    private static <K, V> void traverse(Entry<K, V>[] entries, Consumer<V> c) {
        for (int i=0; i<entries.length; i++) {
            for (var e = entries[i]; e != null; e = e.mNext) {
                V value = e.get();
                if (value != null && !(value instanceof Latch)) {
                    c.accept(value);
                }
            }
        }
    }

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    @Override
    public WeakReference<V> getRef(K key) {
        Object ref = poll();
        if (ref != null) {
            synchronized (this) {
                cleanup(ref);
            }
        }

        var entries = mEntries;
        for (var e = entries[key.hashCode() & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.matches(key)) {
                return e;
            }
        }

        return null;
    }

    /**
     * @return a new weak reference to the value
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public synchronized WeakReference<V> put(K key, V value) {
        Object ref = poll();
        if (ref != null) {
            cleanup(ref);
        }

        var entries = mEntries;
        int hash = key.hashCode();
        int index = hash & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.matches(key)) {
                e.clear();
                var newEntry = newEntry(key, value, hash);
                if (prev == null) {
                    newEntry.mNext = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                    newEntry.mNext = entries[index];
                }
                VarHandle.storeStoreFence(); // ensure that entry value is safely visible
                entries[index] = newEntry;
                return newEntry;
            } else {
                prev = e;
            }
        }

        if (mSize >= entries.length) {
            // Rehash.
            var newEntries = new Entry[entries.length << 1];
            int size = 0;
            for (int i=0; i<entries.length; i++) {
                for (var existing = entries[i]; existing != null; ) {
                    var e = existing;
                    existing = existing.mNext;
                    if (!e.refersTo(null)) {
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

        var newEntry = newEntry(key, value, hash);
        newEntry.mNext = entries[index];
        VarHandle.storeStoreFence(); // ensure that entry value is safely visible
        entries[index] = newEntry;
        mSize++;

        return newEntry;
    }

    @Override
    public synchronized void removeKey(K key) {
        var entries = mEntries;
        int index = key.hashCode() & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.matches(key)) {
                e.clear();
                if (prev == null) {
                    entries[index] = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                }
                mSize--;
                break;
            } else {
                prev = e;
            }
        }

        Object ref = poll();
        if (ref != null) {
            cleanup(ref);
        }
    }

    /**
     * @return true if cache is now empty
     */
    public synchronized boolean removeValues(Predicate<V> p) {
        var entries = mEntries;

        for (int i=entries.length; --i>=0 ;) {
            for (Entry<K, V> e = entries[i], prev = null; e != null; e = e.mNext) {
                V value = e.get();
                if (value == null || p.test(value)) {
                    if (prev == null) {
                        entries[i] = e.mNext;
                    } else {
                        prev.mNext = e.mNext;
                    }
                    mSize--;
                } else {
                    prev = e;
                }
            }
        }

        return mSize == 0;
    }

    synchronized K[] copyKeys(IntFunction<K[]> generator) {
        Object ref = poll();
        if (ref != null) {
            cleanup(ref);
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
     * @param fun accepts a collection and a value, and returns a collection
     * @return the updated collection
     */
    synchronized <C> C findValues(C collection, BiFunction<C, V, C> fun) {
        var entries = mEntries;
        for (Entry<K, V> entry : entries) {
            for (Entry<K, V> e = entry; e != null; e = e.mNext) {
                V value = e.get();
                if (value != null) {
                    collection = fun.apply(collection, value);
                }
            }
        }

        Object ref = poll();
        if (ref != null) {
            cleanup(ref);
        }

        return collection;
    }

    /**
     * Caller must be synchronized.
     *
     * @param ref not null
     */
    @SuppressWarnings({"unchecked"})
    private void cleanup(Object ref) {
        var entries = mEntries;
        do {
            var cleared = (Entry<K, V>) ref;
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
        } while ((ref = poll()) != null);
    }

    protected Entry<K, V> newEntry(K key, V value, int hash) {
        return new Entry<>(key, value, hash, this);
    }

    protected static class Entry<K, V> extends WeakReference<V> {
        protected final K mKey;
        final int mHash;

        Entry<K, V> mNext;

        protected Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(value, queue);
            mKey = key;
            mHash = hash;
        }

        protected boolean matches(K key) {
            return mKey.equals(key);
        }
    }
}
