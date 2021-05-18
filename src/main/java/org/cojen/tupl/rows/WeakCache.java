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

/**
 * Simple cache of weakly referenced values. Not thread safe. I would really like it if a class
 * like this was actually provided by the JDK.
 *
 * Note: Entries can be safely retrieved without explicit synchronization, but they might
 * sometimes appear to go missing. Double check with synchronization.
 *
 * @author Brian S O'Neill
 */
public class WeakCache<K, V> extends ReferenceQueue<Object> {
    private Entry<K, V>[] mEntries;
    private int mSize;

    @SuppressWarnings({"unchecked"})
    public WeakCache() {
        // Initial capacity must be a power of 2.
        mEntries = new Entry[2];
    }

    public V get(K key) {
        var entries = mEntries;
        for (var e = entries[key.hashCode() & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.mKey.equals(key)) {
                return e.get();
            }
        }
        return null;
    }

    /**
     * @return replaced value, or null if none
     */
    @SuppressWarnings({"unchecked"})
    public V put(K key, V value) {
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
                return replaced;
            } else {
                prev = e;
            }
        }

        // Remove cleared entries.
        Object obj = poll();
        if (obj != null) {
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

        return null;
    }

    public void remove(K key) {
        var entries = mEntries;
        int index = key.hashCode() & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.mKey.equals(key)) {
                e.clear();
                if (prev == null) {
                    entries[index] = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                }
                mSize--;
                return;
            } else {
                prev = e;
            }
        }
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
