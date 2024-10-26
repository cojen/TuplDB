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

package org.cojen.tupl.table;

import java.lang.invoke.VarHandle;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

import java.util.function.Consumer;

import org.cojen.tupl.util.Latch;

/**
 * A cache of softly referenced values, referenced by type-key pairs.
 *
 * @author Brian S. O'Neill
 */
public abstract class MultiCache<K, V, H, X extends Throwable> extends ReferenceQueue<Object> {
    public static sealed abstract class Type {
        abstract int hash(int hash);

        abstract <K> boolean matches(Entry<K, ?> entry, K key);

        abstract <K, V> Entry<K, V> newEntry(K key, V value, int hash,
                                             ReferenceQueue<Object> queue);
    }

    /** Cache key type typically used by Table.query implementations. */
    public static final Type TYPE_1 = new Type1();

    /** Cache key type typically used by Table.derive implementations. */
    public static final Type TYPE_2 = new Type2();

    public static final Type TYPE_3 = new Type3();

    public static final Type TYPE_4 = new Type4();

    private Entry<K, V>[] mEntries;
    private int mSize;

    @SuppressWarnings({"unchecked"})
    public MultiCache() {
        // Initial capacity must be a power of 2.
        mEntries = new Entry[2];
    }

    @SuppressWarnings({"unchecked"})
    public final synchronized void cacheClear() {
        if (mSize != 0 || mEntries.length != 2) {
            mEntries = new Entry[2];
            mSize = 0;
        }
    }

    /**
     * Clears the cache and then calls the consumer for each value that was in the cache.
     */
    public final void cacheClear(Consumer<V> c) {
        Entry<K, V>[] entries;
        int size;

        synchronized (this) {
            entries = mEntries;
            size = mSize;
            cacheClear();
        }

        if (size > 0) {
            cacheTraverse(entries, c);
        }
    }

    /**
     * Traverse all values while synchronized.
     */
    public final synchronized void cacheTraverse(Consumer<V> c) {
        if (mSize > 0) {
            cacheTraverse(mEntries, c);
        }
    }

    private static <K, V> void cacheTraverse(Entry<K, V>[] entries, Consumer<V> c) {
        for (int i=0; i<entries.length; i++) {
            for (var e = entries[i]; e != null; e = e.mNext) {
                V value = e.get();
                if (value != null) {
                    c.accept(value);
                }
            }
        }
    }

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    public final V cacheGet(Type type, K key) {
        Object ref = super.poll();
        if (ref != null) {
            synchronized (this) {
                cleanup(ref);
            }
        }

        var entries = mEntries;
        int hash = type.hash(key.hashCode());
        int index = hash & (entries.length - 1);

        for (var e = entries[index]; e != null; e = e.mNext) {
            if (type.matches(e, key)) {
                return e.get();
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public final V cacheObtain(Type type, K key, H helper) throws X {
        LatchType latchType;
        Latch latch;

        while (true) {
            V value = cacheGet(type, key);
            if (value != null) {
                return value;
            }

            synchronized (this) {
                value = cacheGet(type, key);
                if (value != null) {
                    return value;
                }

                latchType = new LatchType(type);
                latch = (Latch) cacheGet(latchType, key);

                if (latch == null) {
                    latch = new Latch(Latch.EXCLUSIVE);
                    cachePut(latchType, key, (V) latch);
                    // Break out of the loop and do the work.
                    break;
                }
            }

            // Wait for another thread to do the work and try again.
            latch.acquireShared();
        }
        
        V value;
        Throwable ex = null;

        try {
            value = cacheNewValue(type, key, helper);
        } catch (Throwable e) {
            value = null;
            ex = e;
        }

        synchronized (this) {
            if (value != null) {
                try {
                    cachePut(type, key, value);
                } catch (Throwable e) {
                    if (ex == null) {
                        ex = e;
                    } else {
                        ex.addSuppressed(e);
                    }
                }
            }

            cacheRemove(latchType, key);
            latch.releaseExclusive();
        }

        if (ex != null) {
            throw RowUtils.rethrow(ex);
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    public synchronized final void cachePut(Type type, K key, V value) {
        Object ref = super.poll();
        if (ref != null) {
            cleanup(ref);
        }

        var entries = mEntries;
        int hash = type.hash(key.hashCode());
        int index = hash & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (type.matches(e, key)) {
                e.clear();
                var newEntry = type.newEntry(key, value, hash, this);
                if (prev == null) {
                    newEntry.mNext = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                    newEntry.mNext = entries[index];
                }
                VarHandle.storeStoreFence(); // ensure that entry value is safely visible
                entries[index] = newEntry;
                return;
            } else {
                prev = e;
            }
        }

        if (mSize >= mEntries.length) {
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

        var newEntry = type.newEntry(key, value, hash, this);
        newEntry.mNext = entries[index];
        VarHandle.storeStoreFence(); // ensure that entry value is safely visible
        entries[index] = newEntry;
        mSize++;
    }

    public synchronized final void cacheRemove(Type type, K key) {
        var entries = mEntries;
        int hash = type.hash(key.hashCode());
        int index = hash & (entries.length - 1);

        for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
            if (type.matches(e, key)) {
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

        Object ref = super.poll();
        if (ref != null) {
            cleanup(ref);
        }
    }

    /**
     * @return non-null value
     */
    protected abstract V cacheNewValue(Type type, K key, H helper) throws X;

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
        } while ((ref = super.poll()) != null);
    }

    @Override
    public final Reference<Object> poll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Reference<Object> remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Reference<Object> remove(long timeout) {
        throw new UnsupportedOperationException();
    }

    private static sealed abstract class Entry<K, V> extends SoftReference<V> {
        final K mKey;
        final int mHash;

        Entry<K, V> mNext;

        Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(value, queue);
            mKey = key;
            mHash = hash;
        }
    }

    private static final class LatchType extends Type {
        private final Type mObtainType;

        LatchType(Type obtainType) {
            mObtainType = obtainType;
        }

        @Override
        int hash(int hash) {
            return mObtainType.hash(hash) * 565696537;
        }

        @Override
        <K> boolean matches(Entry<K, ?> entry, K key) {
            return entry instanceof LatchEntry le
                && le.mObtainType.equals(mObtainType) && le.mKey.equals(key);
        }

        @Override
        <K, V> Entry<K, V> newEntry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            return new LatchEntry<>(key, value, hash, queue, mObtainType);
        }
    }

    private static final class LatchEntry<K, V> extends Entry<K, V> {
        final Type mObtainType;

        LatchEntry(K key, V value, int hash, ReferenceQueue<Object> queue, Type obtainType) {
            super(key, value, hash, queue);
            mObtainType = obtainType;
        }
    }

    private static final class Type1 extends Type {
        @Override
        int hash(int hash) {
            return hash * 219380501;
        }

        @Override
        <K> boolean matches(Entry<K, ?> entry, K key) {
            return entry instanceof Type1Entry && entry.mKey.equals(key);
        }

        @Override
        <K, V> Entry<K, V> newEntry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            return new Type1Entry<>(key, value, hash, queue);
        }
    }

    private static final class Type1Entry<K, V> extends Entry<K, V> {
        Type1Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(key, value, hash, queue);
        }
    }

    private static final class Type2 extends Type {
        @Override
        int hash(int hash) {
            return hash * 785391669;
        }

        @Override
        <K> boolean matches(Entry<K, ?> entry, K key) {
            return entry instanceof Type2Entry && entry.mKey.equals(key);
        }

        @Override
        <K, V> Entry<K, V> newEntry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            return new Type2Entry<>(key, value, hash, queue);
        }
    }

    private static final class Type2Entry<K, V> extends Entry<K, V> {
        Type2Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(key, value, hash, queue);
        }
    }

    private static final class Type3 extends Type {
        @Override
        int hash(int hash) {
            return hash * 1702561261;
        }

        @Override
        <K> boolean matches(Entry<K, ?> entry, K key) {
            return entry instanceof Type3Entry && entry.mKey.equals(key);
        }

        @Override
        <K, V> Entry<K, V> newEntry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            return new Type3Entry<>(key, value, hash, queue);
        }
    }

    private static final class Type3Entry<K, V> extends Entry<K, V> {
        Type3Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(key, value, hash, queue);
        }
    }

    private static final class Type4 extends Type {
        @Override
        int hash(int hash) {
            return hash * 1104993829;
        }

        @Override
        <K> boolean matches(Entry<K, ?> entry, K key) {
            return entry instanceof Type4Entry && entry.mKey.equals(key);
        }

        @Override
        <K, V> Entry<K, V> newEntry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            return new Type4Entry<>(key, value, hash, queue);
        }
    }

    private static final class Type4Entry<K, V> extends Entry<K, V> {
        Type4Entry(K key, V value, int hash, ReferenceQueue<Object> queue) {
            super(key, value, hash, queue);
        }
    }
}
