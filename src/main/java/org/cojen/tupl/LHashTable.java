/*
 *  Copyright 2011-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

/**
 * Simple hash table which maps long keys to customizable entries. The hash function only
 * examines the lowest bits of the keys, and so the keys might need to be scrambled to reduce
 * collisions.
 *
 * @author Brian S O'Neill
 */
abstract class LHashTable<E extends LHashTable.Entry<E>> {
    public static final class ObjEntry<V> extends Entry<ObjEntry<V>> {
        public V value;
    }

    public static final class Obj<V> extends LHashTable<ObjEntry<V>> {
        Obj(int capacity) {
            super(capacity);
        }

        public V getValue(long key) {
            ObjEntry<V> entry = get(key);
            return entry == null ? null : entry.value;
        }

        public V removeValue(long key) {
            ObjEntry<V> entry = remove(key);
            return entry == null ? null : entry.value;
        }

        protected ObjEntry<V> newEntry() {
            return new ObjEntry<>();
        }
    }

    public static final class IntEntry extends Entry<IntEntry> {
        public int value;
    }

    public static final class Int extends LHashTable<IntEntry> {
        Int(int capacity) {
            super(capacity);
        }

        protected IntEntry newEntry() {
            return new IntEntry();
        }
    }

    private static final float LOAD_FACTOR = 0.75f;

    private E[] mEntries;
    private int mSize;
    private int mGrowThreshold;

    /**
     * @param capacity initial capacity
     */
    LHashTable(int capacity) {
        clear(capacity);
    }

    public final int size() {
        return mSize;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public final void clear(int capacity) {
        if (capacity <= 0) {
            capacity = 1;
        }
        capacity = Utils.roundUpPower2(capacity);
        E[] entries = mEntries;
        if (entries != null && entries.length == capacity) {
            java.util.Arrays.fill(entries, null);
        } else {
            mEntries = (E[]) new Entry[capacity];
            mGrowThreshold = (int) (capacity * LOAD_FACTOR);
        }
        mSize = 0;
    }

    /**
     * @return null if entry not found
     */
    public final E get(long key) {
        E[] entries = mEntries;
        for (E e = entries[((int) key) & (entries.length - 1)]; e != null; e = e.next) {
            if (e.key == key) {
                return e;
            }
        }
        return null;
    }

    /**
     * @return new entry if inserted, existing entry otherwise
     */
    public final E insert(long key) {
        E[] entries = mEntries;
        int index = ((int) key) & (entries.length - 1);
        for (E e = entries[index]; e != null; e = e.next) {
            if (e.key == key) {
                return e;
            }
        }
        if (grow()) {
            entries = mEntries;
            index = ((int) key) & (entries.length - 1);
        }
        mSize++;
        return entries[index] = newEntry(key, entries[index]);
    }

    /**
     * @return new entry
     */
    public final E replace(long key) {
        E[] entries = mEntries;
        int index = ((int) key) & (entries.length - 1);
        for (E e = entries[index], prev = null; e != null; e = e.next) {
            if (e.key == key) {
                if (prev == null) {
                    entries[index] = e.next;
                } else {
                    prev.next = e.next;
                }
                return entries[index] = newEntry(key, entries[index]);
            } else {
                prev = e;
            }
        }
        if (grow()) {
            entries = mEntries;
            index = ((int) key) & (entries.length - 1);
        }
        mSize++;
        return entries[index] = newEntry(key, entries[index]);
    }

    /**
     * @return null if entry not found
     */
    public final E remove(long key) {
        E[] entries = mEntries;
        int index = ((int) key) & (entries.length - 1);
        for (E e = entries[index], prev = null; e != null; e = e.next) {
            if (e.key == key) {
                if (prev == null) {
                    entries[index] = e.next;
                } else {
                    prev.next = e.next;
                }
                mSize--;
                return e;
            } else {
                prev = e;
            }
        }
        return null;
    }

    public <X extends Exception> void traverse(Visitor<E, X> v) throws X {
        E[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (E e = entries[i], prev = null; e != null; ) {
                E next = e.next;
                if (v.visit(e)) {
                    if (prev == null) {
                        entries[i] = next;
                    } else {
                        prev.next = next;
                    }
                    mSize--;
                } else {
                    prev = e;
                }
                e = next;
            }
        }
    }

    protected abstract E newEntry();

    private E newEntry(long key, E next) {
        E e = newEntry();
        e.key = key;
        e.next = next;
        return e;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean grow() {
        if (mSize < mGrowThreshold) {
            return false;
        }

        E[] entries = mEntries;

        int capacity = entries.length << 1;
        if (capacity == 0) {
            capacity = 1;
        }
        E[] newEntries = (E[]) new Entry[capacity];
        int newMask = capacity - 1;

        for (int i=entries.length; --i>=0 ;) {
            for (E e = entries[i]; e != null; ) {
                E next = e.next;
                int ix = ((int) e.key) & newMask;
                e.next = newEntries[ix];
                newEntries[ix] = e;
                e = next;
            }
        }

        mEntries = newEntries;
        mGrowThreshold = (int) (capacity * LOAD_FACTOR);

        return true;
    }

    public static class Entry<E extends Entry<E>> {
        public long key;
        E next;
    }

    @FunctionalInterface
    public static interface Visitor<E extends Entry<E>, X extends Exception> {
        /**
         * @return true if entry should be deleted
         */
        boolean visit(E e) throws X;
    }
}
