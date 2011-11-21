/*
 *  Copyright 2011 Brian S O'Neill
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
 * Simple hash table which maps long keys to customizable entries.
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

        protected ObjEntry<V> newEntry() {
            return new ObjEntry<V>();
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
    private int mMask;
    private int mSize;
    private int mGrowThreshold;

    LHashTable(int capacity) {
        capacity = Utils.roundUpPower2(capacity);
        mEntries = (E[]) new Entry[capacity];
        mMask = capacity - 1;
        mGrowThreshold = (int) (capacity * LOAD_FACTOR);
    }

    public final int size() {
        return mSize;
    }

    /**
     * @return null if entry not found
     */
    public final E get(long key) {
        for (E e = mEntries[((int) key) & mMask]; e != null; e = e.next) {
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
        int index = ((int) key) & mMask;
        for (E e = entries[index]; e != null; e = e.next) {
            if (e.key == key) {
                return e;
            }
        }
        if (grow()) {
            entries = mEntries;
            index = ((int) key) & mMask;
        }
        mSize++;
        return entries[index] = newEntry(key, entries[index]);
    }

    /**
     * @return new entry
     */
    public final E replace(long key) {
        E[] entries = mEntries;
        int index = ((int) key) & mMask;
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
            index = ((int) key) & mMask;
        }
        mSize++;
        return entries[index] = newEntry(key, entries[index]);
    }

    /**
     * @return null if entry not found
     */
    public final E remove(long key) {
        E[] entries = mEntries;
        int index = ((int) key) & mMask;
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

    protected abstract E newEntry();

    private E newEntry(long key, E next) {
        E e = newEntry();
        e.key = key;
        e.next = next;
        return e;
    }

    private boolean grow() {
        if (mSize < mGrowThreshold) {
            return false;
        }

        E[] entries = mEntries;

        int capacity = entries.length << 1;
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

        mEntries = entries = newEntries;
        mMask = newMask;
        mGrowThreshold = (int) (capacity * LOAD_FACTOR);

        return true;
    }

    public static class Entry<E extends Entry<E>> {
        public long key;
        E next;
    }
}
