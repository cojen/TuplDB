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
 * Simple hash table which maps long keys to int values.
 *
 * @author Brian S O'Neill
 */
final class LIHashTable {
    private static final float LOAD_FACTOR = 0.75f;

    private Entry[] mEntries;
    private int mMask;
    private int mSize;
    private int mGrowThreshold;
    private int mRemoveAnyIndex;

    /**
     * @param exponent initial capacity exponent
     */
    LIHashTable(int exponent) {
        int capacity = 1 << exponent;
        mEntries = new Entry[capacity];
        mMask = capacity - 1;
        mGrowThreshold = (int) (capacity * LOAD_FACTOR);
    }

    /**
     * @param exponent initial capacity exponent
     */
    void reset(int exponent) {
        int capacity = 1 << exponent;
        if (mEntries.length == capacity) {
            java.util.Arrays.fill(mEntries, null);
        } else {
            mEntries = new Entry[capacity];
            mMask = capacity - 1;
            mGrowThreshold = (int) (capacity * LOAD_FACTOR);
        }
        mSize = 0;
        mRemoveAnyIndex = 0;
    }

    int size() {
        return mSize;
    }

    /**
     * @return null if entry not found
     */
    Entry get(long key) {
        for (Entry e = mEntries[((int) key) & mMask]; e != null; e = e.next) {
            if (e.key == key) {
                return e;
            }
        }
        return null;
    }

    /**
     * @return new entry if inserted, existing entry otherwise
     */
    Entry insert(long key) {
        Entry[] entries = mEntries;
        int index = ((int) key) & mMask;
        for (Entry e = entries[index]; e != null; e = e.next) {
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
    Entry replace(long key) {
        Entry[] entries = mEntries;
        int index = ((int) key) & mMask;
        for (Entry e = entries[index], prev = null; e != null; e = e.next) {
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
    Entry remove(long key) {
        Entry[] entries = mEntries;
        int index = ((int) key) & mMask;
        for (Entry e = entries[index], prev = null; e != null; e = e.next) {
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

    /**
     * @return null if no entries
     */
    Entry removeAny() {
        if (mSize == 0) {
            return null;
        }

        Entry[] entries = mEntries;
        int anyIndex = mRemoveAnyIndex;
        Entry e;

        while (true) {
            int index = anyIndex & mMask;
            e = entries[index];
            if (e == null) {
                anyIndex++;
            } else {
                if ((entries[index] = e.next) == null) {
                    anyIndex++;
                }
                mSize--;
                break;
            }
        }

        mRemoveAnyIndex = anyIndex;
        return e;
    }

    <E extends Exception> void traverse(Vistor<E> v) throws E {
        Entry[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (Entry e = entries[i]; e != null; e = e.next) {
                v.visit(e);
            }
        }
    }

    <E extends Exception> void clear(Vistor<E> v) throws E {
        Entry[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (Entry e = entries[i], prev = null; e != null; e = e.next) {
                if (prev == null) {
                    entries[i] = e.next;
                } else {
                    prev.next = e.next;
                }
                mSize--;
                // Visitor is always called after the entry has been removed.
                v.visit(e);
            }
        }
    }

    /**
     * @param e entry to recycle, which can be null
     * /
    static synchronized void recycle(Entry e) {
        if (e != null) {
            e.next = cFreeList;
            cFreeList = e;
        }
    }

    private static Entry cFreeList;

    private static synchronized Entry newEntry(long key, Entry next) {
        Entry e;
        synchronized (LIHashTable.class) {
            if ((e = cFreeList) != null) {
                cFreeList = e.next;
            }
        }
        if (e == null) {
            e = new Entry();
        }
        e.key = key;
        e.next = next;
        return e;
    }
    */

    private static Entry newEntry(long key, Entry next) {
        Entry e = new Entry();
        e.key = key;
        e.next = next;
        return e;
    }

    private boolean grow() {
        if (mSize < mGrowThreshold) {
            return false;
        }

        Entry[] entries = mEntries;

        int capacity = entries.length << 1;
        Entry[] newEntries = new Entry[capacity];
        int newMask = capacity - 1;

        for (int i=entries.length; --i>=0 ;) {
            for (Entry e = entries[i]; e != null; ) {
                Entry next = e.next;
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

    static interface Vistor<E extends Exception> {
        void visit(Entry e) throws E;
    }

    static final class Entry {
        long key;
        Entry next;
        int value;
    }
}
