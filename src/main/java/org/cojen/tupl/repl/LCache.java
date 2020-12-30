/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

/**
 * Simple cache of objects which have a long key.
 *
 * @author Brian S O'Neill
 */
final class LCache<E extends LCache.Entry<E, C>, C> {
    // Hash spreader. Based on rounded value of 2 ** 63 * (sqrt(5) - 1) equivalent 
    // to unsigned 11400714819323198485.
    private static final long HASH_SPREAD = -7046029254386353131L;

    private int mMaxSize;
    private E[] mEntries;

    private int mSize;

    private E mMostRecentlyUsed;
    private E mLeastRecentlyUsed;

    @SuppressWarnings({"unchecked"})
    LCache(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException();
        }
        mMaxSize = maxSize;
        mEntries = (E[]) new Entry[roundUpPower2(maxSize)];
    }

    public synchronized int size() {
        return mSize;
    }

    /**
     * @param check passed to cacheCheck method
     * @return null if none found
     */
    public synchronized E remove(long key, C check) {
        final E[] entries = mEntries;
        final int slot = hash(key) & (entries.length - 1);

        for (E entry = entries[slot], prev = null; entry != null; ) {
            E next = entry.cacheNext();
            if (entry.cacheKey() != key || !entry.cacheCheck(check)) {
                prev = entry;
                entry = next;
                continue;
            }

            // Found one.

            if (prev == null) {
                entries[slot] = next;
            } else {
                prev.cacheNext(next);
            }
            entry.cacheNext(null);
            mSize--;

            // Remove from usage list.

            final E lessUsed = entry.cacheLessUsed();
            final E moreUsed = entry.cacheMoreUsed();

            if (lessUsed != null) {
                entry.cacheLessUsed(null);
                if (moreUsed != null) {
                    entry.cacheMoreUsed(null);
                    lessUsed.cacheMoreUsed(moreUsed);
                    moreUsed.cacheLessUsed(lessUsed);
                } else if (entry == mMostRecentlyUsed) {
                    mMostRecentlyUsed = lessUsed;
                    lessUsed.cacheMoreUsed(null);
                }
            } else if (entry == mLeastRecentlyUsed) {
                mLeastRecentlyUsed = moreUsed;
                if (moreUsed != null) {
                    entry.cacheMoreUsed(null);
                    moreUsed.cacheLessUsed(null);
                } else {
                    mMostRecentlyUsed = null;
                }
            }

            return entry;
        }

        return null;
    }

    /**
     * @return entry which was evicted (or null)
     */
    public synchronized E add(E entry) {
        final E[] entries = mEntries;
        final int slot = hash(entry.cacheKey()) & (entries.length - 1);

        final E first = entries[slot];
        for (E e = first; e != null; e = e.cacheNext()) {
            if (e.cacheKey() == entry.cacheKey()) {
                // A matching entry already exists, so don't replace it. Instead, evict the
                // given entry, unless it's the same instance.
                return e == entry ? null : entry;
            }
        }

        entry.cacheNext(first);
        entries[slot] = entry;

        final E most = mMostRecentlyUsed;
        if (most == null) {
            mLeastRecentlyUsed = entry;
        } else {
            entry.cacheLessUsed(most);
            most.cacheMoreUsed(entry);
        }
        mMostRecentlyUsed = entry;

        int size = mSize;
        if (size < mMaxSize) {
            mSize = size + 1;
            return null;
        }

        return evictOne();
    }

    // Caller must be synchronized.
    private E evictOne() {
        final E entry = mLeastRecentlyUsed;
        final E moreUsed = entry.cacheMoreUsed();
        mLeastRecentlyUsed = moreUsed;
        entry.cacheMoreUsed(null);
        moreUsed.cacheLessUsed(null);

        final E[] entries = mEntries;
        final int slot = hash(entry.cacheKey()) & (entries.length - 1);

        for (E e = entries[slot], prev = null; e != null; ) {
            E next = e.cacheNext();
            if (e == entry) {
                if (prev == null) {
                    entries[slot] = next;
                } else {
                    prev.cacheNext(next);
                }
                e.cacheNext(null);
                break;
            } else {
                prev = e;
                e = next;
            }
        }

        return entry;
    }

    /**
     * Increase or decrease the maximum size of the cache. If the effective size of the cache
     * is reduced, then an evicted entry is returned. The caller should continue calling this
     * method to evict more entries.
     */
    public synchronized E maxSize(int maxSize) {
        if (maxSize == mMaxSize) {
            return null;
        }

        if (maxSize < mMaxSize) {
            if (maxSize <= 0) {
                throw new IllegalArgumentException();
            }
            if ((mSize - maxSize) > 1) {
                mMaxSize--;
            } else {
                int newLen = roundUpPower2(maxSize);
                if (newLen < (mEntries.length >> 5)) { // don't aggressively shrink
                    newLen = mEntries.length >> 1;
                    rehash(newLen);
                }
                mMaxSize = maxSize;
                if (mSize <= maxSize) {
                    return null;
                }
            }
            mSize--;
            return evictOne();
        }

        int newLen = roundUpPower2(maxSize);
        if (newLen > mEntries.length) {
            rehash(newLen);
        }

        mMaxSize = maxSize;
        return null;
    }

    /**
     * @param newLen must be a power of 2
     */
    @SuppressWarnings({"unchecked"})
    private void rehash(int newLen) {
        final E[] newEntries = (E[]) new Entry[newLen];
        final E[] entries = mEntries;

        for (int i = entries.length; --i >= 0; ) {
            for (E e = entries[i]; e != null; ) {
                E next = e.cacheNext();
                int slot = hash(e.cacheKey()) & (newEntries.length - 1);
                e.cacheNext(newEntries[slot]);
                newEntries[slot] = e;
                e = next;
            }
        }

        mEntries = newEntries;
    }

    private static int roundUpPower2(int i) {
        return Math.max(1, Integer.highestOneBit(i - 1) << 1);
    }

    private static int hash(long v) {
        return Long.hashCode(v * HASH_SPREAD);
    }

    static interface Entry<E extends Entry<E, C>, C> {
        long cacheKey();

        /**
         * @return true if entry is the right kind of object
         */
        boolean cacheCheck(C check);

        E cacheNext();

        void cacheNext(E next);

        E cacheMoreUsed();

        void cacheMoreUsed(E more);

        E cacheLessUsed();

        void cacheLessUsed(E less);
    }
}
