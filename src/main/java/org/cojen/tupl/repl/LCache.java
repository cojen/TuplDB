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
final class LCache<E extends LCache.Entry<E>> {
    // TODO: stripe for concurrency

    // Hash spreader. Based on rounded value of 2 ** 63 * (sqrt(5) - 1) equivalent 
    // to unsigned 11400714819323198485.
    private static final long HASH_SPREAD = -7046029254386353131L;

    private final int mMaxSize;
    private final E[] mEntries;

    private int mSize;

    private E mMostRecentlyUsed;
    private E mLeastRecentlyUsed;

    @SuppressWarnings("unchecked")
    LCache(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException();
        }
        mMaxSize = maxSize;
        // Arrays size must be a power of 2.
        mEntries = (E[]) new Entry[Math.max(1, Integer.highestOneBit(maxSize - 1) << 1)];
    }

    /**
     * @return null if none found
     */
    public E remove(long key) {
        final E[] entries = mEntries;
        final int slot = hash(key) & (entries.length - 1);

        synchronized (entries) {
            for (E entry = entries[slot], prev = null; entry != null; ) {
                E next = entry.cacheNext();
                if (entry.cacheKey() != key) {
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
        }

        return null;
    }

    /**
     * @return entry which was evicted (or null)
     */
    public E add(E entry) {
        final E[] entries = mEntries;
        int slot = hash(entry.cacheKey()) & (entries.length - 1);

        synchronized (entries) {
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

            entry = mLeastRecentlyUsed;
            final E moreUsed = entry.cacheMoreUsed();
            mLeastRecentlyUsed = moreUsed;
            entry.cacheMoreUsed(null);
            moreUsed.cacheLessUsed(null);

            slot = hash(entry.cacheKey()) & (entries.length - 1);

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
    }

    private static int hash(long v) {
        return Long.hashCode(v * HASH_SPREAD);
    }

    static interface Entry<E extends Entry<E>> {
        long cacheKey();

        E cacheNext();

        void cacheNext(E next);

        E cacheMoreUsed();

        void cacheMoreUsed(E more);

        E cacheLessUsed();

        void cacheLessUsed(E less);
    }
}
