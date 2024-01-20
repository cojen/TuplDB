/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.table.filter;

import org.cojen.tupl.core.Utils;

/**
 * An immutable set of filters which uses RowFilter.isMatch behavior.
 *
 * @author Brian S O'Neill
 */
final class MatchSet {
    private final Entry[] mEntries;
    private final int mSize;

    MatchSet(GroupFilter group) {
        this(group.mSubFilters);
    }

    MatchSet(RowFilter... filters) {
        var entries = new Entry[Utils.roundUpPower2(filters.length + 1)];
        int size = 0;

        add: for (RowFilter filter : filters) {
            int hash = filter.matchHashCode();
            int index = hash & (entries.length - 1);
            for (Entry e = entries[index]; e != null; e = e.mNext) {
                if (hash == e.mMatchHash && e.mFilter.equals(filter)) {
                    // Already in the set.
                    continue add;
                }
            }
            entries[index] = new Entry(filter, hash, entries[index]);
            size++;
        }

        mEntries = entries;
        mSize = size;
    }

    /**
     * Checks if the given filter (or its inverse) matches any in the set.
     *
     * @return 0 if doesn't match, 1 if equal match, or -1 if inverse equally matches
     */
    int hasMatch(RowFilter filter) {
        int hash = filter.matchHashCode();
        Entry[] entries = mEntries;
        for (Entry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            int result;
            if (hash == e.mMatchHash && (result = e.mFilter.isMatch(filter)) != 0) {
                return result;
            }
        }
        return 0;
    }

    /**
     * Returns -1 if all filters of the given set are inversely matched to the filters of this
     * set, and both sets have the same size.
     *
     * @return 0 if doesn't match or -1 if inverse equally matches
     */
    int inverseMatches(MatchSet other) {
        if (mSize != other.mSize) {
            return 0;
        }
        Entry[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (Entry e = entries[i]; e != null; e = e.mNext) {
                if (other.hasMatch(e.mFilter) >= 0) {
                    return 0;
                }
            }
        }
        return -1;
    }

    /**
     * Checks if the given filter is found in the set.
     *
     * @return 0 if doesn't match or 1 if equal match
     */
    int hasEqualMatch(RowFilter filter) {
        int hash = filter.matchHashCode();
        Entry[] entries = mEntries;
        for (Entry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            if (hash == e.mMatchHash && e.mFilter.equals(filter)) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * Returns 1 if the given set is exactly equal to this one.
     *
     * @return 0 if doesn't match or 1 if equal match
     */
    int equalMatches(MatchSet other) {
        if (mSize != other.mSize) {
            return 0;
        }
        Entry[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (Entry e = entries[i]; e != null; e = e.mNext) {
                if (other.hasEqualMatch(e.mFilter) == 0) {
                    return 0;
                }
            }
        }
        return 1;
    }

    /**
     * Returns 1 if the given set is exactly equal to this one.
     *
     * @param exclude don't consider this filter, which must exist in this set, and the
     * inverse must exist in the other set
     * @return 0 if doesn't match or 1 if equal match
     */
    int equalMatches(MatchSet other, RowFilter exclude) {
        if (mSize != other.mSize) {
            return 0;
        }
        Entry[] entries = mEntries;
        for (int i=0; i<entries.length; i++) {
            for (Entry e = entries[i]; e != null; e = e.mNext) {
                if (!e.mFilter.equals(exclude) && other.hasEqualMatch(e.mFilter) == 0) {
                    return 0;
                }
            }
        }
        return other.hasMatch(exclude) < 0 ? 1 : 0;
    }

    private static class Entry {
        final RowFilter mFilter;
        final int mMatchHash;
        final Entry mNext;

        Entry(RowFilter filter, int hash, Entry next) {
            mFilter = filter;
            mMatchHash = hash;
            mNext = next;
        }
    }
}
