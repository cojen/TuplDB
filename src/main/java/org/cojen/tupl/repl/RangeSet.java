/*
 *  Copyright (C) 2019 Cojen.org
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

import java.util.TreeSet;

/**
 * Maintains a set of log position ranges. New ranges are combined if they overlap with any
 * existing ranges.
 *
 * @author Brian S O'Neill
 */
final class RangeSet extends TreeSet<RangeSet.Range> {
    private boolean mClosed;

    public RangeSet() {
    }

    @Override
    public synchronized boolean isEmpty() {
        return super.isEmpty();
    }

    /**
     * @param start inclusive start
     * @param end exclusive end
     * @return false if closed
     */
    public synchronized boolean add(long start, long end) {
        if (mClosed) {
            return false;
        }

        if (end <= start) {
            return true;
        }

        var range = new Range(start, end);

        Range existing = super.floor(range); // findLe

        if (existing != null) foundLe: {
            if (start > existing.end) {
                // Add a new range which might be the highest overall.
                super.add(range);
                break foundLe;
            }
            if (end <= existing.end) {
                // New range fits within an existing range.
                return true;
            }
            // Extend the end of existing range.
            existing.end = end;
            range = existing;
        } else {
            existing = super.higher(range); // findGt
            if (existing == null || end < existing.start) {
                // Add a new range which is the lowest overall.
                super.add(range);
                return true;
            }
            // New range extends the start of an existing range.
            super.remove(existing);
            super.add(range);
            if (end <= existing.end) {
                range.end = existing.end;
                return true;
            }
        }

        while (true) {
            existing = super.higher(range); // findGt
            if (existing == null || end < existing.start) {
                return true;
            }
            // New range is adjacent to a higher range, so combine them.
            super.remove(existing);
            if (end <= existing.end) {
                range.end = existing.end;
                return true;
            }
            // Loop back and try to combine more ranges.
        }
    }

    /**
     * @return null if set is empty
     */
    public synchronized Range removeLowest() {
        return super.pollFirst();
    }

    public synchronized RangeSet copy() {
        var copy = new RangeSet();
        for (Range r : this) {
            copy.add(new Range(r.start, r.end));
        }
        return copy;
    }

    /**
     * @return false if not empty
     */
    public synchronized boolean closeIfEmpty() {
        if (super.isEmpty()) {
            mClosed = true;
            return true;
        } else {
            return false;
        }
    }

    public synchronized void close() {
        mClosed = true;
        super.clear();
    }

    static final class Range implements Comparable<Range> {
        public final long start;
        public long end;

        Range(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(Range other) {
            return Long.compare(start, other.start);
        }

        @Override
        public String toString() {
            return '[' + (start + ", " + end + ')');
        }
    }
}
