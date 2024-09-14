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

/**
 * Used by RowMethodsMaker to access join columns via dotted path names.
 *
 * @author Brian S. O'Neill
 */
public final class PathSplitter {
    public static final class Entry {
        /** Column number as defined by RowGen.columnNumbers(). */
        public final int number;

        /** The portion of the path string after the first dot. */
        public final String tail;

        private final String mPath;

        private Entry mNext;

        private Entry(int number, String tail, String path) {
            this.number = number;
            this.tail = tail;
            this.mPath = path;
        }
    }

    private final Class<?> mRowType;

    private Entry[] mEntries;
    private int mSize;

    public PathSplitter(Class<?> rowType) {
        mRowType = rowType;
        // Initial capacity must be a power of 2.
        mEntries = new Entry[4];
    }

    /**
     * @throws IllegalArgumentException if the path doesn't refer to a known column
     */
    public Entry find(String path) {
        Entry e = doFind(path);
        return e != null ? e : findSync(path);
    }

    private Entry findSync(String path) {
        Entry e;
        synchronized (this) {
            e = doFind(path);
        }
        if (e == null) {
            throw new IllegalArgumentException("Column path isn't found: " + path);
        }
        return e;
    }

    private Entry doFind(String path) {
        var entries = mEntries;
        int hash = path.hashCode();
        int index = hash & (entries.length - 1);

        for (var e = entries[index]; e != null; e = e.mNext) {
            if (e.mPath.equals(path)) {
                return e;
            }
        }

        if (!Thread.holdsLock(this)) {
            return null;
        }

        int ix = path.indexOf('.');
        if (ix <= 0) {
            return null;
        }

        String tail = path.substring(ix + 1);
        if (tail.isEmpty()) {
            return null;
        }

        Integer num = RowInfo.find(mRowType).rowGen().columnNumbers().get(path.substring(0, ix));
        if (num == null) {
            return null;
        }

        if (mSize >= entries.length) {
            // Rehash.
            var newEntries = new Entry[entries.length << 1];
            for (int i=0; i<entries.length; i++) {
                for (Entry e = entries[i]; e != null; ) {
                    Entry next = e.mNext;
                    index = e.mPath.hashCode() & (newEntries.length - 1);
                    e.mNext = newEntries[index];
                    newEntries[index] = e;
                    e = next;
                }
            }
            mEntries = entries = newEntries;
            index = hash & (entries.length - 1);
        }

        Entry e = new Entry(num, tail.intern(), path.intern());
        e.mNext = entries[index];
        entries[index] = e;
        mSize++;

        return e;
    }

    public synchronized void remove(Entry entry) {
        Entry[] entries = mEntries;
        int index = entry.mPath.hashCode() & (entries.length - 1);
        for (Entry e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e == entry) {
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
}
