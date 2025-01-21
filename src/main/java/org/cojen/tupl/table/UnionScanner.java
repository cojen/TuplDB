/*
 *  Copyright (C) 2025 Cojen.org
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

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Scanner;

/**
 * @author Brian S. O'Neill
 * @see UnionQuery
 */
final class UnionScanner<R> implements Scanner<R> {
    /**
     * @param sources must have at least one element; each source must be ordered and distinct
     */
    static <R> Scanner<R> make(Comparator<R> c, Scanner<R>[] sources) throws IOException {
        int length = sources.length;
        if (length == 2) {
            return new UnionScanner<R>(c, sources[0], sources[1]);
        }
        return make(c, sources, 0, length);
    }

    private static <R> Scanner<R> make(Comparator<R> c, Scanner<R>[] sources, int start, int end)
        throws IOException
    {
        int length = end - start;
        if (length == 1) {
            return sources[start];
        }
        // Use rint for half-even rounding.
        int mid = start + ((int) Math.rint(length / 2.0));
        return new UnionScanner<R>(c, make(c, sources, start, mid), make(c, sources, mid, end));
    }

    protected final Comparator<R> mComparator;
    protected final Scanner<R> mSource1, mSource2;

    protected Scanner<R> mCurrent;

    UnionScanner(Comparator<R> c, Scanner<R> source1, Scanner<R> source2) throws IOException {
        mComparator = c;
        mSource1 = source1;
        mSource2 = source2;

        R row1 = source1.row();
        if (row1 == null) {
            mCurrent = source2;
        } else {
            R row2 = source2.row();
            while (true) {
                if (row2 == null) {
                    mCurrent = source1;
                    return;
                }
                int cmp = c.compare(row1, row2);
                if (cmp < 0) {
                    mCurrent = source1;
                    return;
                } else if (cmp > 0) {
                    mCurrent = source2;
                    return;
                }
                // When duplicates are found, favor the rows from the first source.
                row2 = source2.step(row2);
            }
        }
    }

    @Override
    public R row() {
        return mCurrent.row();
    }

    @Override
    public R step(R dst) throws IOException {
        Scanner<R> source1 = mSource1, source2 = mSource2;
        Scanner<R> current = mCurrent;
        R row1, row2;

        if (current == source1) {
            row1 = current.step(dst);
            row2 = source2.row();
        } else {
            row1 = source1.row();
            row2 = current.step(dst);
        }

        if (row1 == null) {
            mCurrent = source2;
            return row2;
        } else if (row2 == null) {
            mCurrent = source1;
            return row1;
        }

        Comparator<R> c = mComparator;

        while (true) {
            int cmp = c.compare(row1, row2);
            if (cmp < 0) {
                mCurrent = source1;
                return row1;
            } else if (cmp > 0) {
                mCurrent = source2;
                return row2;
            }
            // When duplicates are found, favor the rows from the first source.
            row2 = source2.step(row2);
            if (row2 == null) {
                mCurrent = source1;
                return row1;
            }
        }
    }

    @Override
    public void close() throws IOException {
        // Use try-with-resources to close both and not lose any exceptions.
        try (mSource1) {
            mSource2.close();
        }
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | DISTINCT | SORTED | NONNULL | CONCURRENT;
    }

    @Override
    public Comparator<R> getComparator() {
        return mComparator;
    }
}
