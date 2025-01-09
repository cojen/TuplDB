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
import java.util.PriorityQueue;

import org.cojen.tupl.Scanner;

/**
 * @author Brian S. O'Neill
 * @see MergeQuery
 */
class MergeScanner<R> implements Scanner<R>, Comparator<Scanner<R>> {
    private final Comparator<R> mComparator;
    private final PriorityQueue<Scanner<R>> mQueue;

    MergeScanner(Scanner<R>[] sources, Comparator<R> c) throws IOException {
        mComparator = c;
        mQueue = new PriorityQueue<>(sources.length, this);
        for (var source : sources) {
            if (source.row() != null) {
                mQueue.add(source);
            }
        }
    }

    @Override
    public R row() {
        Scanner<R> source = mQueue.peek();
        return source == null ? null : source.row();
    }

    @Override
    public R step(R dst) throws IOException {
        PriorityQueue<Scanner<R>> queue = mQueue;

        while (true) {
            Scanner<R> source = queue.poll();

            if (source == null) {
                return null;
            }

            R row = source.step(dst);

            Scanner<R> current;
            if (row == null) {
                current = queue.peek();
                if (current == null) {
                    return null;
                }
            } else {
                queue.add(source);
                current = queue.peek();
                if (current == source) {
                    return row;
                }
            }

            row = current.row();

            if (row != null) {
                return row;
            }

            // This point is only expected to be reached if the current scanner isn't working
            // correctly and has closed for some reason. Loop back to discard it.

            dst = null; // cannot share dst among the sources
        }
    }

    @Override
    public void close() throws IOException {
        Throwable ex = null;

        for (Scanner s : mQueue) {
            try {
                s.close();
            } catch (Throwable e) {
                if (ex == null) {
                    ex = e;
                }
            }
        }

        mQueue.clear();

        if (ex != null) {
            throw RowUtils.rethrow(ex);
        }
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | CONCURRENT;
    }

    @Override
    public int compare(Scanner<R> a, Scanner<R> b) {
        return mComparator.compare(a.row(), b.row());
    }

    protected Scanner<R> current() {
        return mQueue.peek();
    }
}
