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
import org.cojen.tupl.Updater;

/**
 * @author Brian S. O'Neill
 * @see MergeQuery
 */
final class MergeUpdater<R> extends MergeScanner<R> implements Updater<R> {
    /**
     * @param sources must have at least one element
     */
    static <R> Updater<R> make(Comparator<R> c, Updater<R>[] sources) {
        int length = sources.length;
        if (length == 2) {
            return new MergeUpdater<R>(c, sources[0], sources[1]);
        }
        return make(c, sources, 0, length);
    }

    private static <R> Updater<R> make(Comparator<R> c, Updater<R>[] sources, int start, int end) {
        int length = end - start;
        if (length == 1) {
            return sources[start];
        }
        // Use rint for half-even rounding.
        int mid = start + ((int) Math.rint(length / 2.0));
        return new MergeUpdater<R>(c, make(c, sources, start, mid), make(c, sources, mid, end));
    }

    MergeUpdater(Comparator<R> c, Updater<R> source1, Updater<R> source2) {
        super(c, source1, source2);
    }

    @Override
    public R update(R dst) throws IOException {
        var current = (Updater<R>) mCurrent;
        R row1, row2;

        if (current == mSource1) {
            row1 = current.update(dst);
            row2 = mSource2.row();
        } else {
            row1 = mSource1.row();
            row2 = current.update(dst);
        }

        return finishStep(row1, row2);
    }

    @Override
    public R delete(R dst) throws IOException {
        var current = (Updater<R>) mCurrent;
        R row1, row2;

        if (current == mSource1) {
            row1 = current.delete(dst);
            row2 = mSource2.row();
        } else {
            row1 = mSource1.row();
            row2 = current.delete(dst);
        }

        return finishStep(row1, row2);
    }
}
