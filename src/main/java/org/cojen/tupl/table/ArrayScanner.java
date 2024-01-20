/*
 *  Copyright (C) 2022 Cojen.org
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

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;

/**
 * A Scanner backed by an array of decoded rows.
 *
 * @author Brian S O'Neill
 * @see RowSorter
 */
abstract class ArrayScanner<R> implements Scanner<R> {
    private static final Object[] EMPTY = new Object[1];

    private final Table<R> mTable;

    private R[] mRows;
    private int mPosition;

    /**
     * Construct an empty scanner.
     */
    ArrayScanner() {
        mTable = null;
        close();
    }

    ArrayScanner(Table<R> table, R[] rows) {
        mTable = table;
        mRows = rows;
    }

    @Override
    public final R row() {
        return mRows[mPosition];
    }

    @Override
    public final R step() {
        R[] rows = mRows;
        int pos = mPosition;
        rows[pos++] = null; // help GC
        if (pos < rows.length) {
            mPosition = pos;
            return rows[pos];
        }
        close();
        return null;
    }

    @Override
    public final R step(R dst) {
        R[] rows = mRows;
        int pos = mPosition;
        rows[pos++] = null; // help GC
        if (pos < rows.length) {
            R row = rows[pos];
            if (dst == null) {
                dst = row;
            } else {
                mTable.copyRow(row, dst);
                rows[pos] = dst;
            }
            mPosition = pos;
            return dst;
        }
        close();
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void close() {
        mRows = (R[]) EMPTY;
        mPosition = 0;
    }

    @Override
    public final long estimateSize() {
        return mRows.length - mPosition;
    }

    @Override
    public final int characteristics() {
        return NONNULL | ORDERED | IMMUTABLE | SIZED | SORTED;
    }
}
