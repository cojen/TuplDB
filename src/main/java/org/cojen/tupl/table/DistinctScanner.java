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
import org.cojen.tupl.Table;

/**
 * @author Brian S. O'Neill
 * @see AggregatedTable
 */
public final class DistinctScanner<R> implements Scanner<R> {
    private final Table<R> mSourceTable;
    private final Comparator<R> mComparator;
    private final Scanner<R> mSourceScanner;

    private R mHeader;

    /**
     * @param comparator expected to compare all columns
     * @param sourceScanner order matches that of given comparator
     */
    public DistinctScanner(Table<R> sourceTable, Comparator<R> comparator, Scanner<R> sourceScanner)
        throws IOException
    {
        mSourceTable = sourceTable;
        mComparator = comparator;
        mSourceScanner = sourceScanner;
        R sourceRow = sourceScanner.row();
        if (sourceRow != null) {
            mHeader = sourceTable.cloneRow(sourceRow);
        }
    }

    @Override
    public R row() {
        return mSourceScanner.row();
    }

    @Override
    public R step(R row) throws IOException {
        while ((row = mSourceScanner.step(row)) != null) {
            if (mComparator.compare(mHeader, row) != 0) {
                mSourceTable.copyRow(row, mHeader);
                return row;
            }
        }
        mHeader = null;
        return null;
    }

    @Override
    public void close() throws IOException {
        mSourceScanner.close();
        mHeader = null;
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
