/*
 *  Copyright (C) 2023 Cojen.org
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

import org.cojen.tupl.Aggregator;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 * @see AggregatedTable
 */
public final class AggregatedScanner<S, T> implements Scanner<T> {
    private final AggregatedTable<S, T> mAggregatedTable;
    private final Scanner<S> mSource;
    private final Comparator<T> mComparator;

    private Aggregator<S, T> mAggregator;
    private S mHeader;

    private S mSourceRow;
    private T mTargetRow;

    /**
     * @param comparator defines the target ordering; is null if not applicable
     */
    public AggregatedScanner(AggregatedTable<S, T> aggregatedTable, Scanner<S> source,
                             Comparator<T> comparator, T targetRow, Aggregator<S, T> aggregator)
        throws IOException
    {
        mAggregatedTable = aggregatedTable;
        mSource = source;
        mComparator = comparator;

        try {
            S sourceRow = source.row();

            if (sourceRow == null) {
                aggregator.close();
                return;
            }

            mAggregator = aggregator;
            Table<S> sourceTable = aggregatedTable.source();
            mHeader = sourceTable.newRow();

            sourceTable.copyRow(sourceRow, mHeader);
            mSourceRow = aggregator.begin(sourceRow);
        } catch (Throwable e) {
            try {
                close();
            } catch (Throwable e2) {
                RowUtils.suppress(e, e2);
            }
            throw e;
        }

        step(targetRow);
    }

    @Override
    public T row() {
        return mTargetRow;
    }

    @Override
    public T step(T targetRow) throws IOException {
        Aggregator<S, T> aggregator = mAggregator;

        if (aggregator == null) {
            mTargetRow = null;
            return null;
        }

        T finishedTargetRow;

        try {
            S sourceRow = mSourceRow;

            while (true) {
                if ((sourceRow = mSource.step(sourceRow)) != null
                    && mAggregatedTable.compareSourceRows(mHeader, sourceRow) == 0)
                {
                    sourceRow = aggregator.accumulate(sourceRow);
                    continue;
                }

                if (targetRow == null) {
                    targetRow = mAggregatedTable.newRow();
                } else {
                    mAggregatedTable.unsetRow(targetRow);
                }

                if ((finishedTargetRow = aggregator.finish(targetRow)) != null) {
                    mAggregatedTable.finishTarget(mHeader, finishedTargetRow);

                    if (sourceRow == null) {
                        break;
                    }

                    mTargetRow = finishedTargetRow;

                    mAggregatedTable.source().copyRow(sourceRow, mHeader);
                    mSourceRow = aggregator.begin(sourceRow);

                    return finishedTargetRow;
                }

                if (sourceRow == null) {
                    break;
                }

                mAggregatedTable.source().copyRow(sourceRow, mHeader);
                sourceRow = aggregator.begin(sourceRow);
            }
        } catch (Throwable e) {
            try {
                close();
            } catch (Throwable e2) {
                RowUtils.suppress(e, e2);
            }
            throw e;
        }

        mAggregator = null;
        mHeader = null;
        mSourceRow = null;
        mTargetRow = finishedTargetRow;

        try {
            aggregator.close();
        } catch (Throwable e) {
            mTargetRow = null;
            throw e;
        }
        
        return finishedTargetRow;
    }

    @Override
    public void close() throws IOException {
        mHeader = null;
        mSourceRow = null;
        mTargetRow = null;

        // Use try-with-resources to close both and not lose any exceptions.
        try (mSource) {
            Aggregator<S, T> aggregator = mAggregator;
            if (aggregator != null) {
                mAggregator = null;
                aggregator.close();
            }
        }
    }

    @Override
    public long estimateSize() {
        return mAggregatedTable.estimateSize();
    }

    @Override
    public int characteristics() {
        return mAggregatedTable.characteristics(mSource);
    }

    @Override
    public Comparator<? super T> getComparator() {
        Comparator<T> cmp = mComparator;
        if (cmp == null) {
            throw new IllegalStateException();
        }
        return cmp;
    }
}
