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

import org.cojen.tupl.Grouper;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 * @see GroupedTable
 */
public class GroupedScanner<S, T> implements Scanner<T> {
    private final GroupedTable<S, T> mGroupedTable;
    private final Scanner<S> mSource;
    private final Comparator<S> mComparator;

    private Grouper<S, T> mGrouper;
    private S mHeader;

    private S mSourceRow;
    private T mTargetRow;

    public GroupedScanner(GroupedTable<S, T> groupedTable, Scanner<S> source,
                          T targetRow, Grouper<S, T> grouper)
        throws IOException
    {
        mGroupedTable = groupedTable;
        mSource = source;
        mComparator = groupedTable.mGroupComparator;

        try {
            S sourceRow = source.row();

            if (sourceRow == null) {
                grouper.close();
                return;
            }

            mGrouper = grouper;
            Table<S> sourceTable = groupedTable.source();
            mHeader = sourceTable.newRow();

            sourceTable.copyRow(sourceRow, mHeader);
            mSourceRow = grouper.begin(sourceRow);
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
    public final T row() {
        return mTargetRow;
    }

    @Override
    public final T step(T targetRow) throws IOException {
        Grouper<S, T> grouper = mGrouper;

        if (grouper == null) {
            mTargetRow = null;
            return null;
        }

        T actualTargetRow = mTargetRow;

        doStep: try {
            S sourceRow;

            if (actualTargetRow == null) {
                sourceRow = mSourceRow;
            } else {
                targetRow = prepareTargetRow(targetRow);

                while ((actualTargetRow = grouper.step(targetRow)) != null) {
                    if (finish(actualTargetRow)) {
                        mGroupedTable.cleanRow(actualTargetRow);
                        mTargetRow = actualTargetRow;
                        return actualTargetRow;
                    }
                }

                sourceRow = mSourceRow;

                if (sourceRow == null) {
                    break doStep;
                }

                mGroupedTable.source().copyRow(sourceRow, mHeader);
                sourceRow = grouper.begin(sourceRow);
            }

            while (true) {
                if ((sourceRow = mSource.step(sourceRow)) != null
                    && mComparator.compare(mHeader, sourceRow) == 0)
                {
                    sourceRow = grouper.accumulate(sourceRow);
                    continue;
                }

                mSourceRow = sourceRow;
                targetRow = prepareTargetRow(targetRow);
                actualTargetRow = grouper.process(targetRow);

                if (actualTargetRow != null) {
                    if (finish(actualTargetRow)) {
                        mGroupedTable.cleanRow(actualTargetRow);
                        mTargetRow = actualTargetRow;
                        return actualTargetRow;
                    }

                    while ((actualTargetRow = grouper.step(targetRow)) != null) {
                        if (finish(actualTargetRow)) {
                            mGroupedTable.cleanRow(actualTargetRow);
                            mTargetRow = actualTargetRow;
                            return actualTargetRow;
                        }
                    }
                }

                if (sourceRow == null) {
                    mTargetRow = actualTargetRow;
                    break;
                }

                mGroupedTable.source().copyRow(sourceRow, mHeader);
                sourceRow = grouper.begin(sourceRow);
            }
        } catch (Throwable e) {
            try {
                close();
            } catch (Throwable e2) {
                RowUtils.suppress(e, e2);
            }
            throw e;
        }

        mGrouper = null;
        mHeader = null;

        try {
            grouper.close();
        } catch (Throwable e) {
            mTargetRow = null;
            throw e;
        }

        return actualTargetRow;
    }

    private T prepareTargetRow(T targetRow) {
        if (targetRow == null) {
            targetRow = mGroupedTable.newRow();
        } else {
            mGroupedTable.unsetRow(targetRow);
        }
        return targetRow;
    }

    /**
     * Override to apply a custom finishing operation to the target row.
     *
     * @return false if filtered out
     */
    protected boolean finish(T targetRow) {
        return true;
    }

    @Override
    public final void close() throws IOException {
        mHeader = null;
        mSourceRow = null;
        mTargetRow = null;

        // Use try-with-resources to close both and not lose any exceptions.
        try (mSource) {
            Grouper<S, T> grouper = mGrouper;
            if (grouper != null) {
                mGrouper = null;
                grouper.close();
            }
        }
    }

    @Override
    public final long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public final int characteristics() {
        return NONNULL;
    }
}
