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

    private boolean mBegin;

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

        if (targetRow == null) {
            targetRow = mGroupedTable.newRow();
        } else {
            mGroupedTable.unsetRow(targetRow);
        }

        try {
            loop: while (true) {
                while (true) {
                    T actualTargetRow = grouper.step(targetRow);
                    if (actualTargetRow == null) {
                        break;
                    }
                    if (finish(actualTargetRow)) {
                        mGroupedTable.cleanRow(actualTargetRow);
                        mTargetRow = actualTargetRow;
                        return actualTargetRow;
                    }
                }

                doSource: {
                    if (mBegin) {
                        S sourceRow = mSource.row();
                        if (sourceRow != null) {
                            mSourceRow = grouper.begin(sourceRow);
                            mBegin = false;
                            break doSource;
                        } else if (mHeader != null) {
                            mHeader = null;
                        } else {
                            break loop;
                        }
                    } else {
                        S sourceRow = mSource.step(mSourceRow);
                        if (sourceRow != null) {
                            if (mComparator.compare(mHeader, sourceRow) == 0) {
                                mSourceRow = grouper.accumulate(sourceRow);
                                break doSource;
                            }
                            mGroupedTable.source().copyRow(sourceRow, mHeader);
                        } else if (mHeader != null) {
                            mHeader = null;
                        } else {
                            break loop;
                        }
                        mBegin = true;
                    }

                    grouper.finished();
                }

                mGroupedTable.unsetRow(targetRow);
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
        mTargetRow = null;

        grouper.close();

        return null;
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

    /**
     * Override to apply a custom finishing operation to the target row.
     *
     * @return false if filtered out
     */
    protected boolean finish(T targetRow) {
        return true;
    }
}
