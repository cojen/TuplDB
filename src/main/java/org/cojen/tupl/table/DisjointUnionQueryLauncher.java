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

import java.io.IOException;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import static java.util.Spliterator.*;

/**
 * Supports queries that scan over multiple tables.
 *
 * @author Brian S O'Neill
 */
public final class DisjointUnionQueryLauncher<R> extends QueryLauncher<R> {
    private final QueryLauncher<R>[] mLaunchers;

    /**
     * @param launchers at least one, and each launcher must provide a disjoint set of results;
     * each launcher must have the same projection
     */
    public DisjointUnionQueryLauncher(QueryLauncher<R>[] launchers) {
        mLaunchers = launchers;
    }

    @Override
    public Class<R> rowType() {
        return mLaunchers[0].rowType();
    }

    @Override
    public int argumentCount() {
        int max = 0;
        for (QueryLauncher launcher : mLaunchers) {
            max = Math.max(max, launcher.argumentCount());
        }
        return max;
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
        return new ConcatScanner<R>(row) {
            private int mWhich;

            @Override
            public Scanner<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    Scanner<R> next = mLaunchers[which].newScanner(dst, txn, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        return new ConcatUpdater<R>(row) {
            private int mWhich;

            @Override
            public Updater<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    Updater<R> next = mLaunchers[which].newUpdater(dst, txn, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public void scanWrite(Transaction txn, RowWriter writer, Object... args) throws IOException {
        writer.writeCharacteristics(NONNULL | ORDERED | CONCURRENT, 0);

        for (QueryLauncher launcher : mLaunchers) {
            launcher.scanWrite(txn, writer, args);
        }
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
        var subPlans = new QueryPlan[mLaunchers.length];
        for (int i=0; i<subPlans.length; i++) {
            subPlans[i] = mLaunchers[i].scannerPlan(txn, args);
        }
        return new QueryPlan.DisjointUnion(subPlans);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        var subPlans = new QueryPlan[mLaunchers.length];
        for (int i=0; i<subPlans.length; i++) {
            subPlans[i] = mLaunchers[i].updaterPlan(txn, args);
        }
        return new QueryPlan.DisjointUnion(subPlans);
    }

    @Override
    protected void closeIndexes() throws IOException {
        for (QueryLauncher launcher : mLaunchers) {
            launcher.closeIndexes();
        }
    }

    @Override
    protected void clearCache() {
        for (QueryLauncher launcher : mLaunchers) {
            launcher.clearCache();
        }
    }
}
