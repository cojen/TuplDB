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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Set;

import java.util.function.Predicate;

import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import static java.util.Spliterator.*;

/**
 * Supports queries that scan over multiple tables.
 *
 * @author Brian S O'Neill
 */
final class DisjointUnionQueryLauncher<R> implements QueryLauncher<R> {
    private final QueryLauncher<R>[] mLaunchers;

    /**
     * @param launchers at least one, and each launcher must provide a disjoint set of results;
     * each launcher must have the same projection
     */
    DisjointUnionQueryLauncher(QueryLauncher<R>[] launchers) {
        mLaunchers = launchers;
    }

    @Override
    public RowScanner<R> newRowScanner(Transaction txn, R row, Object... args) throws IOException {
        // FIXME: Depending on the projection, DISTINCT shouldn't be included.
        int characteristics = ORDERED | DISTINCT | NONNULL | CONCURRENT;

        return new ConcatRowScanner<R>(characteristics, row) {
            private int mWhich;

            @Override
            public RowScanner<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    RowScanner<R> next = mLaunchers[which].newRowScanner(txn, dst, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public RowUpdater<R> newRowUpdater(Transaction txn, R row, Object... args) throws IOException {
        // FIXME: Depending on the projection, DISTINCT shouldn't be included.
        int characteristics = ORDERED | DISTINCT | NONNULL | CONCURRENT;

        return new ConcatRowUpdater<R>(characteristics, row) {
            private int mWhich;

            @Override
            public RowUpdater<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    RowUpdater<R> next = mLaunchers[which].newRowUpdater(txn, dst, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public QueryPlan plan(Object... args) {
        var subPlans = new QueryPlan[mLaunchers.length];
        for (int i=0; i<subPlans.length; i++) {
            subPlans[i] = mLaunchers[i].plan(args);
        }
        return new QueryPlan.DisjointUnion(subPlans);
    }

    @Override
    public Set<String> projection() {
        return mLaunchers[0].projection();
    }
}
