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

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

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
    public Scanner<R> newScanner(Transaction txn, R row, Object... args) throws IOException {
        return new ConcatScanner<R>(characteristics(), row) {
            private int mWhich;

            @Override
            public Scanner<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    Scanner<R> next = mLaunchers[which].newScanner(txn, dst, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public Updater<R> newUpdater(Transaction txn, R row, Object... args) throws IOException {
        return new ConcatUpdater<R>(characteristics(), row) {
            private int mWhich;

            @Override
            public Updater<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mLaunchers.length) {
                    return null;
                } else {
                    Updater<R> next = mLaunchers[which].newUpdater(txn, dst, args);
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

    @Override
    public int characteristics() {
        return mLaunchers[0].characteristics();
    }
}
