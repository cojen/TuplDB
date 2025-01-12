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

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

/**
 * @author Brian S. O'Neill
 * @see ConcatTable
 */
final class MergeQuery<R> extends ConcatQuery<R> {
    private final Comparator<R> mComparator;

    /**
     * @param sources must have at least one element; all must have the same arguments
     */
    MergeQuery(Query<R>[] sources, Comparator<R> c) {
        super(sources);
        mComparator = c;
    }

    @Override
    public Scanner<R> newScanner(R dst, Transaction txn, Object... args) throws IOException {
        @SuppressWarnings("unchecked")
        Scanner<R>[] sources = new Scanner[mSources.length];

        sources[0] = mSources[0].newScanner(dst, txn, args);

        for (int i=1; i<mSources.length; i++) {
            // cannot share dst among the sources
            sources[i] = mSources[i].newScanner(txn, args);
        }

        return MergeScanner.make(mComparator, sources);
    }

    @Override
    public Updater<R> newUpdater(R dst, Transaction txn, Object... args) throws IOException {
        checkCanUpdate(txn, args);

        @SuppressWarnings("unchecked")
        Updater<R>[] sources = new Updater[mSources.length];

        sources[0] = mSources[0].newUpdater(dst, txn, args);

        for (int i=1; i<mSources.length; i++) {
            // cannot share dst among the sources
            sources[i] = mSources[i].newUpdater(txn, args);
        }

        return MergeUpdater.make(mComparator, sources);
    }

    @Override
    public long deleteAll(Transaction txn, Object... args) throws IOException {
        // Use the default deleteAll implementation to delete in the desired order.
        return RowUtils.deleteAll(this, txn, args);
    }

    @Override
    protected QueryPlan newPlan(QueryPlan[] subPlans) {
        return new QueryPlan.Merge(subPlans);
    }
}
