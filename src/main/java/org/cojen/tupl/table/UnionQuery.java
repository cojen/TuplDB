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

import org.cojen.tupl.diag.QueryPlan;

/**
 * @author Brian S. O'Neill
 * @see MergeQuery
 */
final class UnionQuery<R> implements Query<R> {
    private final Comparator<R> mComparator;
    private final Query<R>[] mSources;

    /**
     * @param sources must have at least one element; all must have the same arguments
     */
    UnionQuery(Comparator<R> c, Query<R>[] sources) {
        mComparator = c;
        mSources = sources;
    }

    @Override
    public Class<R> rowType() {
        return mSources[0].rowType();
    }

    @Override
    public int argumentCount() {
        return mSources[0].argumentCount();
    }

    @Override
    public Scanner<R> newScanner(R dst, Transaction txn, Object... args) throws IOException {
        @SuppressWarnings("unchecked")
        Scanner<R>[] sources = new Scanner[mSources.length];

        sources[0] = mSources[0].newScanner(dst, txn, args);

        try {
            for (int i=1; i<mSources.length; i++) {
                // cannot share dst among the sources
                sources[i] = mSources[i].newScanner(txn, args);
            }
        } catch (Throwable e) {
            for (var source : sources) RowUtils.closeQuietly(source);
            throw e;
        }

        return UnionScanner.make(mComparator, sources);
    }

    @Override
    public boolean anyRows(R row, Transaction txn, Object... args) throws IOException {
        for (var source : mSources) {
            if (source.anyRows(row, txn, args)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
        var subPlans = new QueryPlan[mSources.length];
        for (int i=0; i<subPlans.length; i++) {
            subPlans[i] = mSources[i].scannerPlan(txn, args);
        }
        return new QueryPlan.MergeUnion(subPlans);
    }
}
