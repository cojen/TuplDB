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

import java.util.stream.Stream;

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.diag.QueryPlan;

/**
 * @author Brian S. O'Neill
 * @see ConcatTable
 */
class ConcatQuery<R> implements Query<R> {
    protected final Query<R>[] mSources;

    protected Boolean mCanUpdate;

    /**
     * @param sources must have at least one element; all must have the same arguments
     */
    ConcatQuery(Query<R>[] sources) {
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
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
        return new ConcatScanner<R>(row) {
            private int mWhich;

            @Override
            public Scanner<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mSources.length) {
                    return null;
                } else {
                    Scanner<R> next = mSources[which].newScanner(row, txn, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        checkCanUpdate(txn, args);

        return new ConcatUpdater<R>(row) {
            private int mWhich;

            @Override
            public Updater<R> next(R dst) throws IOException {
                int which = mWhich;
                if (which >= mSources.length) {
                    return null;
                } else {
                    Updater<R> next = mSources[which].newUpdater(row, txn, args);
                    mWhich = which + 1;
                    return next;
                }
            }
        };
    }

    @Override
    public long deleteAll(Transaction txn, Object... args) throws IOException {
        long total = 0;

        if (txn != null) {
            txn.enter();
        }

        try {
            for (var source : mSources) {
                total += source.deleteAll(txn, args);
            }
            if (txn != null) {
                txn.commit();
            }
        } finally {
            if (txn != null) {
                txn.exit();
            }
        }

        return total;
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
        return newPlan(subPlans);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        var subPlans = new QueryPlan[mSources.length];
        for (int i=0; i<subPlans.length; i++) {
            subPlans[i] = mSources[i].updaterPlan(txn, args);
        }
        return newPlan(subPlans);
    }

    protected QueryPlan newPlan(QueryPlan[] subPlans) {
        return new QueryPlan.Concat(subPlans);
    }

    protected final void checkCanUpdate(Transaction txn, Object... args) throws IOException {
        Boolean canUpdate = mCanUpdate;
        if (canUpdate == null) {
            doCheckCanUpdate(txn, args);
        } else if (!canUpdate) {
            throw new ViewConstraintException();
        }
    }

    private void doCheckCanUpdate(Transaction txn, Object... args) throws IOException {
        boolean canUpdate;
        try {
            updaterPlan(txn, args);
            canUpdate = true;
        } catch (ViewConstraintException e) {
            canUpdate = false;
        }
        mCanUpdate = canUpdate;
        if (!canUpdate) {
            throw new ViewConstraintException();
        }
    }
}
