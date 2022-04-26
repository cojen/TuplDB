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

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

/**
 * A factory that combines ScanControllers which are known to be disjoint.
 *
 * @author Brian S O'Neill
 */
final class DisjointUnionScanControllerFactory<R> implements ScanControllerFactory<R> {
    private final ScanControllerFactory<R>[] mSubsets;

    DisjointUnionScanControllerFactory(ScanControllerFactory<R>[] subsets) {
        if (subsets.length <= 1) {
            throw new IllegalArgumentException();
        }
        mSubsets = subsets;
    }

    @Override
    public QueryPlan plan(Object... args) {
        var plans = new QueryPlan[mSubsets.length];
        for (int i=0; i<plans.length; i++) {
            plans[i] = mSubsets[i].plan(args);
        }
        return new QueryPlan.DisjointUnion(plans);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanControllerFactory<R> reverse() {
        var reversed = new ScanControllerFactory[mSubsets.length];
        for (int i=0; i<reversed.length; i++) {
            reversed[reversed.length - i - 1] = mSubsets[i].reverse();
        }
        return new DisjointUnionScanControllerFactory<R>(reversed);
    }

    @Override
    public RowPredicate<R> predicate(Object... args) {
        return mSubsets[0].predicate(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanController<R> scanController(Object... args) {
        var subsets = mSubsets;
        var controllers = new ScanController[subsets.length];

        var first = subsets[0].scanController(args);
        RowPredicate predicate = first.predicate();
        controllers[0] = first;

        for (int i=1; i<controllers.length; i++) {
            controllers[i] = subsets[i].scanController(predicate);
        }

        return new DisjointUnionScanController<R>(controllers);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanController<R> scanController(RowPredicate predicate) {
        var subsets = mSubsets;
        var controllers = new ScanController[subsets.length];

        for (int i=0; i<controllers.length; i++) {
            controllers[i] = subsets[i].scanController(predicate);
        }

        return new DisjointUnionScanController<R>(controllers);
    }
}
