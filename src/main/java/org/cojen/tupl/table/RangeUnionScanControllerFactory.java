/*
 *  Copyright (C) 2021 Cojen.org
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

import java.util.Arrays;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

/**
 * A factory that combines SingleScanControllers which have the same natural order.
 *
 * @author Brian S O'Neill
 */
final class RangeUnionScanControllerFactory<R> implements ScanControllerFactory<R> {
    private final ScanControllerFactory<R>[] mRanges;

    /**
     * Each range must produce SingleScanController instances.
     */
    RangeUnionScanControllerFactory(ScanControllerFactory<R>[] ranges) {
        if (ranges.length <= 1) {
            throw new IllegalArgumentException();
        }
        mRanges = ranges;
    }

    @Override
    public int argumentCount() {
        int max = 0;
        for (ScanControllerFactory<R> range : mRanges) {
            max = Math.max(max, range.argumentCount());
        }
        return max;
    }

    @Override
    public QueryPlan plan(Object... args) {
        var plans = new QueryPlan[mRanges.length];
        for (int i=0; i<plans.length; i++) {
            plans[i] = mRanges[i].plan(args);
        }
        return new QueryPlan.RangeUnion(plans);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanControllerFactory<R> reverse() {
        var reversed = new ScanControllerFactory[mRanges.length];
        for (int i=0; i<reversed.length; i++) {
            reversed[reversed.length - i - 1] = mRanges[i].reverse();
        }
        return new RangeUnionScanControllerFactory<R>(reversed);
    }

    @Override
    public RowPredicate<R> predicate(Object... args) {
        return mRanges[0].predicate(args);
    }

    @Override
    public int characteristics() {
        return mRanges[0].characteristics();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanController<R> scanController(Object... args) {
        var ranges = mRanges;
        var controllers = new SingleScanController[ranges.length];

        var first = (SingleScanController<R>) ranges[0].scanController(args);
        RowPredicate<R> predicate = first.predicate();
        controllers[0] = first;

        for (int i=1; i<controllers.length; i++) {
            controllers[i] = (SingleScanController<R>) ranges[i].scanController(predicate);
        }

        Arrays.sort(controllers, SingleScanController::compareLow);

        return new RangeUnionScanController<R>(controllers);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanController<R> scanController(RowPredicate<R> predicate) {
        var ranges = mRanges;
        var controllers = new SingleScanController[ranges.length];

        for (int i=0; i<controllers.length; i++) {
            controllers[i] = (SingleScanController<R>) ranges[i].scanController(predicate);
        }

        Arrays.sort(controllers, SingleScanController::compareLow);

        return new RangeUnionScanController<R>(controllers);
    }
}
