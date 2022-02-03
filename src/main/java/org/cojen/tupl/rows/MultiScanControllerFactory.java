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

package org.cojen.tupl.rows;

import java.util.Arrays;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class MultiScanControllerFactory<R> implements ScanControllerFactory<R> {
    private final ScanControllerFactory<R>[] mRanges;

    MultiScanControllerFactory(ScanControllerFactory<R>[] ranges) {
        if (ranges.length <= 1) {
            throw new IllegalArgumentException();
        }
        mRanges = ranges;
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
    public ScanController<R> newScanController(Object... args) {
        var ranges = mRanges;
        var controllers = new ScanController[ranges.length];

        ScanController<R> first = ranges[0].newScanController(args);
        RowPredicate predicate = first.predicate();
        controllers[0] = first;

        for (int i=1; i<controllers.length; i++) {
            controllers[i] = ranges[i].newScanController(predicate);
        }

        Arrays.sort(controllers, ScanController::compareLow);

        return new MultiScanController<R>(controllers);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanController<R> newScanController(RowPredicate predicate) {
        var ranges = mRanges;
        var controllers = new ScanController[ranges.length];

        for (int i=0; i<controllers.length; i++) {
            controllers[i] = ranges[i].newScanController(predicate);
        }

        Arrays.sort(controllers, ScanController::compareLow);

        return new MultiScanController<R>(controllers);
    }
}
