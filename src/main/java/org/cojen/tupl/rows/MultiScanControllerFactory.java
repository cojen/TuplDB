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
    @SuppressWarnings("unchecked")
    public ScanController<R> newScanController(Object... args) {
        var ranges = mRanges;
        var controllers = new ScanController[ranges.length];
        for (int i=0; i<controllers.length; i++) {
            controllers[i] = ranges[i].newScanController(args);
        }

        Arrays.sort(controllers, ScanController::compareLow);

        return new MultiScanController<R>(controllers);
    }
}