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

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.core.RowPredicate;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see DisjointUnionScanControllerFactory
 */
final class DisjointUnionScanController<R> implements ScanController<R> {
    private final ScanController<R>[] mControllers;

    private int mPosition;

    DisjointUnionScanController(ScanController<R>[] controllers) {
        mControllers = controllers;
    }

    @Override
    public RowPredicate<R> predicate() {
        return mControllers[0].predicate();
    }

    @Override
    public Cursor newCursor(View view, Transaction txn) throws IOException {
        return mControllers[mPosition].newCursor(view, txn);
    }

    @Override
    public RowDecoderEncoder<R> decoder() {
        return mControllers[mPosition].decoder();
    }

    @Override
    public boolean next() {
        return ++mPosition < mControllers.length;
    }
}
