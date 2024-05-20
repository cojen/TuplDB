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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

import static java.util.Spliterator.*;

/**
 * Scan of nothing.
 *
 * @author Brian S O'Neill
 */
final class EmptyScanController extends SingleScanController implements ScanControllerFactory {
    private static final EmptyScanController THE = new EmptyScanController();
    private static RowPredicate cPredicate;

    @SuppressWarnings("unchecked")
    static <R> ScanControllerFactory<R> factory() {
        return THE;
    }

    private EmptyScanController() {
        super(false, EMPTY, false, null, false);
    }

    @Override
    public int argumentCount() {
        return 0;
    }

    @Override
    public QueryPlan plan(Object... args) {
        return new QueryPlan.Empty();
    }

    @Override
    public ScanControllerFactory reverse() {
        return this;
    }

    @Override
    public RowPredicate predicate() {
        RowPredicate predicate = cPredicate;
        if (predicate == null) {
            cPredicate = predicate = new RowPredicate.None();
        }
        return predicate;
    }

    @Override
    public RowPredicate predicate(Object... args) {
        return predicate();
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | IMMUTABLE | SIZED | DISTINCT;
    }

    @Override
    public ScanController scanController(Object... args) {
        return THE;
    }

    @Override
    public long evolvableTableId() {
        return 0;
    }

    @Override
    public byte[] secondaryDescriptor() {
        return null;
    }

    @Override
    public Object evalRow(Cursor c, LockResult result, Object row) {
        throw new AssertionError();
    }

    @Override
    public Object decodeRow(Object row, byte[] key, byte[] value) {
        throw new AssertionError();
    }

    @Override
    public void writeRow(RowWriter writer, byte[] key, byte[] value) {
        throw new AssertionError();
    }

    @Override
    public byte[] updateKey(Object row, byte[] original) {
        throw new AssertionError();
    }

    @Override
    public byte[] updateValue(Object row, byte[] original) {
        throw new AssertionError();
    }
}
