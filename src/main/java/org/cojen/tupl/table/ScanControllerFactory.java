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

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface ScanControllerFactory<R> {
    int argumentCount();

    /**
     * Returns true if the ScanController attempts to load exactly one row.
     */
    default boolean loadsOne() {
        return false;
    }

    QueryPlan plan(Object... args);

    /**
     * If loadsOne returns true, call this method to obtain the plan for it.
     */
    default QueryPlan loadOnePlan(Object... args) {
        return plan(args);
    }

    ScanControllerFactory<R> reverse();

    /**
     * Returns a predicate which is shared by all scan batches.
     */
    RowPredicate<R> predicate(Object... args);

    /**
     * Returns Spliterator characteristics which is shared by all scan batches.
     */
    int characteristics();

    /**
     * Return a ScanController which constructs a RowPredicate from the given filter arguments.
     */
    ScanController<R> scanController(Object... args);

    /**
     * Return a ScanController which references a RowPredicate as constructed by the first batch.
     */
    default ScanController<R> scanController(RowPredicate<R> predicate) {
        throw new UnsupportedOperationException();
    }
}
