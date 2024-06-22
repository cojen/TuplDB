/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Interface for mapping source rows to target rows. Inverse mapping is optional, but it's
 * necessary for supporting modifiable views, and it's also used by the query optimizer.
 * Mapper implementations must be thread-safe.
 *
 * <p>For supporting inverse mapping, define a public static method for each target column
 * which can be mapped to a source column. The naming pattern must be {@code
 * <target_name>_to_<source_name>}, and the method must be a pure function. The parameter type
 * must exactly match the target column type, and the return type must exactly match the source
 * column type. If the function doesn't transform the column value, then it should be annotated
 * with {@link Untransformed @Untransformed}, since it helps with query optimization.
 *
 * @author Brian S O'Neill
 * @see Table#map Table.map
 */
public interface Mapper<R, T> {
    /**
     * Maps source rows to target rows.
     *
     * @param source never null
     * @param target never null; all columns are initially unset
     * @return null if filtered out
     */
    T map(R source, T target) throws IOException;

    /**
     * Returns true if the map method can filter out rows, which is true by default. If the map
     * method never performs filtering, then false should be returned to allow sort operations
     * to be pushed to the source table whenever possible.
     */
    default boolean performsFiltering() {
        return true;
    }

    /**
     * Checks if the given source row can be stored into the source table. By default, a {@code
     * ViewConstraintException} is always thrown.
     *
     * @param row all required columns are guaranteed to be set
     */
    default void checkStore(Table<R> table, R row) throws ViewConstraintException {
        throw new ViewConstraintException();
    }

    /**
     * Checks if the given source row can be updated into the source table. By default, a
     * {@code ViewConstraintException} is always thrown.
     *
     * @param row only the primary key columns are guaranteed to be set
     */
    default void checkUpdate(Table<R> table, R row) throws ViewConstraintException {
        throw new ViewConstraintException();
    }

    /**
     * Checks if the given source row can be deleted from the source table. By default, a
     * {@code ViewConstraintException} is always thrown.
     *
     * @param row only the primary key columns are guaranteed to be set
     */
    default void checkDelete(Table<R> table, R row) throws ViewConstraintException {
        throw new ViewConstraintException();
    }

    /**
     * Returns a comma-separated list of source columns which are needed by this {@code
     * Mapper}. Null is returned by default, which indicates that all columns are needed. The
     * implementation of this method must return a static constant.
     */
    default String sourceProjection() {
        return null;
    }

    /**
     * Override this method to customize the mapper's query plan.
     *
     * @param plan original plan
     * @return original or replacement plan
     */
    default QueryPlan plan(QueryPlan.Mapper plan) {
        return plan;
    }
}
