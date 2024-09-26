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

import java.io.Closeable;
import java.io.IOException;

import org.cojen.tupl.diag.QueryPlan;

/**
 * Interface which processes groups of rows into other groups of rows.
 *
 * @author Brian S. O'Neill
 * @see Table#group Table.group
 */
public interface Grouper<R, T> extends Closeable {
    /**
     * Is used to generate a new {@link Grouper} instance for every query against the target
     * table.
     *
     * <p>To help query optimization, inverse mapping functions should be provided. They're
     * defined as public static methods for each target column which can be mapped to a source
     * column. The naming pattern must be {@code <target_name>_to_<source_name>}, and the
     * method must be a pure function. The parameter type must exactly match the target column
     * type, and the return type must exactly match the source column type. If the function
     * doesn't transform the column value, then it should be annotated with {@link
     * Untransformed @Untransformed}.
     */
    public static interface Factory<R, T> {
        Grouper<R, T> newGrouper() throws IOException;

        /**
         * Returns a comma-separated list of source columns which are needed by the {@code
         * Grouper} instances. Null is returned by default, which indicates that all columns
         * are needed. The implementation of this method must return a static constant.
         */
        default String sourceProjection() {
            return null;
        }

        /**
         * Override this method to customize the grouper's query plan.
         *
         * @param plan original plan
         * @return original or replacement plan
         */
        default QueryPlan plan(QueryPlan.Grouper plan) {
            return plan;
        }
    }

    /**
     * Is called for the first source row in the group, and then the {@link #step step} method
     * is called.
     *
     * @param source never null
     * @return the next source row instance to use, or null if it was kept by the grouper
     */
    R begin(R source) throws IOException;

    /**
     * Is called for each source row in the group, other than the first one, and then the
     * {@link #step step} method is called.
     *
     * @param source never null
     * @return the next source row instance to use, or null if it was kept by the grouper
     */
    R accumulate(R source) throws IOException;

    /**
     * Is called after the last source row in the group has been provided, and then the {@link
     * #step step} method is called.
     */
    default void finished() throws IOException {
    }

    /**
     * Is called to produce the next target row. Returning null indicates that no target
     * rows remain, and that reading of source group rows can resume.
     *
     * @param target never null; all columns are initially unset
     * @return null if no target rows remain
     */
    T step(T target) throws IOException;

    /**
     * Is called when this {@code Grouper} instance is no longer needed.
     */
    @Override
    default void close() throws IOException {
    }
}
