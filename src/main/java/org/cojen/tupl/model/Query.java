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

package org.cojen.tupl.model;

import org.cojen.tupl.Table;

/**
 * Represents a factory for making tables or query result sets.
 *
 * @author Brian S. O'Neill
 * @see RelationNode
 */
public interface Query<R> {
    static final Object[] NO_ARGS = new Object[0];

    Class<R> rowType();

    /**
     * Returns the minimum amount of arguments which must be passed to the {@link asTable}
     * method.
     */
    int argumentCount();

    /**
     * Returns a fully functional table from this query.
     *
     * @throws IllegalArgumentException if not enough arguments are given
     */
    Table<R> asTable(Object... args);

    default Table<R> asTable() {
        return asTable(NO_ARGS);
    }

    // FIXME: Provide access to the projected column names, which can differ from the table
    // column names. Perhaps this should be a feature of Table?
    //   Map<String, String> columnLabels();

    /**
     * Returns a Query which just wraps a Table.
     */
    public static <R> Query<R> make(Table<R> table) {
        return new Query<>() {
            @Override
            public Class<R> rowType() {
                return table.rowType();
            }

            @Override
            public int argumentCount() {
                return 0;
            }

            @Override
            public Table<R> asTable(Object... args) {
                return table;
            }
        };
    }

    public static abstract class Wrapped implements Query {
        protected final Query mFromQuery;
        protected final int mArgCount;

        protected Wrapped(Query fromQuery, int argCount) {
            mFromQuery = fromQuery;
            mArgCount = argCount;
        }

        @Override
        public final Class rowType() {
            return mFromQuery.rowType();
        }

        @Override
        public final int argumentCount() {
            return mArgCount;
        }

        protected final int checkArgumentCount(Object... args) {
            int argCount = mArgCount;
            if (args.length < argCount) {
                throw new IllegalArgumentException("Not enough query arguments provided");
            }
            return argCount;
        }
    }
}
