/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import org.cojen.tupl.Table;

import org.cojen.tupl.table.RowUtils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public interface TableProvider<R> {
    Class<R> rowType();

    /**
     * Returns the minimum amount of arguments which must be passed to the {@link #table}
     * method.
     */
    int argumentCount();

    /**
     * Returns a fully functional table.
     *
     * @throws IllegalArgumentException if not enough arguments are given
     */
    Table<R> table(Object... args);

    default Table<R> table() {
        return table(RowUtils.NO_ARGS);
    }

    /**
     * Returns a TableProvider which just wraps a Table.
     */
    public static <R> TableProvider<R> make(Table<R> table) {
        return new TableProvider<R>() {
            @Override
            public final Class<R> rowType() {
                return table.rowType();
            }

            @Override
            public final int argumentCount() {
                return 0;
            }

            @Override
            public final Table<R> table(Object... args) {
                return table;
            }
        };
    }

    public static abstract class Wrapped<R> implements TableProvider<R> {
        protected final TableProvider<R> source;
        protected final int argCount;

        protected Wrapped(TableProvider<R> source, int argCount) {
            this.source = source;
            this.argCount = argCount;
        }

        @Override
        public Class<R> rowType() {
            return source.rowType();
        }

        @Override
        public final int argumentCount() {
            return argCount;
        }

        protected final int checkArgumentCount(Object... args) {
            int argCount = this.argCount;
            if (args.length < argCount) {
                throw new IllegalArgumentException("Not enough query arguments provided");
            }
            return argCount;
        }
    }
}
