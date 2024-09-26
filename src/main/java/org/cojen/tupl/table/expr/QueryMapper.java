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

import org.cojen.tupl.Mapper;
import org.cojen.tupl.Table;

/**
 * @author Brian S. O'Neill
 * @see MappedQueryExpr
 */
public abstract class QueryMapper<R, T> implements Mapper<R, T> {
    /*
      Override the check methods to do nothing. This behavior is correct for an update
      statement, because it permits altering the row to appear outside the set of rows selected
      by the filter. This behavior is incorrect for a view, which disallows creating or
      altering rows such that they appear outside the view's bounds. A view needs to check
      another filter before allowing the operation to proceed.
    */

    @Override
    public final void checkStore(Table<R> table, R row) {
    }

    @Override
    public final void checkUpdate(Table<R> table, R row) {
    }

    @Override
    public final void checkDelete(Table<R> table, R row) {
    }

    /**
     * Factory method which returns a new or singleton Mapper instance.
     */
    public abstract Mapper<R, T> mapperFor(Object[] args);

    public static abstract class Unfiltered<R, T> extends QueryMapper<R, T> {
        @Override
        public final boolean performsFiltering() {
            return false;
        }
    }
}
