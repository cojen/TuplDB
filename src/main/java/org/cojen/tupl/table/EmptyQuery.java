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

package org.cojen.tupl.table;

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class EmptyQuery<R> implements Query<R> {
    private final Class<R> mRowType;

    public EmptyQuery(Class<R> rowType) {
        mRowType = rowType;
    }

    @Override
    public Class<R> rowType() {
        return mRowType;
    }

    @Override
    public int argumentCount() {
        return 0;
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) {
        return EmptyScanner.the();
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) {
        return new QueryPlan.Empty();
    }
}
