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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class QueryLauncher<R> implements Query<R> {
    /**
     * @param row initial row; can be null
     */
    @Override
    public abstract Scanner<R> newScanner(R row, Transaction txn, Object... args)
        throws IOException;

    /**
     * @param row initial row; can be null
     */
    @Override
    public abstract Updater<R> newUpdater(R row, Transaction txn, Object... args)
        throws IOException;

    /**
     * Scan and write rows to a remote endpoint.
     */
    public abstract void scanWrite(Transaction txn, RowWriter writer, Object... args)
        throws IOException;

    @Override
    public abstract QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException;

    @Override
    public abstract QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException;

    /**
     * Is called when the StoredTable is closed, in order to close all the secondary indexes
     * referenced by this query. This method must not be public.
     */
    protected abstract void closeIndexes() throws IOException;

    /**
     * Clears any cached state.
     */
    protected abstract void clearCache();
}
