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

package org.cojen.tupl.table.join;

import java.io.IOException;

import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.QueryLauncher;

/**
 * @author Brian S. O'Neill
 * @see JoinQueryLauncherMaker
 */
public abstract class JoinQueryLauncher<R> extends QueryLauncher<R> {
    @Override
    public final Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    public final QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        throw new UnmodifiableViewException();
    }

    @Override
    protected final void closeIndexes() {
        // Nothing to do.
    }

    @Override
    protected final void clearCache() {
        // Nothing to do.
    }
}
