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

package org.cojen.tupl.model;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.RowUtils;

/**
 * Interface which defines an arbitrary command which is expected to have side effects.
 *
 * @author Brian S. O'Neill
 * @see CommandNode
 */
public interface Command {
    /**
     * Returns the minimum amount of arguments which must be passed to the {@link #exec}
     * method.
     */
    int argumentCount();

    /**
     * @return the number of rows this command acted upon, or 0 if not applicable
     * @throws IllegalArgumentException if not enough arguments are given, or if a transaction
     * is provided but the command isn't transactional
     */
    long exec(Transaction txn, Object... args) throws IOException;

    /**
     * @return the number of rows this command acted upon, or 0 if not applicable
     * @throws IllegalArgumentException if not enough arguments are given, or if a transaction
     * is provided but the command isn't transactional
     */
    default long exec(Transaction txn) throws IOException {
        return exec(txn, RowUtils.NO_ARGS);
    }

    /**
     * Returns an optional QueryPlan for this command.
     */
    default QueryPlan plan(Transaction txn, Object... args) throws IOException {
        return null;
    }
}
