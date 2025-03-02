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

import java.util.stream.Stream;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.RowUtils;

import static org.cojen.tupl.table.RowUtils.NO_ARGS;

/**
 * Represents a sharable object which performs a query against a table. Queries might require
 * additional arguments to be supplied, as required by the original query expression.
 *
 * @author Brian S. O'Neill
 * @see Table#query
 */
public interface Query<R> {
    /**
     * Returns the interface which defines the rows of this query.
     */
    Class<R> rowType();

    /**
     * Returns the minimum amount of arguments needed by this query.
     */
    int argumentCount();

    /**
     * Returns a new scanner for all the rows of this query.
     *
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @return a new scanner positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    default Scanner<R> newScanner(Transaction txn, Object... args) throws IOException {
        return newScanner(null, txn, args);
    }

    /**
     * @hidden
     */
    default Scanner<R> newScanner(Transaction txn) throws IOException {
        return newScanner(txn, NO_ARGS);
    }

    /**
     * Returns a new scanner for all the rows of this query.
     *
     * @param row row instance for the scanner to use; pass null to create a new instance
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @return a new scanner positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException;

    /**
     * @hidden
     */
    default Scanner<R> newScanner(R row, Transaction txn) throws IOException {
        return newScanner(row, txn, NO_ARGS);
    }

    /**
     * Returns a new updater for all the rows of this query.
     *
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @return a new updater positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    default Updater<R> newUpdater(Transaction txn, Object... args) throws IOException {
        return newUpdater(null, txn, args);
    }

    /**
     * @hidden
     */
    default Updater<R> newUpdater(Transaction txn) throws IOException {
        return newUpdater(txn, NO_ARGS);
    }

    /**
     * Returns a new updater for all the rows of this query.
     *
     * @param row row instance for the updater to use; pass null to create a new instance
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @return a new updater positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    default Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * @hidden
     */
    default Updater<R> newUpdater(R row, Transaction txn) throws IOException {
        return newUpdater(row, txn, NO_ARGS);
    }

    /**
     * Returns a new stream for all the rows of this query.
     *
     * @param txn optional transaction for the stream to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @return a new stream positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    default Stream<R> newStream(Transaction txn, Object... args) {
        try {
            return RowUtils.newStream(newScanner(txn, args));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @hidden
     */
    default Stream<R> newStream(Transaction txn) {
        return newStream(txn, NO_ARGS);
    }

    /**
     * Deletes all rows specified by this query.
     *
     * @param txn optional transaction to use; pass null for auto-commit mode against each row
     * @param args arguments required by this query
     * @return the amount of rows deleted
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    default long deleteAll(Transaction txn, Object... args) throws IOException {
        return RowUtils.deleteAll(this, txn, args);
    }

    /**
     * @hidden
     */
    default long deleteAll(Transaction txn) throws IOException {
        return deleteAll(txn, NO_ARGS);
    }

    /**
     * Returns true if this query produces any rows.
     *
     * @param txn optional transaction to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @throws IllegalStateException if transaction belongs to another database instance
     * @see Table#isEmpty
     */
    default boolean anyRows(Transaction txn, Object... args) throws IOException {
        // TODO: Subclasses should provide an optimized implementation.
        return anyRows(null, txn, args);
    }

    /**
     * @hidden
     */
    default boolean anyRows(Transaction txn) throws IOException {
        return anyRows(txn, NO_ARGS);
    }

    /**
     * Returns true if this query produces any rows.
     *
     * @param row row instance for the implementation to use; pass null to create a new
     * instance if necessary
     * @param txn optional transaction to use; pass null for auto-commit mode
     * @param args arguments required by this query
     * @throws IllegalStateException if transaction belongs to another database instance
     * @see Table#isEmpty
     */
    default boolean anyRows(R row, Transaction txn, Object... args) throws IOException {
        // TODO: Subclasses should provide an optimized implementation.
        Scanner<R> s = newScanner(row, txn, args);
        boolean result = s.row() != null;
        s.close();
        return result;
    }

    /**
     * @hidden
     */
    default boolean anyRows(R row, Transaction txn) throws IOException {
        return anyRows(row, txn, NO_ARGS);
    }

    /**
     * Returns a query plan used by {@link #newScanner(Transaction, Object...) newScanner}.
     *
     * @param txn optional transaction to be used; pass null for auto-commit mode
     * @param args optional query arguments
     */
    QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException;

    /**
     * @hidden
     */
    default QueryPlan scannerPlan(Transaction txn) throws IOException {
        return scannerPlan(txn, NO_ARGS);
    }

    /**
     * Returns a query plan used by {@link #newUpdater(Transaction, Object...) newUpdater}.
     *
     * @param txn optional transaction to be used; pass null for auto-commit mode
     * @param args optional query arguments
     */
    default QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * @hidden
     */
    default QueryPlan updaterPlan(Transaction txn) throws IOException {
        return updaterPlan(txn, NO_ARGS);
    }

    /**
     * Returns a query plan used by {@link #newStream(Transaction, Object...) newStream}.
     *
     * @param txn optional transaction to be used; pass null for auto-commit mode
     * @param args optional query arguments
     */
    default QueryPlan streamPlan(Transaction txn, Object... args) throws IOException {
        return scannerPlan(txn, args);
    }

    /**
     * @hidden
     */
    default QueryPlan streamPlan(Transaction txn) throws IOException {
        return streamPlan(txn, NO_ARGS);
    }
}
