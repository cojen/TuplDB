/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.IOException;

import java.util.Comparator;

import java.util.function.Predicate;

import java.util.stream.Stream;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.RowSpliterator;

/**
 * Defines a relational collection of persistent rows. A row is defined by an interface
 * consisting of accessor/mutator methods corresponding to each column:
 *
 * <pre>
 * &#64;PrimaryKey("id")
 * public interface MyRow {
 *     long id();
 *     void id(long id);
 *
 *     String name();
 *     void name(String str);
 *
 *     String message();
 *     void message(String str);
 * }
 * </pre>
 *
 * <p>Supported column types:
 * <ul>
 * <li>Simple objects — {@code String}, {@code BigInteger}, and {@code BigDecimal}
 * <li>Primitives — {@code int}, {@code double}, etc
 * <li>Boxed primitives — {@code Integer}, {@code Double}, etc
 * <li>Primitive arrays — {@code int[]}, {@code double[]}, etc
 * </ul>
 *
 * By default, object columns cannot be set to null, and attempting to do so causes an {@code
 * IllegalArgumentException} to be thrown. The column definition must be annotated with {@link
 * Nullable @Nullable} to support nulls.
 *
 * <p>The actual row implementation class is generated at runtime, and the standard {@code
 * equals}, {@code hashCode}, {@code toString}, and {@code clone} methods are automatically
 * generated as well. If the row interface declares a {@code clone} method which returns the
 * exact row type, then the row can be cloned without requiring an explicit cast. If the row
 * interface extends {@link Comparable}, then rows are comparable by primary key. Any default
 * methods defined in the row interface are never overridden by the generated class, unless the
 * method is defined in the {@link Object} class.
 *
 * <p>Scans over the rows of the table can be reduced by a filter, described by this syntax:
 *
 * <blockquote><pre>{@code
 * Filter       = RowFilter
 *              | Projection [ ':' RowFilter ]
 * RowFilter    = AndFilter { "||" AndFilter }
 * AndFilter    = EntityFilter { "&&" EntityFilter }
 * EntityFilter = ColumnFilter | ParenFilter
 * ParenFilter  = [ "!" ] "(" RowFilter ")"
 * ColumnFilter = ColumnName RelOp ( ArgRef | ColumnName )
 *              | ColumnName "in" ArgRef
 * RelOp        = "==" | "!=" | ">=" | "<" | "<=" | ">"
 * Projection   = [ "~" ] "{" Columns "}"
 * Columns      = [ ColumnName { "," ColumnName } ]
 * ColumnName   = string
 * ArgRef       = "?" [ uint ]
 * }</pre></blockquote>
 *
 * @author Brian S O'Neill
 * @see Database#openTable Database.openTable
 * @see PrimaryKey
 */
public interface Table<R> {
    /**
     * Returns the interface which defines the rows of this table.
     */
    public Class<R> rowType();

    /**
     * Returns a new row instance with unset columns.
     */
    public R newRow();

    /**
     * Returns a new row instance which is an exact copy of the given row.
     */
    public R cloneRow(R row);

    /**
     * Resets the state of the given row such that all columns are unset.
     */
    public void unsetRow(R row);

    /**
     * Copies all columns and states from one row to another.
     */
    public void copyRow(R from, R to);

    /**
     * Returns a new scanner for all rows of this table.
     *
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     * @see #queryPlan queryPlan
     */
    public RowScanner<R> newRowScanner(Transaction txn) throws IOException;

    /**
     * Returns a new scanner for a subset of rows of this table, as specified by the filter
     * expression.
     *
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table accepted by the filter
     * @throws IllegalStateException if transaction belongs to another database instance
     * @see #queryPlan queryPlan
     */
    public RowScanner<R> newRowScanner(Transaction txn, String filter, Object... args)
        throws IOException;

    /**
     * Returns a new updater for all rows of this table.
     *
     * <p>When providing a transaction which acquires locks (or the transaction is null),
     * upgradable locks are acquired for each row visited by the updater. If the transaction
     * lock mode is non-repeatable, any lock acquisitions for rows which are stepped over are
     * released when moving to the next row. Updates with a null transaction are auto-committed
     * and become visible to other transactions as the updater moves along.
     *
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @return a new updater positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public RowUpdater<R> newRowUpdater(Transaction txn) throws IOException;

    /**
     * Returns a new updater for a subset of rows of this table, as specified by the filter
     * expression.
     *
     * <p>When providing a transaction which acquires locks (or the transaction is null),
     * upgradable locks are acquired for each row visited by the updater. If the transaction
     * lock mode is non-repeatable, any lock acquisitions for rows which are stepped over are
     * released when moving to the next row. Updates with a null transaction are auto-committed
     * and become visible to other transactions as the updater moves along.
     *
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @return a new updater positioned at the first row in the table accepted by the filter
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public RowUpdater<R> newRowUpdater(Transaction txn, String filter, Object... args)
        throws IOException;

    /**
     * Returns a new stream for all rows of this table. The stream must be explicitly closed
     * when no longer used, or else it must be used with a try-with-resources statement. If an
     * underlying {@code IOException} is generated, it's thrown as if it was unchecked.
     *
     * @param txn optional transaction for the stream to use; pass null for auto-commit mode
     * @return a new stream positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Stream<R> newStream(Transaction txn) {
        try {
            return RowSpliterator.newStream(newRowScanner(txn));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a new stream for a subset of rows of this table, as specified by the filter
     * expression. The stream must be explicitly closed when no longer used, or else it must be
     * used with a try-with-resources statement. If an underlying {@code IOException} is
     * generated, it's thrown as if it was unchecked.
     *
     * @param txn optional transaction for the stream to use; pass null for auto-commit mode
     * @return a new stream positioned at the first row in the table accepted by the filter
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Stream<R> newStream(Transaction txn, String filter, Object... args) {
        try {
            return RowSpliterator.newStream(newRowScanner(txn, filter, args));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a new transaction which is compatible with this table. If the provided durability
     * mode is null, a default mode is selected.
     */
    public Transaction newTransaction(DurabilityMode durabilityMode);

    /**
     * Non-transactionally determines if the table has nothing in it. A return value of true
     * guarantees that the table is empty, but false negatives are possible.
     */
    public boolean isEmpty() throws IOException;

    /**
     * Fully loads the row by primary key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     */
    public boolean load(Transaction txn, R row) throws IOException;

    /**
     * Checks if a row exists by searching against the primary key. This method should be
     * called only if the row doesn't need to be loaded or stored &mdash; calling exists and
     * then calling a load or store method is typically less efficient than skipping the exists
     * check entirely.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     */
    public boolean exists(Transaction txn, R row) throws IOException;

    /**
     * Unconditionally stores the given row, potentially replacing a corresponding row which
     * already exists.
     *
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public void store(Transaction txn, R row) throws IOException;

    /**
     * Unconditionally stores the given row, potentially replacing a corresponding row which
     * already exists.
     *
     * @return a copy of the replaced row, or null if none existed
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public R exchange(Transaction txn, R row) throws IOException;

    /**
     * Stores the given row when a corresponding row doesn't exist.
     *
     * @return false if a corresponding row already exists and nothing was inserted
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public boolean insert(Transaction txn, R row) throws IOException;

    /**
     * Stores the given row when a corresponding row already exists.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public boolean replace(Transaction txn, R row) throws IOException;

    /**
     * Updates an existing row with the modified columns of the given row, but the resulting
     * row isn't loaded back.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public boolean update(Transaction txn, R row) throws IOException;

    /**
     * Updates an existing row with the modified columns of the given row, and then loads the
     * result back into the given row.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public boolean merge(Transaction txn, R row) throws IOException;

    /**
     * Unconditionally removes an existing row by primary key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     */
    public boolean delete(Transaction txn, R row) throws IOException;

    /**
     * Returns a row comparator based on the given specification, which defines the ordering
     * columns. Each column name is prefixed with '+' or '-', to indicate ascending or
     * descending order. For example: {@code "+lastName+firstName-birthdate"}. By default,
     * nulls are treated as higher non-nulls, but a '!' after the '+'/'-' character causes
     * nulls to be treated as lower than non-nulls.
     *
     * @throws IllegalArgumentException if the specification is malformed
     * @throws IllegalStateException if the specification refers to non-existent columns
     */
    public Comparator<R> comparator(String spec);

    /**
     * Returns a row predicate for the given filter expression and arguments.
     */
    public Predicate<R> predicate(String filter, Object... args);

    /**
     * Returns a view of this table which doesn't perform automatic index selection.
     */
    public Table<R> viewPrimaryKey();

    /**
     * Returns a view of this table where the primary key is specified by the columns of an
     * alternate key, and the row is fully resolved by joining to the primary table. Direct
     * stores against the returned table aren't permitted, and an {@link
     * UnmodifiableViewException} is thrown when attempting to do so. Modifications are
     * permitted when using a {@link RowUpdater}.
     *
     * @param columns column specifications for the alternate key
     * @return alternate key as a table
     * @throws IllegalStateException if alternate key wasn't found
     */
    public Table<R> viewAlternateKey(String... columns) throws IOException;

    /**
     * Returns a view of this table where the primary key is specified by the columns of a
     * secondary index, and the row is fully resolved by joining to the primary table. Direct
     * stores against the returned table aren't permitted, and an {@link
     * UnmodifiableViewException} is thrown when attempting to do so. Modifications are
     * permitted when using a {@link RowUpdater}.
     *
     * @param columns column specifications for the secondary index
     * @return secondary index as a table
     * @throws IllegalStateException if secondary index wasn't found
     */
    public Table<R> viewSecondaryIndex(String... columns) throws IOException;

    /**
     * Returns a direct view of an alternate key or secondary index, in the form of an
     * unmodifiable table. The rows of the table only contain the columns of the alternate key
     * or secondary index.
     *
     * @return an unjoined table, or else this table if it's not joined
     */
    public Table<R> viewUnjoined();

    //public Table<R> viewOrderBy(String... columns);

    //public Table<R> viewFiltered(String filter, Object... args);

    //public Table<R> viewUnmodifiable();

    //public boolean isUnmodifiable();

    /**
     * Returns a query plan used by {@link #newRowScanner(Transaction, String, Object...)
     * newRowScanner} et al.
     *
     * @param txn optional transaction to be used; pass null for auto-commit mode
     * @param filter optional filter expression
     * @param args optional filter arguments
     */
    public QueryPlan queryPlan(Transaction txn, String filter, Object... args) throws IOException;
}
