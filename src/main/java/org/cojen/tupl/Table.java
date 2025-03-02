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

import java.io.Closeable;
import java.io.IOException;

import java.util.Comparator;

import java.util.function.Predicate;

import java.util.stream.Stream;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.AggregatedTable;
import org.cojen.tupl.table.CommonRowTypeMaker;
import org.cojen.tupl.table.ComparatorMaker;
import org.cojen.tupl.table.ConcatTable;
import org.cojen.tupl.table.EmptyTable;
import org.cojen.tupl.table.GroupedTable;
import org.cojen.tupl.table.JoinIdentityTable;
import org.cojen.tupl.table.MappedTable;
import org.cojen.tupl.table.MapperMaker;
import org.cojen.tupl.table.PlainPredicateMaker;

import org.cojen.tupl.table.join.JoinTableMaker;

import org.cojen.tupl.table.RowUtils;

import static org.cojen.tupl.table.RowUtils.NO_ARGS;

/**
 * Defines a relational collection of persistent rows. A row is defined by an interface
 * consisting of accessor/mutator methods corresponding to each column:
 *
 * {@snippet lang="java" :
 * @PrimaryKey("id")
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
 * }
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
 * <p>Scans over the rows of the table can be reduced by a query, described by this syntax:
 *
 * <blockquote><pre>{@code
 * Query        = RowFilter
 *              | Projection [ RowFilter ]
 * RowFilter    = AndFilter { "||" AndFilter }
 * AndFilter    = EntityFilter { "&&" EntityFilter }
 * EntityFilter = ColumnFilter | ParenFilter
 * ParenFilter  = [ "!" ] "(" [ RowFilter ] ")"
 * ColumnFilter = ColumnName RelOp ( ArgRef | ColumnName )
 *              | ColumnName "in" ArgRef
 *              | ArgRef RelOp ColumnName
 * RelOp        = "==" | "!=" | ">=" | "<" | "<=" | ">"
 * Projection   = "{" ProjColumns "}"
 * ProjColumns  = [ ProjColumn { "," ProjColumn } ]
 * ProjColumn   = ( ( ( ( "+" | "-" ) [ "!" ] ) | "~" ) ColumnName ) | "*"
 * ColumnName   = string
 * ArgRef       = "?" [ uint ]
 * }</pre></blockquote>
 *
 * Note that a query projection specifies the minimum set of requested columns, but additional
 * ones might be provided if they were needed by the query implementation.
 *
 * @author Brian S O'Neill
 * @see Database#openTable Database.openTable
 * @see PrimaryKey
 */
public interface Table<R> extends Closeable {
    /**
     * Returns true if this table has a primary key defined, as specified by the {@link rowType
     * row type}.
     */
    public boolean hasPrimaryKey();

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
     * Sets columns which have a dirty state to clean. All unset columns remain unset.
     */
    public void cleanRow(R row);

    /**
     * Copies all columns and states from one row to another.
     */
    public void copyRow(R from, R to);

    /**
     * Returns true if the given row column is set.
     *
     * @throws IllegalArgumentException if column is unknown
     */
    public boolean isSet(R row, String name);

    /**
     * For the given row, performs an action for each column which is set.
     */
    public void forEach(R row, ColumnProcessor<? super R> action);

    /**
     * Returns a new scanner for all rows of this table.
     *
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Scanner<R> newScanner(Transaction txn) throws IOException {
        return newScanner(null, txn);
    }

    /**
     * Returns a new scanner for all rows of this table.
     *
     * @param row row instance for the scanner to use; pass null to create a new instance
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public Scanner<R> newScanner(R row, Transaction txn) throws IOException;

    /**
     * Returns a new scanner for a subset of rows from this table, as specified by the query
     * expression.
     *
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Scanner<R> newScanner(Transaction txn, String query, Object... args)
        throws IOException
    {
        return newScanner(null, txn, query, args);
    }

    /**
     * @hidden
     */
    public default Scanner<R> newScanner(Transaction txn, String query) throws IOException {
        return newScanner(txn, query, NO_ARGS);
    }

    /**
     * Returns a new scanner for a subset of rows from this table, as specified by the query
     * expression.
     *
     * @param row row instance for the scanner to use; pass null to create a new instance
     * @param txn optional transaction for the scanner to use; pass null for auto-commit mode
     * @return a new scanner positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Scanner<R> newScanner(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return query(query).newScanner(row, txn, args);
    }

    /**
     * @hidden
     */
    public default Scanner<R> newScanner(R row, Transaction txn, String query) throws IOException {
        return newScanner(row, txn, query, NO_ARGS);
    }

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
    public default Updater<R> newUpdater(Transaction txn) throws IOException {
        return newUpdater(null, txn);
    }

    /**
     * Returns a new updater for all rows of this table.
     *
     * <p>When providing a transaction which acquires locks (or the transaction is null),
     * upgradable locks are acquired for each row visited by the updater. If the transaction
     * lock mode is non-repeatable, any lock acquisitions for rows which are stepped over are
     * released when moving to the next row. Updates with a null transaction are auto-committed
     * and become visible to other transactions as the updater moves along.
     *
     * @param row row instance for the updater to use; pass null to create a new instance
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @return a new updater positioned at the first row in the table
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Updater<R> newUpdater(R row, Transaction txn) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Returns a new updater for a subset of rows from this table, as specified by the query
     * expression.
     *
     * <p>When providing a transaction which acquires locks (or the transaction is null),
     * upgradable locks are acquired for each row visited by the updater. If the transaction
     * lock mode is non-repeatable, any lock acquisitions for rows which are stepped over are
     * released when moving to the next row. Updates with a null transaction are auto-committed
     * and become visible to other transactions as the updater moves along.
     *
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @return a new updater positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Updater<R> newUpdater(Transaction txn, String query, Object... args)
        throws IOException
    {
        return newUpdater(null, txn, query, args);
    }

    /**
     * @hidden
     */
    public default Updater<R> newUpdater(Transaction txn, String query) throws IOException {
        return newUpdater(txn, query, NO_ARGS);
    }

    /**
     * Returns a new updater for a subset of rows from this table, as specified by the query
     * expression.
     *
     * <p>When providing a transaction which acquires locks (or the transaction is null),
     * upgradable locks are acquired for each row visited by the updater. If the transaction
     * lock mode is non-repeatable, any lock acquisitions for rows which are stepped over are
     * released when moving to the next row. Updates with a null transaction are auto-committed
     * and become visible to other transactions as the updater moves along.
     *
     * @param row row instance for the updater to use; pass null to create a new instance
     * @param txn optional transaction for the updater to use; pass null for auto-commit mode
     * @return a new updater positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Updater<R> newUpdater(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return query(query).newUpdater(row, txn, args);
    }

    /**
     * @hidden
     */
    public default Updater<R> newUpdater(R row, Transaction txn, String query) throws IOException {
        return newUpdater(row, txn, query, NO_ARGS);
    }

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
            return RowUtils.newStream(newScanner(txn));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a new stream for a subset of rows from this table, as specified by the query
     * expression. The stream must be explicitly closed when no longer used, or else it must be
     * used with a try-with-resources statement. If an underlying {@code IOException} is
     * generated, it's thrown as if it was unchecked.
     *
     * @param txn optional transaction for the stream to use; pass null for auto-commit mode
     * @return a new stream positioned at the first row in the table accepted by the query
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default Stream<R> newStream(Transaction txn, String query, Object... args) {
        try {
            return RowUtils.newStream(newScanner(txn, query, args));
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @hidden
     */
    public default Stream<R> newStream(Transaction txn, String query) {
        return newStream(txn, query, NO_ARGS);
    }

    /**
     * Returns a query for a subset of rows from this table, as specified by the query
     * expression.
     */
    public Query<R> query(String query) throws IOException;

    /**
     * Returns a query for all rows of this table.
     */
    public default Query<R> queryAll() throws IOException {
        return query("{*}");
    }

    /**
     * Returns true if any rows exist in this table.
     *
     * @param txn optional transaction to use; pass null for auto-commit mode
     * @throws IllegalStateException if transaction belongs to another database instance
     * @see #isEmpty
     */
    public default boolean anyRows(Transaction txn) throws IOException {
        // TODO: Subclasses should provide an optimized implementation.
        return anyRows(null, txn);
    }

    /**
     * @hidden
     */
    public default boolean anyRows(R row, Transaction txn) throws IOException {
        // TODO: Subclasses should provide an optimized implementation.
        return anyRows(row, txn, "{}", NO_ARGS);
    }

    /**
     * Returns true if a subset of rows from this table exists, as specified by the query
     * expression.
     *
     * @param txn optional transaction to use; pass null for auto-commit mode
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default boolean anyRows(Transaction txn, String query, Object... args)
        throws IOException
    {
        // TODO: Subclasses should provide an optimized implementation.
        return anyRows(null, txn, query, args);
    }

    /**
     * @hidden
     */
    public default boolean anyRows(Transaction txn, String query) throws IOException {
        return anyRows(txn, query, NO_ARGS);
    }

    /**
     * Returns true if a subset of rows from this table exists, as specified by the query
     * expression.
     *
     * @param row row instance for the implementation to use; pass null to create a new
     * instance if necessary
     * @param txn optional transaction to use; pass null for auto-commit mode
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default boolean anyRows(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        // TODO: Subclasses should provide an optimized implementation.
        Scanner<R> s = newScanner(row, txn, query, args);
        boolean result = s.row() != null;
        s.close();
        return result;
    }

    /**
     * @hidden
     */
    public default boolean anyRows(R row, Transaction txn, String query) throws IOException {
        return anyRows(row, txn, query, NO_ARGS);
    }

    /**
     * Returns a new transaction which is compatible with this table. If the provided durability
     * mode is null, a default mode is selected.
     */
    public Transaction newTransaction(DurabilityMode durabilityMode);

    /**
     * Non-transactionally determines if the table has nothing in it. A return value of true
     * guarantees that the table is empty, but false negatives are possible.
     *
     * @see #anyRows(Transaction)
     */
    public boolean isEmpty() throws IOException;

    /**
     * Fully loads the row by primary key.
     *
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws NoSuchRowException if a corresponding row doesn't exist
     */
    public default void load(Transaction txn, R row) throws IOException {
        if (!tryLoad(txn, row)) {
            throw new NoSuchRowException();
        }
    }

    /**
     * Fully loads the row by primary key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     */
    public boolean tryLoad(Transaction txn, R row) throws IOException;

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
    public default void store(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Unconditionally stores the given row, potentially replacing a corresponding row which
     * already exists.
     *
     * @return a copy of the replaced row, or null if none existed
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default R exchange(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Stores the given row when a corresponding row doesn't exist.
     *
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting primary or alternate key exists
     */
    public default void insert(Transaction txn, R row) throws IOException {
        if (!tryInsert(txn, row)) {
            throw new UniqueConstraintException("Primary key");
        }
    }

    /**
     * Stores the given row when a corresponding row doesn't exist.
     *
     * @return false if a corresponding row already exists and nothing was inserted
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default boolean tryInsert(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Stores the given row when a corresponding row already exists.
     *
     * @throws IllegalStateException if any required columns aren't set
     * @throws NoSuchRowException if a corresponding row doesn't exist
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default void replace(Transaction txn, R row) throws IOException {
        if (!tryReplace(txn, row)) {
            throw new NoSuchRowException();
        }
    }

    /**
     * Stores the given row when a corresponding row already exists.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if any required columns aren't set
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default boolean tryReplace(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Updates an existing row with the modified columns of the given row, but the resulting
     * row isn't loaded back.
     *
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws NoSuchRowException if a corresponding row doesn't exist
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default void update(Transaction txn, R row) throws IOException {
        if (!tryUpdate(txn, row)) {
            throw new NoSuchRowException();
        }
    }

    /**
     * Updates an existing row with the modified columns of the given row, but the resulting
     * row isn't loaded back.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default boolean tryUpdate(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Updates an existing row with the modified columns of the given row, and then loads the
     * result back into the given row.
     *
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws NoSuchRowException if a corresponding row doesn't exist
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default void merge(Transaction txn, R row) throws IOException {
        if (!tryMerge(txn, row)) {
            throw new NoSuchRowException();
        }
    }

    /**
     * Updates an existing row with the modified columns of the given row, and then loads the
     * result back into the given row.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws UniqueConstraintException if a conflicting alternate key exists
     */
    public default boolean tryMerge(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Unconditionally removes an existing row by primary key.
     *
     * @throws IllegalStateException if primary key isn't fully specified
     * @throws NoSuchRowException if a corresponding row doesn't exist
     */
    public default void delete(Transaction txn, R row) throws IOException {
        if (!tryDelete(txn, row)) {
            throw new NoSuchRowException();
        }
    }

    /**
     * Unconditionally removes an existing row by primary key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if primary key isn't fully specified
     */
    public default boolean tryDelete(Transaction txn, R row) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Returns a view backed by this table, whose rows have been mapped to target rows. The
     * returned table instance will throw a {@link ViewConstraintException} for operations
     * against rows not supported by the mapper, and closing the table has no effect.
     *
     * @throws NullPointerException if any parameter is null
     */
    public default <T> Table<T> map(Class<T> targetType, Mapper<R, T> mapper) throws IOException {
        return MappedTable.map(this, targetType, mapper);
    }

    /**
     * Returns a view backed by this table, whose rows have been mapped to target rows,
     * applying any necessary column value conversions. The returned table instance will throw
     * a {@link ConversionException} for operations against rows which cannot be converted back
     * source rows, and closing the table has no effect.
     *
     * <p>Source column values are converted to target column values using a potentially lossy
     * conversion, which can cause numbers to be clamped to a narrower range, etc. Columns
     * which aren't mapped to the target are dropped, and columns which don't map from the
     * source are set to the most appropriate default value, preferably null.
     *
     * @param targetType target row type; the primary key is possibly ignored
     * @throws NullPointerException if any parameter is null
     */
    @SuppressWarnings("unchecked")
    public default <T> Table<T> map(Class<T> targetType) throws IOException {
        Class<R> sourceType = rowType();
        if (targetType == sourceType) {
            return (Table<T>) this;
        }
        return map(targetType, MapperMaker.make(sourceType, targetType));
    }

    /**
     * Returns a view backed by this table, consisting of aggregate rows, which are grouped by
     * the {@link PrimaryKey primary key} of the target type. The primary key columns must
     * exactly correspond to columns of this source table. If no primary key is defined, then
     * the resulting table has one row, which is the aggregate result of all the rows of this
     * table. The view returned by this method is unmodifiable, and closing it has no effect.
     *
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if target primary key is malformed
     */
    public default <T> Table<T> aggregate(Class<T> targetType, Aggregator.Factory<R, T> factory)
        throws IOException
    {
        return AggregatedTable.aggregate(this, targetType, factory);
    }

    /**
     * Returns a view backed by this table, which processes groups of source rows into groups
     * of target rows. The view returned by this method is unmodifiable, closing it has no
     * effect, and a {@link ViewConstraintException} is thrown from operations which act upon a
     * primary key.
     *
     * @param groupBy grouping {@link #comparator specification}; pass an empty string to group
     * all source rows together
     * @param orderBy ordering specification within each group; pass an empty string if
     * undefined
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if groupBy or orderBy specification is malformed
     * @throws IllegalStateException if any groupBy or orderBy columns don't exist
     */
    public default <T> Table<T> group(String groupBy, String orderBy,
                                      Class<T> targetType, Grouper.Factory<R, T> factory)
        throws IOException
    {
        return GroupedTable.group(this, groupBy, orderBy, targetType, factory);
    }

    /**
     * Joins tables together into an unmodifiable view. The returned view doesn't have any
     * primary key, and so operations which act upon one aren't supported. In addition, closing
     * the view doesn't have any effect.
     *
     * <p>The {@code joinType} parameter is a class which resembles an ordinary row definition
     * except that all columns must refer to other row types. Any annotations which define keys
     * and indexes are ignored.
     *
     * <p>The join specification consists of each join column, separated by a join operator,
     * and possibly grouped with parenthesis:
     *
     * <blockquote><pre>{@code
     * JoinOp = Source { Type Source }
     * Source = Column | Group
     * Group  = "(" JoinOp ")"
     * Column = string
     * Type   = ":" | ">:" | ":<" | ">:<" | ">" | "<" | "><"
     *
     * a : b    inner join
     * a >: b   left outer join
     * a :< b   right outer join
     * a >:< b  full outer join
     * a > b    left anti join
     * a < b    right anti join
     * a >< b   full anti join
     * }</pre></blockquote>
     *
     * <p>In order for a requested join type to work correctly, a suitable join filter must be
     * provided by a query. Scanning all rows of the table without a join filter results in a
     * <em>cross join</em>.
     *
     * @param spec join specification
     * @throws NullPointerException if any parameters are null
     * @throws IllegalArgumentException if join type is malformed, or if the specification is
     * malformed, or if there are any table matching issues
     * @see Database#openJoinTable
     */
    public static <J> Table<J> join(Class<J> joinType, String spec, Table<?>... tables)
        throws IOException
    {
        return JoinTableMaker.join(joinType, spec, tables);
    }

    /**
     * Joins tables together into a generated join type class.
     *
     * @param spec join specification
     * @throws NullPointerException if any parameters are null
     * @throws IllegalArgumentException if the specification is malformed, or if there are any
     * table matching issues
     * @see #join(Class,String,Table...)
     */
    public static Table<Row> join(String spec, Table<?>... tables) throws IOException {
        return JoinTableMaker.join(spec, tables);
    }

    /**
     * Returns an unmodifiable table consisting of one row with no columns, representing the
     * identity element when joining an empty set of tables. Calling {@link #derive derive}
     * against the join identity table can be used to perform arbitrary expression evaluation.
     */
    public static Table<Row> join() {
        return JoinIdentityTable.THE;
    }

    /**
     * Returns a view backed by this table, specified by a fully-featured query expression. The
     * returned table instance will throw a {@link ViewConstraintException} for operations
     * against rows which are restricted by the query, and closing the table has no effect.
     *
     * @param derivedType the projected query columns must correspond to columns defined in the
     * derived row type
     */
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException;

    /**
     * @hidden
     */
    public default <D> Table<D> derive(Class<D> derivedType, String query) throws IOException {
        return derive(derivedType, query, NO_ARGS);
    }

    /**
     * Returns a view backed by this table, specified by a fully-featured query expression. The
     * returned table instance will throw a {@link ViewConstraintException} for operations
     * against rows which are restricted by the query, and closing the table has no effect.
     */
    public Table<Row> derive(String query, Object... args) throws IOException;

    /**
     * @hidden
     */
    public default Table<Row> derive(String query) throws IOException {
        return derive(query, NO_ARGS);
    }

    /**
     * Returns a view consisting of all rows from the given source tables concatenated
     * together, possibly resulting in duplicate rows. All of the rows are mapped to the target
     * type, applying any necessary column value conversions. The returned table instance will
     * throw a {@link ConversionException} for operations against rows which cannot be
     * converted back source rows, and closing the table has no effect.
     *
     * @param targetType target row type; the primary key is possibly ignored
     * @param sources source tables to concatenate; if none are provided, then the returned
     * view is empty
     * @throws NullPointerException if any parameter is null
     * @see #map(Class)
     */
    public static <T> Table<T> concat(Class<T> targetType, Table<?>... sources)
        throws IOException
    {
        if (sources.length > 1) {
            return ConcatTable.concat(targetType, sources);
        } else if (sources.length == 1) {
            return sources[0].map(targetType);
        } else {
            return new EmptyTable<>(targetType);
        }
    }

    /**
     * Returns a view consisting of all rows from the given source tables concatenated
     * together, possibly resulting in duplicate rows. All of the rows are mapped to an
     * automatically selected target type (possibly generated), applying any necessary column
     * value conversions. The returned table instance will throw a {@link ConversionException}
     * for operations against rows which cannot be converted back source rows, and closing the
     * table has no effect.
     *
     * @param sources source tables to concatenate; if none are provided, then the returned
     * view is empty
     * @return a table whose row type primary key consists of all the source columns, but the
     * table doesn't actually have a primary key
     * @throws NullPointerException if any parameter is null
     * @see #map(Class)
     */
    public static Table<Row> concat(Table<?>... sources) throws IOException {
        if (sources.length > 1) {
            return ConcatTable.concat(CommonRowTypeMaker.makeFor(sources), sources);
        } else if (sources.length == 1) {
            return sources[0].map(CommonRowTypeMaker.makeFor(sources[0]));
        } else {
            return new EmptyTable<>(Row.class);
        }
    }

    /**
     * Returns a view of this table which has duplicate rows filtered out. If this table
     * doesn't have any duplicates, then it's simply returned as-is. If an actual view is
     * returned, then the instance is unmodifiable, and closing it has no effect.
     */
    public Table<R> distinct() throws IOException;

    /**
     * Returns a row comparator based on the given specification, which defines the ordering
     * columns. Each column name is prefixed with '+' or '-', to indicate ascending or
     * descending order. For example: {@code "+lastName+firstName-birthdate"}. By default,
     * nulls are treated as higher than non-nulls, but a '!' after the '+'/'-' character causes
     * nulls to be treated as lower than non-nulls.
     *
     * @throws IllegalArgumentException if the specification is malformed
     * @throws IllegalStateException if the specification refers to non-existent columns
     */
    public default Comparator<R> comparator(String spec) {
        return ComparatorMaker.comparator(rowType(), spec);
    }

    /**
     * Returns a row predicate for the given query expression and arguments.
     */
    public default Predicate<R> predicate(String query, Object... args) {
        return PlainPredicateMaker.predicate(rowType(), query, args);
    }

    /**
     * @hidden
     */
    public default Predicate<R> predicate(String query) {
        return predicate(query, NO_ARGS);
    }

    /**
     * @see Index#close
     */
    @Override
    public void close() throws IOException;

    public boolean isClosed();
}
