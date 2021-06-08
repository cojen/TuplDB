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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RowView<R> {
    public Class<R> rowType();

    /**
     * Returns a new row instance with unset columns.
     */
    public R newRow();

    /**
     * Resets the state of the given row such that all columns are unset.
     */
    public void reset(R row);

    public RowScanner<R> newScanner(Transaction txn) throws IOException;

    public RowScanner<R> newScanner(Transaction txn, String filter, Object... args)
        throws IOException;

    public RowUpdater<R> newUpdater(Transaction txn) throws IOException;

    public RowUpdater<R> newUpdater(Transaction txn, String filter, Object... args)
        throws IOException;

    public Transaction newTransaction(DurabilityMode durabilityMode);

    public boolean isEmpty() throws IOException;

    /**
     * Fully loads the row by a primary or alternate key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    public boolean load(Transaction txn, R row) throws IOException;

    /**
     * Checks if a row exists by searching against a primary or alternate key. This method
     * should be called only if the row doesn't need to be loaded or stored &mdash; calling
     * exists and then calling a load or store method is typically less efficient than skipping
     * the exists check entirely.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    public boolean exists(Transaction txn, R row) throws IOException;

    /**
     * Unconditionally stores the given row, potentially replacing a corresponding row which
     * already exists.
     *
     * @throws IllegalStateException if any required columns aren't set
     */
    public void store(Transaction txn, R row) throws IOException;

    /**
     * Unconditionally stores the given row, potentially replacing a corresponding row which
     * already exists.
     *
     * @return a copy of the replaced row, or null if none existed
     * @throws IllegalStateException if any required columns aren't set
     */
    public R exchange(Transaction txn, R row) throws IOException;

    /**
     * Stores the given row when a corresponding row doesn't exist.
     *
     * @return false if a corresponding row already exists and nothing was inserted
     * @throws IllegalStateException if any required columns aren't set
     */
    public boolean insert(Transaction txn, R row) throws IOException;

    /**
     * Stores the given row when a corresponding row already exists.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if any required columns aren't set
     */
    public boolean replace(Transaction txn, R row) throws IOException;

    /**
     * Updates an existing row with the modified columns of the given row, but the resulting
     * row isn't loaded back.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    public boolean update(Transaction txn, R row) throws IOException;

    /**
     * Updates an existing row that matches the given criteria with the given modifications,
     * but the resulting row isn't loaded back. This method permits updating the primary key.
     *
     * @param match the row match criteria
     * @param row the row modifications to apply
     * @return false if a matching row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    //public boolean update(Transaction txn, R match, R row) throws IOException;

    /**
     * Updates an existing row with the modified columns of the given row, and then loads the
     * result back into the given row.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    public boolean merge(Transaction txn, R row) throws IOException;

    /**
     * Updates an existing row that matches the given criteria with the given modifications,
     * and then loads the result back into the given row. This method permits updating the
     * primary key.
     *
     * @param match the row match criteria
     * @param row the row modifications to apply, and also the target of the loaded result
     * @return false if a matching row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    //public boolean merge(Transaction txn, R match, R row) throws IOException;

    /**
     * Unconditionally removes an existing row by a primary or alternate key.
     *
     * @return false if a corresponding row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    public boolean delete(Transaction txn, R row) throws IOException;

    /**
     * Removes the given row by a primary or alternate key, also checking that all set columns
     * of the given row match to an existing row.
     *
     * @return false if a matching row doesn't exist
     * @throws IllegalStateException if no primary or alternate key is fully specified
     */
    //public boolean remove(Transaction txn, R match) throws IOException;

    /**
     * Returns a view which is filtered by the given expression and arguments.
     *
     * <blockquote><pre>{@code
     * RowFilter    = AndFilter { "|" AndFilter }
     * AndFilter    = EntityFilter { "&" EntityFilter }
     * EntityFilter = ColumnFilter | ParenFilter
     * ParenFilter  = [ "!" ] "(" Filter ")"
     * ColumnFilter = ColumnName RelOp ( ArgRef | ColumnName )
     *              | ColumnName "in" ArgRef
     * RelOp        = "==" | "!=" | "<" | ">=" | ">" | "<="
     * ColumnName   = string
     * ArgRef       = "?" [ uint ]
     * }</pre></blockquote>
     */
    // FIXME: A filtered view is too restrictive. It prevents RowUpdater from making changes to
    // rows when the change is part of the filter. Add this feature later. Initially support
    // passing a filter and args to the newScanner and newUpdater methods. Evolve the filter
    // specification such that it becomes a full query specification with select, order-by,
    // index hints, etc. However, it's the parameterized nature of the filter that makes it
    // special. How about thinking in terms of composability. It makes little sense to add
    // more view layers after performing projection, for example.
    //public RowView<R> viewFiltered(String filter, Object... args);

    // FIXME: viewOf? viewSelection? viewProjection? viewOnly? viewWith? viewWithout?
    // Projection.
    //public RowView<R> viewSelect(String... columns);

    //public RowView<R> viewReverse();

    //public RowView<R> viewUnmodifiable();

    //public boolean isUnmodifiable();
}
