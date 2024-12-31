/*
 *  Copyright (C) 2021 Cojen.org
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

import org.cojen.tupl.Transaction;

import org.cojen.tupl.util.WideLatch;

/**
 * Defines the single main trigger that a table can have. A trigger implementation is expected
 * to have several sub-tasks, and it might also provide a way to quickly add and remove
 * sub-tasks. Adding and removing sub-tasks can have race conditions, and so a copy-on-write
 * approach which replaces the trigger all at once might be safer.
 *
 * @author Brian S O'Neill
 * @see StoredTable#setTrigger
 */
public class Trigger<R> extends WideLatch {
    public static final int ACTIVE = 0, SKIP = 1, DISABLED = 2;

    // Set by StoredTable.
    int mMode;

    /**
     * Called after a row has been locked, but before the row has been marked clean. By
     * default, this method always throws an exception.
     *
     * @param txn never null, although it can be BOGUS
     * @param row never null, all columns are set
     * @param key never null
     * @param oldValue never null
     * @param newValue never null
     */
    public void store(Transaction txn, R row, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called after a row has been locked, but before the row has been marked undirty. This
     * variant supports a partially filled in row, when called from the "update" method or from
     * an updater which only has partial rows. The oldValue and newValue are fully specified,
     * but the row object remains partial. When the "merge" method is called (or any other), or
     * when all columns are dirty, the regular trigger store method is called with a fully
     * populated row. By default, this method always throws an exception.
     *
     * @param txn never null, although it can be BOGUS
     * @param row never null
     * @param key never null
     * @param oldValue never null
     * @param newValue never null
     */
    public void storeP(Transaction txn, R row, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called after a row has been locked, but before the row has been marked clean. By
     * default, this method always throws an exception.
     *
     * @param txn never null, although it can be BOGUS
     * @param row never null, all columns are set
     * @param key never null
     * @param newValue never null
     */
    public void insert(Transaction txn, R row, byte[] key, byte[] newValue) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Variant which supports partial rows.
     *
     * @param txn never null, although it can be BOGUS
     * @param row never null
     * @param key never null
     * @param newValue never null
     */
    public void insertP(Transaction txn, R row, byte[] key, byte[] newValue) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Called after a row has been locked, but before the row has been marked clean. By
     * default, this method always throws an exception.
     *
     * @param txn never null, although it can be BOGUS
     * @param row never null, all key columns are set
     * @param key never null
     * @param oldValue never null
     */
    public void delete(Transaction txn, R row, byte[] key, byte[] oldValue) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Variant which is called when no row object is available.
     */
    public void delete(Transaction txn, byte[] key, byte[] oldValue) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * If active, then must call the store or update method. If skip, then don't call a trigger
     * method, but still hold the shared lock for the whole operation. If disabled, then
     * release the shared lock and retry with the latest trigger instance.
     */
    public int mode() {
        return mMode;
    }

    /**
     * Disables this trigger and waits. Called by StoredTable at most once.
     */
    final void disable() {
        // Note that mode field can be assigned using "plain" mode because lock acquisition
        // applies a volatile fence.
        mMode = Trigger.DISABLED;

        // Wait for in-flight operations to finish.
        acquireExclusive();
        releaseExclusive();

        // At this point, any threads which acquire the shared lock on old trigger will observe
        // that it's disabled by virtue of having applied a volatile fence to obtain the lock
        // in the first place.

        notifyDisabled();
    }

    /**
     * Called after trigger has been disabled and isn't being used anymore. Implementation
     * should return quickly or else run any tasks in a separate thread.
     */
    protected void notifyDisabled() {
    }
}
