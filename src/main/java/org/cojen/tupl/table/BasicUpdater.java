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

import java.util.Arrays;
import java.util.TreeSet;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Updater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;
import org.cojen.tupl.UnpositionedCursorException;

import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.views.ViewUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class BasicUpdater<R> extends BasicScanner<R> implements Updater<R> {
    TreeSet<byte[]> mKeysToSkip;

    BasicUpdater(StoredTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    public final R update(R row) throws IOException {
        updateCurrent();
        return doStep(row);
    }

    private void updateCurrent() throws IOException {
        try {
            R current = mRow;
            if (current == null) {
                throw new IllegalStateException("No current row");
            }
            doUpdateCurrent(current);
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (UniqueConstraintException e) {
            throw e;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }

        unlocked(); // prevent subclass from attempting to release the lock
    }

    private void doUpdateCurrent(R row) throws IOException {
        Cursor c = mCursor;

        byte[] key, value;
        {
            RowEvaluator<R> evaluator = mEvaluator;
            key = evaluator.updateKey(row, c.key());
            value = evaluator.updateValue(row, c.value());
        }

        int cmp;
        if (key == null || (cmp = c.compareKeyTo(key)) == 0) {
            // Key didn't change.
            doUpdateAsStore(c, value);
            return;
        }

        // This point is reached when the key changed, and so the update is out of sequence. A
        // new value is inserted (if permitted), and the current one is deleted. If the new key
        // is higher, it's added to a remembered set and not observed again by this updater.

        if (cmp < 0 && (!mController.predicate().testP(row, key, value) || !addKeyToSkip(key))) {
            // No need to remember it because the updated row is filtered out, or don't try
            // removing it from the set in case of UniqueConstraintException.
            cmp = 0;
        }

        try {
            doUpdateAsDeleteInsert(row, c, key, value);
        } catch (UniqueConstraintException e) {
            if (cmp < 0) {
                mKeysToSkip.remove(key);
            }
            throw e;
        }
    }

    boolean addKeyToSkip(byte[] key) {
        if (mKeysToSkip == null) {
            // TODO: Consider designing a more memory efficient set or hashtable.
            mKeysToSkip = new TreeSet<>(RowUtils.KEY_COMPARATOR);
        }
        // FIXME: For AutoCommitUpdater, consider limiting the size of the set and
        // use a temporary index. All other updaters maintain locks, and so the key
        // objects cannot be immediately freed anyhow.
        return mKeysToSkip.add(key);
    }

    /**
     * Called when the primary key didn't change.
     */
    private void doUpdateAsStore(Cursor c, byte[] value) throws IOException {
        Trigger<R> trigger = mTable.getTrigger();

        if (trigger == null) {
            storeValue(c, value);
            return;
        }

        while (true) {
            trigger.acquireShared();
            try {
                int mode = trigger.mode();
                if (mode == Trigger.SKIP) {
                    storeValue(c, value);
                    return;
                }
                if (mode != Trigger.DISABLED) {
                    storeValue(trigger, mRow, c, value);
                    return;
                }
            } finally {
                trigger.releaseShared();
            }
            trigger = mTable.trigger();
        }
    }

    /**
     * Called when the primary key changed.
     *
     * @param c positioned at key to delete; position isn't changed
     * @param key key to insert
     * @param value value to insert
     * @throws UniqueConstraintException
     */
    private void doUpdateAsDeleteInsert(R row, Cursor c, byte[] key, byte[] value)
        throws IOException
    {
        Index source = mTable.mSource;

        Transaction txn = ViewUtils.enterScope(source, c.link());
        try {
            Trigger<R> trigger = mTable.getTrigger();
            if (trigger != null) while (true) {
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();
                    if (mode == Trigger.SKIP) {
                        break;
                    }
                    if (mode != Trigger.DISABLED) {
                        byte[] oldValue = c.value();
                        if (oldValue != null) {
                            // Don't pass the row because the key columns were modified.
                            trigger.delete(txn, c.key(), oldValue);
                        }
                        trigger.insertP(txn, row, key, value);
                        break;
                    }
                } finally {
                    trigger.releaseShared();
                }
                trigger = mTable.trigger();
            }

            boolean result;
            RowPredicateLock<R> lock = mTable.mIndexLock;
            lock.redoPredicateMode(txn);
            try (RowPredicateLock.Closer closer = lock.openAcquireP(txn, row, key, value)) {
                result = source.insert(txn, key, value);
            }

            if (!result) {
                throw new UniqueConstraintException("Primary key");
            }

            c.commit(null);
        } finally {
            txn.exit();
        }

        postStoreKeyValue(txn);
    }

    // Called by JoinedUpdater.
    final void joinedUpdateCurrent() throws IOException {
        try {
            R current = mRow;
            if (current == null) {
                throw new IllegalStateException("No current row");
            }

            Cursor c = mCursor;

            byte[] key, value;
            {
                RowEvaluator<R> evaluator = mEvaluator;
                key = evaluator.updateKey(current, c.key());
                value = evaluator.updateValue(current, c.value());
            }

            if (Arrays.equals(key, c.key())) {
                doUpdateAsStore(c, value);
            } else {
                doUpdateAsDeleteInsert(current, c, key, value);
            }
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (UniqueConstraintException e) {
            throw e;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }

        unlocked(); // prevent subclass from attempting to release the lock
    }

    @Override
    public final R delete(R row) throws IOException {
        deleteCurrent();
        return doStep(row);
    }

    protected final void deleteCurrent() throws IOException {
        doDelete: try {
            Trigger<R> trigger = mTable.getTrigger();
            if (trigger == null) {
                doDelete();
            } else while (true) {
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();
                    if (mode == Trigger.SKIP) {
                        doDelete();
                        break doDelete;
                    }
                    if (mode != Trigger.DISABLED) {
                        R current = mRow;
                        if (current == null) {
                            throw new IllegalStateException("No current row");
                        }
                        doDelete(trigger, current);
                        break doDelete;
                    }
                } finally {
                    trigger.releaseShared();
                }
                trigger = mTable.trigger();
            }
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }

        unlocked(); // prevent subclass from attempting to release the lock
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        LockResult result = c.first();
        c.register();
        return result;
    }

    /**
     * Called when the key didn't change.
     */
    protected void storeValue(Cursor c, byte[] value) throws IOException {
        c.store(value);
    }

    /**
     * Called when the key didn't change.
     */
    protected void storeValue(Trigger<R> trigger, R row, Cursor c, byte[] value)
        throws IOException
    {
        Transaction txn = ViewUtils.enterScope(mTable.mSource, c.link());
        try {
            byte[] oldValue = c.value();
            c.store(value);
            // Only need to enable redoPredicateMode for the trigger, since it might insert new
            // secondary index entries (and call openAcquire).
            mTable.redoPredicateMode(txn);
            if (oldValue == null) {
                trigger.insertP(txn, row, c.key(), value);
            } else {
                trigger.storeP(txn, row, c.key(), oldValue, value);
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Called after the key and value changed and have been updated.
     *
     * @param txn never null
     */
    protected void postStoreKeyValue(Transaction txn) throws IOException {
    }

    protected void doDelete() throws IOException {
        mCursor.delete();
    }

    protected void doDelete(Trigger<R> trigger, R row) throws IOException {
        Cursor c = mCursor;
        Transaction txn = ViewUtils.enterScope(mTable.mSource, c.link());
        try {
            byte[] oldValue = c.value();
            if (oldValue != null) {
                // Don't pass the row in case the key columns were modified.
                trigger.delete(txn, c.key(), oldValue);
                c.commit(null);
            }
        } finally {
            txn.exit();
        }
    }

    @Override
    protected final R evalRow(Cursor c, LockResult result, R row) throws IOException {
        if (mKeysToSkip != null && mKeysToSkip.remove(c.key())) {
            return null;
        }
        return super.evalRow(c, result, row);
    }
}
