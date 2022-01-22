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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Arrays;
import java.util.Objects;
import java.util.TreeSet;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
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
class BasicRowUpdater<R> extends BasicRowScanner<R> implements RowUpdater<R> {
    private TreeSet<byte[]> mKeysToSkip;

    BasicRowUpdater(AbstractTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    public final R update() throws IOException {
        return doUpdateAndStep(null);
    }

    @Override
    public final R update(R row) throws IOException {
        Objects.requireNonNull(row);
        return doUpdateAndStep(row);
    }

    private R doUpdateAndStep(R row) throws IOException {
        try {
            R current = mRow;
            if (current == null) {
                throw new IllegalStateException("No current row");
            }
            doUpdate(current);
        } catch (UnpositionedCursorException e) {
            finished();
            throw new IllegalStateException("No current row");
        } catch (UniqueConstraintException e) {
            throw e;
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
        unlocked(); // prevent subclass from attempting to release the lock
        return doStep(row);
    }

    @Override
    public final R delete() throws IOException {
        return doDeleteAndStep(null);
    }

    @Override
    public final R delete(R row) throws IOException {
        Objects.requireNonNull(row);
        return doDeleteAndStep(row);
    }

    private R doDeleteAndStep(R row) throws IOException {
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
                        doDelete(trigger, mRow);
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

        return doStep(row);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        LockResult result = c.first();
        c.register();
        return result;
    }

    protected final void doUpdate(R row) throws IOException {
        byte[] key, value;
        {
            RowDecoderEncoder<R> encoder = mDecoder;
            key = encoder.encodeKey(row);
            value = encoder.encodeValue(row);
        }

        Cursor c = mCursor;

        int cmp;
        if (key == null || (cmp = c.compareKeyTo(key)) == 0) {
            // Key didn't change.

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

        // This point is reached when the key changed, and so the update is out of sequence. A
        // new value is inserted (if permitted), and the current one is deleted. If the new key
        // is higher, it's added to a remembered set and not observed again by this updater.

        if (cmp < 0) {
            if (mKeysToSkip == null) {
                // TODO: Consider designing a more memory efficient set or hashtable.
                mKeysToSkip = new TreeSet<>(Arrays::compareUnsigned);
            }
            // FIXME: For AutoCommitRowUpdater, consider limiting the size of the set and
            // use a temporary index. All other updaters maintain locks, and so the key
            // objects cannot be immediately freed anyhow.
            if (!mKeysToSkip.add(key)) {
                // Won't be removed from the set in case of UniqueConstraintException.
                cmp = 0;
            }
        }

        Index source = mTable.mSource;

        Transaction txn = ViewUtils.enterScope(source, c.link());
        doUpdate: try {
            boolean result;

            RowPredicateLock<R> lock = mTable.mIndexLock;
            if (lock == null) {
                result = source.insert(txn, key, value);
            } else {
                try (RowPredicateLock.Closer closer = lock.openAcquire(txn, row)) {
                    result = source.insert(txn, key, value);
                }
            }

            if (!result) {
                if (cmp < 0) {
                    mKeysToSkip.remove(key);
                }
                throw new UniqueConstraintException("Primary key");
            }

            Trigger<R> trigger = mTable.getTrigger();
            if (trigger == null) {
                c.commit(null);
            } else while (true) {
                trigger.acquireShared();
                try {
                    int mode = trigger.mode();
                    if (mode == Trigger.SKIP) {
                        c.commit(null);
                        break doUpdate;
                    }
                    if (mode != Trigger.DISABLED) {
                        c.delete();
                        trigger.insert(txn, mRow, c.key(), value);
                        txn.commit();
                        break doUpdate;
                    }
                } finally {
                    trigger.releaseShared();
                }
                trigger = mTable.trigger();
            }
        } finally {
            txn.exit();
        }

        postStoreKeyValue(txn);
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
            if (oldValue == null) {
                trigger.insert(txn, row, c.key(), value);
            } else {
                trigger.store(txn, row, c.key(), oldValue, value);
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
                c.delete();
                trigger.delete(txn, row, c.key(), oldValue);
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    @Override
    protected R decodeRow(Cursor c, R row) throws IOException {
        if (mKeysToSkip != null && mKeysToSkip.remove(c.key())) {
            return null;
        }
        return super.decodeRow(c, row);
    }
}
