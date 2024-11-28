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

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.HashSet;
import java.util.Set;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.LocalDatabase;
import org.cojen.tupl.core.RedoListener;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.util.Runner;
import org.cojen.tupl.util.Worker;

/**
 * Secondary index backfill task, to be used when new indexes are added. Entries which are
 * added into the index aren't replicated.
 *
 * @author Brian S O'Neill
 */
public abstract class IndexBackfill<R> extends Worker.Task implements RedoListener, Closeable {
    final TableManager<R> mManager;
    final RowStore mRowStore;
    final boolean mAutoload;
    final Index mSecondaryIndex;
    final byte[] mSecondaryDescriptor;
    final String mSecondaryStr;

    // Set of new secondary index entries to add.
    private volatile Sorter mSorter;

    // Track secondary index entries which are concurrently deleted.
    private volatile Index mDeleted;

    // Set of triggers that are using this backfill. When null, the backfill is closed.
    private Set<Trigger<R>> mTriggers;

    // The new secondary index as built by the sorter.
    private Index mNewSecondaryIndex;

    /**
     * @param autoload pass true to autoload values from the primary index because they're
     * needed to create secondary index entries
     * @param secondaryStr name of secondary index
     */
    protected IndexBackfill(RowStore rs, TableManager<R> manager, boolean autoload,
                            Index secondaryIndex, byte[] secondaryDesc, String secondaryStr)
        throws IOException
    {
        mRowStore = rs;
        mManager = manager;
        mAutoload = autoload;
        mSecondaryIndex = secondaryIndex;
        mSecondaryDescriptor = secondaryDesc;
        mSecondaryStr = secondaryStr;

        Database db = mRowStore.mDatabase;
        mSorter = db.newSorter();
        mDeleted = db.newTemporaryIndex();

        mTriggers = new HashSet<>();
    }

    @Override
    public void run() {
        EventListener listener;
        checkNotify: {
            try {
                if (mManager.mPrimaryIndex.isEmpty()) {
                    // No need to notify if index is empty because there isn't really a backfill.
                    listener = null;
                    break checkNotify;
                }
            } catch (IOException e) {
            }
            listener = mRowStore.mDatabase.eventListener();
        }

        if (listener != null) {
            listener.notify(EventType.TABLE_INDEX_INFO,
                            "Starting backfill for %1$s", mSecondaryStr);
        }

        boolean success = false;

        try {
            success = doRun();

            if (!success && listener != null) {
                listener.notify(EventType.TABLE_INDEX_INFO,
                                "Stopped backfill for %1$s", mSecondaryStr);
            }
        } catch (Throwable e) {
            error("Unable to backfill %1$s: %2$s", e);
        }

        try {
            mRowStore.activateSecondaryIndex(this, success);

            if (success && listener != null) {
                listener.notify(EventType.TABLE_INDEX_INFO,
                                "Finished backfill for %1$s", mSecondaryStr);
            }
        } catch (Throwable e) {
            error("Unable to activate %1$s: %2$s", e);
        }
    }

    private void error(String message, Throwable e) {
        if (!mRowStore.mDatabase.isClosed()) {
            EventListener listener = mRowStore.mDatabase.eventListener();
            if (listener == null) {
                RowUtils.uncaught(e);
            } else {
                listener.notify(EventType.TABLE_INDEX_ERROR, message, mSecondaryStr, e);
            }
        }
    }

    /**
     * @return false if closed
     */
    private boolean doRun() throws IOException {
        var primaryBatch = new byte[100 * 2][];
        var secondaryBatch = new byte[primaryBatch.length][];

        Transaction txn = mRowStore.mDatabase.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        txn.lockTimeout(-1, null);
        txn.durabilityMode(DurabilityMode.NO_REDO);

        try (Cursor c = mManager.mPrimaryIndex.newCursor(txn)) {
            c.autoload(mAutoload);
            c.first();
            int length = 0;
            while (true) {
                byte[] key = c.key();
                if (key == null) {
                    if (length > 0) {
                        if (!addBatch(primaryBatch, secondaryBatch, length)) {
                            return false;
                        }
                    }
                    break;
                }
                primaryBatch[length++] = key;
                primaryBatch[length++] = c.value();
                if (length >= primaryBatch.length) {
                    if (!addBatch(primaryBatch, secondaryBatch, length)) {
                        return false;
                    }
                    length = 0;
                }
                c.next();
            }
        }

        Sorter sorter = mSorter;
        if (sorter == null) {
            return false;
        }

        Index newIndex;
        try {
            newIndex = sorter.finish();
        } catch (InterruptedIOException e) {
            if (mSorter != null) {
                throw e;
            }
            return false;
        }

        Index deleted;
        synchronized (this) {
            deleted = mDeleted;
            if (deleted == null) {
                return false;
            }

            // Lock all triggers to prevent concurrent modifications, and change their behavior
            // now that backfill is finishing.

            withTriggerLock(() -> {
                mNewSecondaryIndex = newIndex;
            });
        }

        txn.lockMode(LockMode.UPGRADABLE_READ);

        // At this point, no threads should be inserting new entries into mSecondaryIndex, but
        // entries might be deleted from it. For those that are deleted, this indicates that
        // the tracked entry has been superseded, and the real entry was stored into newIndex.
        // Likewise, no threads should be inserting into mDeleted, but entries might be deleted
        // from it as well.

        try (Cursor secondaryCursor = mSecondaryIndex.newCursor(txn);
             // Note that deletedCursor doesn't need to acquire locks. The lock on the original
             // secondary index entry suffices.
             Cursor deletedCursor = deleted.newCursor(Transaction.BOGUS))
        {
            secondaryCursor.first();
            for (byte[] key; (key = secondaryCursor.key()) != null; secondaryCursor.next()) {
                deletedCursor.findNearby(key);
                if (deletedCursor.value() == null) {
                    newIndex.store(txn, key, secondaryCursor.value());
                } else {
                    deletedCursor.delete();
                }
                secondaryCursor.commit(null);
            }
        }

        // Don't acquire locks when sweeping through the remaining deleted keys, avoiding
        // deadlocks. Proper lock acquisition order is mSecondaryIndex and then mDeleted.
        try (Cursor deletedCursor = deleted.newCursor(Transaction.BOGUS)) {
            deletedCursor.first();
            for (byte[] key; (key = deletedCursor.key()) != null; deletedCursor.next()) {
                newIndex.lockUpgradable(txn, key);
                try {
                    deletedCursor.load();
                    if (deletedCursor.value() != null) {
                        newIndex.store(txn, key, null);
                        deletedCursor.store(null);
                        txn.commit();
                    }
                } finally {
                    txn.exit();
                }
            }
        }

        // Swap the fully populated new secondary index with the original, which is now
        // empty. As a side-effect, the contents of the new secondary index which was temporary
        // are now associated with a real index, and the original secondary is associated with
        // the temporary index.
        withTriggerLock(() -> {
            try {
                mRowStore.mDatabase.rootSwap(newIndex, mSecondaryIndex);
            } catch (IOException e) {
                throw RowUtils.rethrow(e);
            }

            // Backfill is finished, so stop tracking changes.
            mDeleted = null;
            mNewSecondaryIndex = null;
        });

        try {
            // Eagerly delete the temporary index.
            mRowStore.mDatabase.deleteIndex(newIndex).run();
        } catch (Exception e) {
            // Ignore.
        }

        try {
            // Eagerly delete the temporary index.
            mRowStore.mDatabase.deleteIndex(deleted).run();
        } catch (Exception e) {
            // Ignore.
        }

        return true;
    }

    /**
     * Runs the given callback with the trigger lock held exclusively.
     */
    private synchronized void withTriggerLock(Runnable r) {
        // Note that this method is synchronized. This ensures that the mTriggers set doesn't
        // change at all.

        for (Trigger<R> trigger : mTriggers) {
            trigger.acquireExclusive();
        }

        Throwable ex = null;
        try {
            mRowStore.mDatabase.withRedoLock(r);
        } catch (Throwable e) {
            ex = e;
        }

        for (Trigger<R> trigger : mTriggers) {
            trigger.releaseExclusive();
        }

        if (ex != null) {
            throw RowUtils.rethrow(ex);
        }
    }

    /**
     * @return false if closed
     */
    private boolean addBatch(byte[][] primaryBatch, byte[][] secondaryBatch, int length)
        throws IOException
    {
        Sorter sorter = mSorter;
        if (sorter == null) {
            // Closed.
            return false;
        }

        for (int i=0; i<length; i+=2) {
            encode(primaryBatch[i], primaryBatch[i + 1], secondaryBatch, i);
        }

        sorter.addBatch(secondaryBatch, 0, length >> 1);

        if (mSorter != null) {
            // Still in use.
            return true;
        }

        // Closed. Clean up the mess.
        try {
            sorter.reset();
        } catch (Exception e) {
            // Ignore.
        }

        return false;
    }

    @Override
    public void close() {
        unused(null, true);
    }

    /**
     * Defined by RedoListener, and is used to observe deletes against the secondary index,
     * triggered by the incoming replication stream.
     */
    @Override
    public final void store(Transaction txn, Index ix, byte[] key, byte[] value) {
        if (ix == mSecondaryIndex) {
            try {
                if (value == null) {
                    deleted(txn, key);
                } else {
                    inserted(txn, key, value);
                }
            } catch (Throwable e) {
                error("Uncaught exception during backfill of %1$s: %2$s", e);
            }
        }
    }

    /**
     * Called by a trigger to indicate that it's actively using this backfill.
     */
    public synchronized void used(Trigger<R> trigger) {
        if (mTriggers == null) {
            throw new IllegalStateException("Closed");
        }
        mTriggers.add(trigger);
    }

    /**
     * Called by a trigger to indicate that it's done using this backfill.
     */
    public void unused(Trigger<R> trigger) {
        unused(trigger, false);
    }

    private void unused(Trigger<R> trigger, boolean close) {
        Sorter sorter;
        Index deleted;

        synchronized (this) {
            if (mTriggers == null) {
                return;
            }
            mTriggers.remove(trigger);
            if (!close && !mTriggers.isEmpty()) {
                return;
            }
            mTriggers = null;
            sorter = mSorter;
            mSorter = null;
            deleted = mDeleted;
            mDeleted = null;
        }

        if (sorter == null && deleted == null) {
            return;
        }

        LocalDatabase db = mRowStore.mDatabase;

        db.removeRedoListener(this);

        Runner.start(() -> {
            Runnable deleteTask = null;
            if (deleted != null) {
                try {
                    deleteTask = db.deleteIndex(deleted);
                } catch (Exception e) {
                    // Ignore.
                }
            }

            if (sorter != null) {
                try {
                    sorter.reset();
                } catch (Exception e) {
                    // Ignore.
                }
            }

            if (deleteTask != null) {
                deleteTask.run();
            }
        });
    }

    /**
     * Called by a trigger when an entry is deleted from the secondary index. It's expected
     * that the transaction holds a lock on the secondary key within the secondary index.
     *
     * @param txn not null
     */
    public void deleted(Transaction txn, byte[] secondaryKey) throws IOException {
        Index deleted = mDeleted;
        if (deleted == null) {
            // Backfill is finished.
            return;
        }

        try {
            Index newIndex = mNewSecondaryIndex;
            if (newIndex == null) {
                deleted.store(txn, secondaryKey, RowUtils.EMPTY_BYTES);
            } else {
                // At this point, the backfill is in the finishing phase.

                // The other two indexes are used for tracking modifications which must be
                // applied into the new index, but because the delete supersedes them, also
                // delete the corresponding tracking entries. Note that the caller just deleted
                // from mSecondaryIndex, so there's no reason to delete it again.
                deleted.delete(txn, secondaryKey);

                // Delete from what will become the real secondary index.
                newIndex.delete(txn, secondaryKey);
            }
        } catch (ClosedIndexException e) {
            // Assume that there was a race condition and the backfill now is closed.
        }
    }

    /**
     * Called by a trigger when an entry is inserted into the secondary index. It's expected
     * that the transaction holds a lock on the secondary key within the secondary index.
     *
     * @param txn not null
     */
    public void inserted(Transaction txn, byte[] secondaryKey, byte[] secondaryValue)
        throws IOException
    {
        Index newIndex = mNewSecondaryIndex;
        Index deleted = mDeleted;
        if (newIndex == null || deleted == null) {
            // Backfill hasn't reached the finishing phase or is finished.
            return;
        }

        // At this point, the backfill is in the finishing phase.

        if (txn.isBogus()) {
            // Need a transaction that supports rollback.
            txn = mRowStore.mDatabase.newTransaction(DurabilityMode.NO_REDO);
            txn.lockTimeout(-1, null);
        } else {
            txn.enter();
        }

        try {
            // The other two indexes are used for tracking modifications which must be applied
            // into the new index, but because the insert supersedes them, delete the
            // corresponding entries. Note that the caller just inserted into mSecondaryIndex,
            // and the delete undoes it. It would be more efficient to not insert in the first
            // place, but this is simpler, and backfills are infrequent.
            mSecondaryIndex.delete(txn, secondaryKey); // lock first to avoid deadlock
            deleted.delete(txn, secondaryKey);

            // Insert into what will become the real secondary index.
            newIndex.store(txn, secondaryKey, secondaryValue);

            txn.commit();
        } catch (ClosedIndexException e) {
            // Assume that there was a race condition and the backfill now is closed. The
            // scoped transaction allows the mSecondaryIndex delete to be rolled back.
        } finally {
            txn.exit();
        }
    }

    /**
     * Encode a secondary index entry, derived from a primary index entry. If the autoload
     * constructor parameter was false, the primary value must be ignored.
     *
     * @param primaryKey primary index key
     * @param primaryValue primary index value
     * @param secondaryEntry key-value pair for secondary index to be stored here
     * @param offset offset into secondaryEntry for key-value pair
     */
    protected abstract void encode(byte[] primaryKey, byte[] primaryValue,
                                   byte[][] secondaryEntry, int offset);
}
