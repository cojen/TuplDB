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

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Arrays;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.util.Runner;
import org.cojen.tupl.util.Worker;

/**
 * Secondary index backfill task, to be used when new indexes are added. Entries which are
 * added into the index aren't replicated.
 *
 * @author Brian S O'Neill
 */
public abstract class IndexBackfill<R> extends Worker.Task implements Closeable {
    final TableManager mManager;
    final RowStore mRowStore;
    final boolean mAutoload;
    final Index mSecondaryIndex;
    final byte[] mSecondaryDescriptor;
    final String mSecondaryStr;

    // Set of new secondary index entries to add.
    private volatile Sorter mSorter;

    // Track secondary index entries which are concurrently deleted.
    private volatile Index mDeleted;

    // Highest secondary key which has been backfilled.
    private volatile byte[] mBackfillProgress;

    // Count of triggers that are using this backfill. When negative, the backfill is closed.
    private int mUsed;

    /**
     * @param autoload pass true to autoload values from the primary index because they're
     * needed to create secondary index entries
     * @param secondaryStr name of secondary index
     */
    protected IndexBackfill(RowStore rs, TableManager manager, boolean autoload,
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
    }

    @Override
    public void run() {
        EventListener listener = mRowStore.mDatabase.eventListener();

        if (listener != null) {
            listener.notify(EventType.TABLE_INDEX_BACKFILL_INFO,
                            "Starting backfill for %1$s", mSecondaryStr);
        }

        boolean success = false;

        try {
            success = doRun();

            if (listener != null) {
                String message = (success ? "Finsished" : "Stopped") + " backfill for %1$s";
                listener.notify(EventType.TABLE_INDEX_BACKFILL_INFO, message, mSecondaryStr);
            }
        } catch (Throwable e) {
            error("Unable to backfill %1$s: %2$s", e);
        }

        try {
            mRowStore.activateSecondaryIndex(this, success);
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
                listener.notify(EventType.TABLE_INDEX_BACKFILL_ERROR, message, mSecondaryStr, e);
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

        Scanner scanner;
        try {
            scanner = sorter.finishScan();
        } catch (InterruptedIOException e) {
            if (mSorter != null) {
                throw e;
            }
            return false;
        }

        Index deleted = mDeleted;
        if (deleted == null) {
            return false;
        }

        try (scanner) {
            txn.lockMode(LockMode.UPGRADABLE_READ);

            try (Cursor secondaryCursor = mSecondaryIndex.newCursor(txn)) {
                secondaryCursor.autoload(false);

                // Note that deletedCursor doesn't need to acquire locks. The lock on the
                // secondary index entry suffices and prevents deadlocks.
                try (Cursor deletedCursor = deleted.newCursor(Transaction.BOGUS)) {
                    deletedCursor.autoload(false);

                    for (byte[] key; (key = scanner.key()) != null; scanner.step()) {
                        secondaryCursor.findNearby(key); // must find and lock first
                        deletedCursor.findNearby(key);
                        if (secondaryCursor.value() == null && deletedCursor.value() == null) {
                            secondaryCursor.commit(scanner.value());
                        }
                        txn.commit();
                        mBackfillProgress = key;
                        deletedCursor.delete();
                    }
                }
            }
        }

        try {
            mRowStore.mDatabase.deleteIndex(deleted).run();
        } catch (Exception e) {
            // Ignore.
        }

        return true;
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

        synchronized (this) {
            if (mSorter != null) {
                // Still in use.
                return true;
            }
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
        unused(true);
    }

    /**
     * Called by a trigger to indicate that it's actively using this backfill.
     */
    public synchronized void used() {
        if (mUsed < 0) {
            throw new IllegalStateException("Closed");
        }
        mUsed++;
    }

    /**
     * Called by a trigger to indicate that it's done using this backfill.
     */
    public void unused() {
        unused(false);
    }

    private void unused(boolean close) {
        Sorter sorter;
        Index deleted;

        synchronized (this) {
            if (mUsed < 0 || (!close && --mUsed > 0)) {
                return;
            }
            mUsed = -1;
            sorter = mSorter;
            mSorter = null;
            deleted = mDeleted;
            mDeleted = null;
            mBackfillProgress = null;
        }

        if (sorter == null && deleted == null) {
            return;
        }

        Database db = mRowStore.mDatabase;

        Runner.start(() -> {
            Runnable deleteTask = null;
            try {
                deleteTask = db.deleteIndex(deleted);
            } catch (Exception e) {
                // Ignore.
            }

            try {
                sorter.reset();
            } catch (Exception e) {
                // Ignore.
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
        if (deleted != null) {
            byte[] progress = mBackfillProgress;
            if (progress == null || Arrays.compareUnsigned(secondaryKey, progress) > 0) {
                try {
                    deleted.store(txn, secondaryKey, RowUtils.EMPTY_BYTES);
                } catch (ClosedIndexException e) {
                    // Ignore and assume that there was a race condition and mDeleted is now null.
                }
            }
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
