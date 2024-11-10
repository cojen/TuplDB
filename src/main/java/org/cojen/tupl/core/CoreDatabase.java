/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.ext.Crypto;

import org.cojen.tupl.table.RowStore;

/**
 * Provides access to features of LocalDatabase implementation class, to reduce the number of
 * generated classes. In particular, there's no generated _Checkpointer class because
 * Checkpointer references only CoreDatabase instead of LocalDatabase. Without this, a
 * _Checkpointer would be generated to reference the generated _LocalDatabase.
 *
 * @author Brian S O'Neill
 */
public abstract class CoreDatabase implements Database {
    /**
     * @param owner the upgradable owner of the lock
     */
    public abstract DetachedLock newDetachedLock(Transaction owner);

    public abstract <R> RowPredicateLock<R> newRowPredicateLock(long indexId);

    /**
     * Defines anonymous secondary indexes and invokes a callback which should transactionally
     * store references to them. Next, a redo op is written which notifies replicas that the
     * set of secondaries has changed. Finally, the transaction is committed.
     *
     * @param txn is committed as a side effect; will be switched to SYNC mode if replicated
     * @param primaryIndexId index id in the notification op; pass 0 to disable notification
     * @param ids each array slot is filled in with a new identifier
     * @param callback invoked after the ids are filled in, with the commit lock held
     * @throws NullPointerException if any parameter is null
     */
    public abstract void createSecondaryIndexes(Transaction txn, long primaryIndexId,
                                                long[] ids, Runnable callback)
        throws IOException;

    /**
     * @return a non-null RowStore instance
     */
    public abstract RowStore rowStore() throws IOException;

    /**
     * Add a listener which observes incoming replication operations.
     *
     * @return false if replication isn't enabled or if the listener was already added
     */
    public abstract boolean addRedoListener(RedoListener listener);

    /**
     * Remove a listener which was added earlier.
     *
     * @return false if the listener wasn't found
     */
    public abstract boolean removeRedoListener(RedoListener listener);

    /**
     * Invoke the given callback with the redo lock held, which ensures that no incoming
     * replication operations are being processed.
     */
    public abstract void withRedoLock(Runnable callback);

    /**
     * Atomically swaps the root nodes of two trees.
     */
    public void rootSwap(Index a, Index b) throws IOException {
        ((Tree) a).rootSwap((Tree) b);
    }

    public abstract boolean isInTrash(Transaction txn, long treeId) throws IOException;

    abstract long databaseId();

    abstract boolean isDirectPageAccess();

    abstract boolean isCacheOnly();

    abstract boolean isReadOnly();

    abstract Crypto dataCrypto();

    abstract Supplier<Checksum> checksumFactory();

    abstract Tree registry();

    abstract Tree registryKeyMap();

    abstract boolean scanAllIndexes(ScanVisitor visitor) throws IOException;

    abstract Index anyIndexById(long id) throws IOException;

    /**
     * Copies all entries from a source index into a new temporary index, which can be null if
     * empty. No threads should be active in the source index.
     *
     * @param workerCount maximum parallelism; must be at least 1
     */
    abstract Tree parallelCopy(Index source, int workerCount) throws IOException;

    /**
     * @return null if none
     */
    public abstract EventListener eventListener();

    /**
     * Performs a checkpoint if automatic checkpoints are currently enabled.
     *
     * @return false if no checkpoint was performed
     */
    public abstract boolean checkpointIfEnabled() throws IOException;

    /**
     * Called by Checkpointer task.
     */
    abstract boolean checkpoint(long sizeThreshold, long delayThresholdNanos) throws IOException;

    /**
     * Called by ReplController.
     */
    abstract long writeControlMessage(byte[] message) throws IOException;
}
