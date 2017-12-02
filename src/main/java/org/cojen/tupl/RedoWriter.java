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

package org.cojen.tupl;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.cojen.tupl.util.Latch;

/**
 * Abstract class for active transactions to write into. Redo operations are encoded and
 * buffered by TransactionContext.
 *
 * @author Brian S O'Neill
 * @see RedoDecoder
 */
/*P*/
abstract class RedoWriter extends Latch implements Closeable, Flushable {
    // Only access while latched. Is accessed by TransactionContext and ReplRedoWriter.
    long mLastTxnId;

    volatile Throwable mCloseCause;

    RedoWriter() {
    }

    final void closeCause(Throwable cause) {
        if (cause != null) {
            acquireExclusive();
            if (mCloseCause == null) {
                mCloseCause = cause;
            }
            releaseExclusive();
        }
    }

    /**
     * Called to sync a redo operation which has no associated transaction.
     *
     * @param commitPos highest position to sync (exclusive)
     */
    abstract void commitSync(TransactionContext context, long commitPos) throws IOException;

    /**
     * Called after redoCommitFinal.
     *
     * @param txn transaction committed
     * @param commitPos highest position to sync (exclusive)
     */
    abstract void txnCommitSync(LocalTransaction txn, long commitPos) throws IOException;

    /**
     * Called after redoCommitFinal.
     *
     * @param pending pending transaction committed
     */
    abstract void txnCommitPending(PendingTxn pending) throws IOException;

    abstract long encoding();

    /**
     * Return a new or existing RedoWriter for a new transaction.
     */
    abstract RedoWriter txnRedoWriter();

    /**
     * Returns true if uncheckpointed redo size is at least the given threshold
     */
    abstract boolean shouldCheckpoint(long sizeThreshold);

    /**
     * Called before checkpointSwitch, to perform any expensive operations like opening a new
     * file. Method must not perform any checkpoint state transition.
     */
    abstract void checkpointPrepare() throws IOException;

    /**
     * With exclusive commit lock held, switch to the previously prepared state, also capturing
     * the checkpoint position and transaction id.
     *
     * @param contexts all contexts which flush into this
     */
    abstract void checkpointSwitch(TransactionContext[] contexts) throws IOException;

    /**
     * Returns the checkpoint number for the first change after the checkpoint switch.
     */
    abstract long checkpointNumber() throws IOException;

    /**
     * Returns the redo position for the first change after the checkpoint switch.
     */
    abstract long checkpointPosition() throws IOException;

    /**
     * Returns the transaction id for the first change after the checkpoint switch, which is
     * later used by recovery. If not needed by recovery, simply return 0.
     */
    abstract long checkpointTransactionId() throws IOException;

    /**
     * Called after checkpointPrepare and exclusive commit lock is released, but checkpoint is
     * aborted due to an exception.
     */
    abstract void checkpointAborted();

    /**
     * Called after exclusive commit lock is released. Dirty pages start flushing as soon as
     * this method returns.
     */
    abstract void checkpointStarted() throws IOException;

    /**
     * Called after all dirty pages have flushed.
     */
    abstract void checkpointFlushed() throws IOException;

    /**
     * Writer can discard all redo data lower than the checkpointed position, which was
     * captured earlier.
     */
    abstract void checkpointFinished() throws IOException;

    /**
     * Negate the identifier if a replica, but leave alone otherwise.
     *
     * @param id new transaction identifier; greater than zero
     */
    long adjustTransactionId(long txnId) {
        // Non-replica by default.
        return txnId;
    }

    /**
     * @param mode requested mode; can be null if not applicable
     * @return actual mode to use
     * @throws UnmodifiableReplicaException if a replica
     */
    abstract DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException;

    /**
     * @return true if all redo operations end with a terminator
     */
    abstract boolean shouldWriteTerminators();

    /**
     * Write to the physical log.
     *
     * @param flush true to immediately flush the log
     * @param length never 0
     * @param commitLen length of message which is fully committable (no torn operations); pass
     * <= 0 if nothing is committable
     * @return highest log position afterwards
     */
    // Caller must hold exclusive latch.
    abstract long write(boolean flush, byte[] bytes, int offset, int length, int commitLen)
        throws IOException;

    /**
     * @param enable when enabled, a flush is also performed immediately
     */
    abstract void alwaysFlush(boolean enable) throws IOException;

    /**
     * Durably writes all flushed data.
     *
     * @param metadata true to durably write applicable file system metadata too
     */
    abstract void force(boolean metadata) throws IOException;
}
