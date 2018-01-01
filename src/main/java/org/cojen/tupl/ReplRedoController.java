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

import java.io.IOException;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Controller is used for checkpoints and as a non-functional writer when in replica mode.
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
/*P*/
final class ReplRedoController extends ReplRedoWriter {
    final ReplicationManager mManager;

    private volatile ReplRedoWriter mTxnRedoWriter;

    // These fields capture the state of the last written commit at the start of a checkpoint.
    private ReplRedoWriter mCheckpointRedoWriter;
    private long mCheckpointPos;
    private long mCheckpointTxnId;

    private long mCheckpointNum;

    ReplRedoController(ReplRedoEngine engine) {
        super(engine, null);
        mManager = engine.mManager;
        // Use this instance for replica mode.
        mTxnRedoWriter = this;
    }

    void initCheckpointNumber(long num) {
        acquireExclusive();
        mCheckpointNum = num;
        releaseExclusive();
    }

    public void ready(long initialTxnId, ReplicationManager.Accessor accessor) throws IOException {
        mEngine.startReceiving(mManager.readPosition(), initialTxnId);
        mManager.ready(accessor);
    }

    @Override
    public RedoWriter txnRedoWriter() {
        return mTxnRedoWriter;
    }

    @Override
    boolean shouldCheckpoint(long sizeThreshold) {
        acquireShared();
        try {
            ReplicationManager.Writer writer = mTxnRedoWriter.mReplWriter;
            long pos = writer == null ? mEngine.decodePosition() : writer.position();
            return (pos - (mCheckpointPos & ~(1L << 63))) >= sizeThreshold;
        } finally {
            releaseShared();
        }
    }

    @Override
    void checkpointPrepare() throws IOException {
        // Suspend before commit lock is acquired, preventing deadlock.
        mEngine.suspend();
    }

    @Override
    void checkpointSwitch(TransactionContext[] contexts) throws IOException {
        mCheckpointNum++;

        ReplRedoWriter redo = mTxnRedoWriter;
        mCheckpointRedoWriter = redo;

        // Only capture new checkpoint state if previous attempt succeeded.
        if (mCheckpointPos <= 0 && mCheckpointTxnId == 0) {
            ReplicationManager.Writer writer = redo.mReplWriter;
            if (writer == null) {
                mCheckpointPos = mEngine.suspendedDecodePosition();
                mCheckpointTxnId = mEngine.suspendedDecodeTransactionId();
            } else {
                redo.acquireShared();
                mCheckpointPos = redo.mLastCommitPos;
                mCheckpointTxnId = redo.mLastCommitTxnId;
                redo.releaseShared();
            }
        }
    }

    @Override
    long checkpointNumber() {
        return mCheckpointNum;
    }

    @Override
    long checkpointPosition() {
        return mCheckpointPos;
    }

    @Override
    long checkpointTransactionId() {
        return mCheckpointTxnId;
    }

    @Override
    void checkpointAborted() {
        mEngine.resume();
        mCheckpointRedoWriter = null;
    }

    @Override
    void checkpointStarted() throws IOException {
        mEngine.resume();
    }

    @Override
    void checkpointFlushed() throws IOException {
        // Attempt to confirm the log position which was captured by the checkpoint switch.

        ReplRedoWriter redo = mCheckpointRedoWriter;
        ReplicationManager.Writer writer = redo.mReplWriter;

        if (writer != null && !writer.confirm(mCheckpointPos)) {
            // Leadership lost, so checkpoint no higher than the position that the next leader
            // starts from. The transaction id can be zero, because the next leader always
            // writes a reset operation to the redo log.
            long endPos = writer.confirmEnd();
            if (endPos < mCheckpointPos) {
                mCheckpointPos = endPos;
                mCheckpointTxnId = 0;
            }

            // Force next checkpoint to behave like a replica
            mCheckpointRedoWriter = this;

            throw nowUnmodifiable(writer);
        }

        // Make sure that durable replication data is caught up to the local database.

        mManager.syncConfirm(mCheckpointPos);
    }

    @Override
    void checkpointFinished() throws IOException {
        mManager.checkpointed(mCheckpointPos);
        mCheckpointRedoWriter = null;
        // Keep checkpoint position for the benefit of the shouldCheckpoint method, but flip
        // the bit for the checkpointSwitch method to detect successful completion.
        mCheckpointPos |= 1L << 63;
        mCheckpointTxnId = 0;
    }

    @Override
    DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException {
        throw mEngine.unmodifiable();
    }

    @Override
    long adjustTransactionId(long txnId) {
        return -txnId;
    }

    @Override
    void force(boolean metadata) throws IOException {
        // Interpret metadata option as a durability confirmation request.

        if (metadata) {
            try {
                long pos;
                ReplicationManager.Writer writer = mTxnRedoWriter.mReplWriter;
                if (writer == null) {
                    pos = mEngine.decodePosition();
                } else {
                    pos = writer.confirmedPosition();
                }
                mEngine.mManager.syncConfirm(pos);
                return;
            } catch (IOException e) {
                // Try regular sync instead, in case leadership just changed.
            }
        }

        mEngine.mManager.sync();
    }

    /**
     * Called by ReplRedoEngine when local instance has become the leader.
     */
    void leaderNotify() throws UnmodifiableReplicaException, IOException {
        acquireExclusive();
        try {
            if (mTxnRedoWriter.mReplWriter != null) {
                // Must be in replica mode.
                return;
            }

            ReplicationManager.Writer writer = mManager.writer();

            if (writer == null) {
                // Panic.
                mEngine.fail(new IllegalStateException("No writer for the leader"));
                return;
            }

            ReplRedoWriter redo = new ReplRedoWriter(mEngine, writer);
            redo.start();
            TransactionContext context = mEngine.mDatabase.anyTransactionContext();

            context.fullAcquireRedoLatch(redo);
            try {
                // If these initial redo ops fail because leadership is immediately lost, the
                // unmodifiable method will be called and needs to see the redo writer.
                mTxnRedoWriter = redo;

                if (!writer.leaderNotify(() -> switchToReplica(writer))) {
                    throw nowUnmodifiable(writer);
                }

                // Clear the log state and write a reset op to signal leader transition.
                context.doRedoReset(redo);

                // Record leader transition epoch.
                context.doRedoTimestamp(redo, RedoOps.OP_TIMESTAMP, DurabilityMode.NO_FLUSH);

                // Don't trust timestamp alone to help detect divergent logs. Use NO_SYNC mode
                // to flush everything out, but no need to wait for confirmation.
                context.doRedoNopRandom(redo, DurabilityMode.NO_SYNC);
            } finally {
                context.releaseRedoLatch();
            }
        } finally {
            releaseExclusive();
        }
    }

    // Also called by ReplRedoWriter, sometimes with the latch held.
    UnmodifiableReplicaException nowUnmodifiable(ReplicationManager.Writer expect)
        throws DatabaseException
    {
        switchToReplica(expect);
        return mEngine.unmodifiable();
    }

    // Must be called without latch held.
    void switchToReplica(ReplicationManager.Writer expect) {
        if (shouldSwitchToReplica(expect) != null) {
            // Invoke from a separate thread, avoiding deadlock. This method can be invoked by
            // ReplRedoWriter while latched, which is an inconsistent order.
            new Thread(() -> doSwitchToReplica(expect)).start();
        }
    }

    private void doSwitchToReplica(ReplicationManager.Writer expect) {
        ReplRedoWriter redo;

        ReplRedoController.this.acquireExclusive();
        try {
            redo = shouldSwitchToReplica(expect);
            if (redo == null) {
                return;
            }
            // Use this instance for replica mode.
            mTxnRedoWriter = this;
        } finally {
            ReplRedoController.this.releaseExclusive();
        }

        long pos;
        try {
            pos = redo.mReplWriter.confirmEnd();
        } catch (ConfirmationFailureException e) {
            // Position is required, so panic.
            mEngine.fail(e);
            return;
        }

        redo.flipped(pos);

        // Start receiving if not, but does nothing if already receiving. A reset op is
        // expected, and so the initial transaction id can be zero.
        mEngine.startReceiving(pos, 0);
    }

    /**
     * @return null if shouldn't switch; mTxnRedoWriter otherwise
     */
    private ReplRedoWriter shouldSwitchToReplica(ReplicationManager.Writer expect) {
        if (mEngine.mDatabase.isClosed()) {
            // Don't bother switching modes, since it won't work properly anyhow.
            return null;
        }

        ReplRedoWriter redo = mTxnRedoWriter;
        ReplicationManager.Writer writer = redo.mReplWriter;

        if (writer == null || writer != expect) {
            // Must be in leader mode.
            return null;
        }

        return redo;
    }
}
