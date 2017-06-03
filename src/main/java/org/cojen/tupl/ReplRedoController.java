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
    private final ReplicationManager mManager;

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

    public void recover(long initialTxnId, EventListener listener) throws IOException {
        mEngine.startReceiving(mManager.readPosition(), initialTxnId);
        mManager.recover(listener);
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
            return (pos - mCheckpointPos) >= sizeThreshold;
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

        // Only capture new checkpoint state if previous attempt succeeded.
        if (mCheckpointPos == 0 && mCheckpointTxnId == 0) {
            ReplRedoWriter redo = mTxnRedoWriter;
            mCheckpointRedoWriter = redo;
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
        LocalDatabase db = redo.mEngine.mDatabase;

        if (writer != null) {
            if (writer.confirm(mCheckpointPos, -1)) {
                // Update confirmed state, to prevent false undo if leadership is lost.
                db.anyTransactionContext().confirmed(mCheckpointPos, mCheckpointTxnId);
            } else {
                // Leadership lost. Use a known confirmed position for the next checkpoint. If
                // restored from the checkpoint, any in-progress transactions will re-apply
                // earlier operations. Transactional operations are expected to be idempotent,
                // but the transactions will roll back regardless.

                long[] result = db.highestTransactionContext().copyConfirmed();

                mCheckpointPos = result[0];
                mCheckpointTxnId = result[1];
                // Force next checkpoint to behave like a replica
                mCheckpointRedoWriter = this;

                throw nowUnmodifiable(writer);
            }
        }

        // Make sure that durable replication data is caught up to the local database.

        mManager.syncConfirm(mCheckpointPos, -1);
    }

    @Override
    void checkpointFinished() throws IOException {
        mManager.checkpointed(mCheckpointPos);
        mCheckpointRedoWriter = null;
        mCheckpointPos = 0;
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
            long pos;
            {
                ReplRedoWriter redo = mTxnRedoWriter;
                if (redo.mReplWriter == null) {
                    pos = mEngine.decodePosition();
                } else {
                    redo.acquireShared();
                    pos = redo.mLastCommitPos;
                    redo.releaseShared();
                }
            }

            try {
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

            mManager.flip();
            ReplicationManager.Writer writer = mManager.writer();

            if (writer == null) {
                // False alarm?
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

                // If replication system makes us the leader at this position, it's confirmed.
                // Note that transaction id is 0, because the reset operation also sets the
                // last transaction id to 0. Delta encoding will continue to work correctly.
                context.confirmed(redo.mLastCommitPos = writer.position(), 0);

                if (!writer.leaderNotify(() -> switchToReplica(writer, false))) {
                    throw nowUnmodifiable(writer);
                }

                // Clear the log state and write a reset op to signal leader transition.
                context.doRedoReset(redo);

                // Record leader transition epoch.
                context.doRedoTimestamp(redo, RedoOps.OP_TIMESTAMP);

                // Don't trust timestamp alone to help detect divergent logs.
                context.doRedoNopRandom(redo);

                context.doFlush();
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
        switchToReplica(expect, false);
        return mEngine.unmodifiable();
    }

    boolean switchToReplica(final ReplicationManager.Writer expect, final boolean syncd) {
        if (mEngine.mDatabase.isClosed()) {
            // Don't bother switching modes, since it won't work properly anyhow.
            return false;
        }

        final ReplRedoWriter redo = mTxnRedoWriter;
        ReplicationManager.Writer writer = redo.mReplWriter;

        if (writer == null || writer != expect) {
            // Must be in leader mode.
            return false;
        }

        if (syncd) {
            mManager.flip();

            // Use this instance for replica mode.
            mTxnRedoWriter = this;
        } else {
            // Invoke from a separate thread, avoiding deadlock. This method can be invoked by
            // ReplRedoWriter while latched, which is an inconsistent order.
            new Thread(() -> {
                ReplRedoController.this.acquireExclusive();
                try {
                    if (!switchToReplica(expect, true)) {
                        return;
                    }
                } finally {
                    ReplRedoController.this.releaseExclusive();
                }

                long pos = mManager.readPosition();

                redo.flipped(pos);

                // Start receiving if not, but does nothing if already receiving. A reset
                // op is expected, and so the initial transaction id can be zero.
                mEngine.startReceiving(pos, 0);
            }).start();
        }

        return true;
    }
}
