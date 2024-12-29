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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.IOException;

import java.util.Objects;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.diag.DatabaseStats;

import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

/**
 * Controller is used for checkpoints and as a non-functional writer when in replica mode.
 *
 * @author Brian S O'Neill
 * @see ReplEngine
 */
final class ReplController extends ReplWriter {
    private static final VarHandle cCheckpointPosHandle, cSwitchingHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cCheckpointPosHandle = lookup.findVarHandle
                (ReplController.class, "mCheckpointPos", long.class);

            cSwitchingHandle = lookup.findVarHandle
                (ReplController.class, "mSwitchingToReplica", boolean.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    final StreamReplicator mRepl;

    private Latch.Condition mLeaderNotifyCondition;

    private volatile ReplWriter mTxnRedoWriter;
    private volatile boolean mSwitchingToReplica;

    // These fields capture the state of the last written commit at the start of a checkpoint.
    private ReplWriter mCheckpointRedoWriter;
    private long mCheckpointPos;
    private long mCheckpointTxnId;

    private long mCheckpointNum;

    ReplController(ReplEngine engine) {
        super(engine, null);
        mRepl = engine.mRepl;
        // Use this instance for replica mode.
        mTxnRedoWriter = this;
        mUnmodifiable = true;
    }

    void initCheckpointNumber(long num) {
        acquireExclusive();
        mCheckpointNum = num;
        releaseExclusive();
    }

    public ReplDecoder ready(long initialPosition, long initialTxnId) throws IOException {
        acquireExclusive();
        try {
            mLeaderNotifyCondition = new Latch.Condition();
            // Init for the shouldCheckpoint method. Without this, an initial checkpoint is
            // performed even if it's not necessary.
            cCheckpointPosHandle.setOpaque(this, initialPosition | (1L << 63));
        } finally {
            releaseExclusive();
        }

        ReplDecoder decoder = mEngine.startReceiving(initialPosition, initialTxnId);

        if (decoder == null) {
            // Failed to start, and database has been closed with an exception.
            return null;
        }

        LocalDatabase db = mEngine.mDatabase;

        // Can now send control messages.
        mRepl.controlMessageAcceptor(message -> {
            try {
                db.writeControlMessage(message);
            } catch (UnmodifiableReplicaException e) {
                // Drop it.
            } catch (Throwable e) {
                Utils.uncaught(e);
            }
        });

        // Can now accept snapshot requests.
        mRepl.snapshotRequestAcceptor(sender -> {
            try {
                ReplUtils.sendSnapshot(db, sender);
            } catch (Throwable e) {
                Utils.closeQuietly(sender);
                if (e instanceof DatabaseException || !(e instanceof IOException)) {
                    Utils.uncaught(e);
                }
            }
        });

        // Update the local member role.
        mRepl.start();

        return decoder;
    }

    /**
     * Waits until replication has "caught up" before returning.
     */
    public void catchup(ReplDecoder decoder) throws IOException {
        if (decoder == null) {
            // Failed to start.
            return;
        }

        boolean isLeader = decoder.catchup();

        // We're not truly caught up until all outstanding redo operations have been applied.
        // Suspend and resume does the trick.
        mEngine.suspend();
        mEngine.resume();

        // Wait for leaderNotify method to be called. The local member might be the leader now,
        // or the new leadership might have been immediately revoked. Either case is detected.
        acquireExclusive();
        try {
            if (isLeader && mLeaderNotifyCondition != null) {
                mLeaderNotifyCondition.await(this);
            }
        } finally {
            mLeaderNotifyCondition = null;
            releaseExclusive();
        }
    }

    @Override
    public ReplWriter txnRedoWriter() {
        return mTxnRedoWriter;
    }

    @Override
    boolean shouldCheckpoint(long sizeThreshold) {
        acquireShared();
        try {
            StreamReplicator.Writer writer = mTxnRedoWriter.mReplWriter;
            long pos = writer == null ? mEngine.decodePosition() : writer.position();
            return (pos - checkpointPosition()) >= sizeThreshold;
        } catch (IllegalStateException e) {
            // Decoder hasn't been assigned yet.
            return false;
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

        ReplWriter redo = mTxnRedoWriter;
        mCheckpointRedoWriter = redo;

        // Only capture new checkpoint state if previous attempt succeeded.
        if (mCheckpointPos <= 0 && mCheckpointTxnId == 0) {
            if (redo.mReplWriter == null) {
                cCheckpointPosHandle.setOpaque(this, mEngine.suspendedDecodePosition());
                mCheckpointTxnId = mEngine.suspendedDecodeTransactionId();
            } else {
                redo.acquireShared();
                cCheckpointPosHandle.set(this, redo.mLastCommitPos);
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
        return mCheckpointPos & ~(1L << 63);
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

        ReplWriter redo = mCheckpointRedoWriter;
        StreamReplicator.Writer writer = redo.mReplWriter;

        if (writer != null && !confirm(writer, mCheckpointPos, -1)) {
            // Leadership lost, so checkpoint no higher than the position that the next leader
            // starts from. The transaction id can be zero, because the next leader always
            // writes a reset operation to the redo log.
            long endPos = confirmEnd(writer);
            if (endPos < mCheckpointPos) {
                cCheckpointPosHandle.setOpaque(this, endPos);
                mCheckpointTxnId = 0;
            }

            // Force next checkpoint to behave like a replica
            mCheckpointRedoWriter = this;

            throw nowUnmodifiable(writer);
        }

        // Make sure that durable replication data is caught up to the local database.

        syncConfirm(mCheckpointPos, -1);
    }

    private void syncConfirm(long position, long nanosTimeout) throws IOException {
        if (nanosTimeout < 0) {
            while (!mRepl.syncCommit(position, 1_000_000_000L)) {
                if (mEngine.mDatabase.isClosed()) {
                    throw new ConfirmationFailureException("Database is closed");
                }
            }
        } else if (!mRepl.syncCommit(position, nanosTimeout)) {
            throw new ConfirmationTimeoutException(nanosTimeout);
        }
    }
    
    @Override
    void checkpointFinished() throws IOException {
        long pos = mCheckpointPos;
        mRepl.compact(pos);
        mCheckpointRedoWriter = null;
        // Keep checkpoint position for the benefit of the shouldCheckpoint method, but flip
        // the bit for the checkpointSwitch method to detect successful completion.
        cCheckpointPosHandle.setOpaque(this, pos | 1L << 63);
        mCheckpointTxnId = 0;
    }

    @Override
    DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException {
        throw unmodifiable();
    }

    @Override
    long adjustTransactionId(long txnId) {
        return -txnId;
    }

    @Override
    void sync(boolean metadata, long nanosTimeout) throws IOException {
        // Interpret metadata option as a durability confirmation request.

        if (metadata) {
            try {
                long pos;
                StreamReplicator.Writer writer = mTxnRedoWriter.mReplWriter;
                if (writer == null) {
                    pos = mEngine.decodePosition();
                } else {
                    pos = writer.commitPosition();
                }

                syncConfirm(pos, nanosTimeout);

                // Also inform that the log can be compacted, in case it was rejected the last
                // time. This method (force) can be called outside the regular checkpoint
                // workflow, so use opaque access to avoid special synchronization.
                pos = (long) cCheckpointPosHandle.getOpaque(this);
                if (pos < 0) {
                    try {
                        mRepl.compact(pos & ~(1L << 63));
                    } catch (Throwable e) {
                        // Ignore.
                    }
                }

                return;
            } catch (IOException e) {
                // Try regular sync instead, in case leadership just changed.
            }
        }

        super.sync(metadata, nanosTimeout);
    }

    @Override
    void addStats(DatabaseStats stats) {
        if (!isLeader()) {
            try {
                // Capture this first, to avoid reporting a negative backlog.
                long decodePosition = mEngine.decodePosition();
                long commitPosition = mEngine.mRepl.commitPosition();
                stats.replicationBacklog = commitPosition - decodePosition;
            } catch (IllegalStateException e) {
                // Decoder hasn't been set yet.
            }
        }
    }

    @Override
    boolean isLeader() {
        return !mTxnRedoWriter.mUnmodifiable;
    }

    @Override
    void uponLeader(Runnable acquired, Runnable lost) {
        acquireExclusive();
        try {
            doUponLeader(acquired, lost);
        } finally {
            releaseExclusive();
        }
    }

    // Caller must hold exclusive latch.
    private void doUponLeader(Runnable acquired, Runnable lost) {
        if (isLeader()) {
            if (acquired != null) {
                Runner.start(acquired);
            }
            if (lost != null) {
                mTxnRedoWriter.mReplWriter.uponCommit(Long.MAX_VALUE, pos -> Runner.start(lost));
            }
        } else {
            if (mLeaderNotifyCondition == null) {
                mLeaderNotifyCondition = new Latch.Condition();
            }

            // When signaled and with the latch held, need to check everything again.
            mLeaderNotifyCondition.uponSignal(() -> doUponLeader(acquired, lost));
        }
    }

    /**
     * Called by ReplEngine when local instance has become the leader.
     *
     * @return new leader redo writer
     */
    ReplWriter leaderNotify(StreamReplicator.Writer writer) throws IOException {
        Objects.requireNonNull(writer);

        acquireExclusive();
        try {
            // Note: Signal isn't delivered until after latch is released.
            if (mLeaderNotifyCondition != null) {
                mLeaderNotifyCondition.signalAll(this);
                mLeaderNotifyCondition = null;
            }

            if (mTxnRedoWriter.mReplWriter != null) {
                // Must be in replica mode.
                return null;
            }

            var redo = new ReplWriter(mEngine, writer);
            redo.start();
            TransactionContext context = mEngine.mDatabase.anyTransactionContext();

            context.fullAcquireRedoLatch(redo);
            try {
                // If these initial redo ops fail because leadership is immediately lost, the
                // unmodifiable method will be called and needs to see the redo writer.
                mTxnRedoWriter = redo;

                // Switch to replica when leadership is revoked.
                writer.uponCommit(Long.MAX_VALUE, pos -> switchToReplica(writer));

                // Clear the log state and write a reset op to signal leader transition.
                context.doRedoReset(redo);

                // Record leader transition epoch.
                context.doRedoTimestamp(redo, RedoOps.OP_TIMESTAMP, DurabilityMode.NO_FLUSH);

                // Don't trust timestamp alone to help detect divergent logs. Use NO_SYNC mode
                // to flush everything out, but no need to wait for confirmation.
                context.doRedoNopRandom(redo, DurabilityMode.NO_SYNC);

                return redo;
            } finally {
                context.releaseRedoLatch();
            }
        } finally {
            releaseExclusive();
        }
    }

    // Also called by ReplWriter.
    UnmodifiableReplicaException nowUnmodifiable(StreamReplicator.Writer expect)
        throws DatabaseException
    {
        switchToReplica(expect);
        return unmodifiable();
    }

    // Also called by ReplWriter.
    void switchToReplica(StreamReplicator.Writer expect) {
        if (mEngine.mDatabase.isClosed()) {
            // Don't bother switching modes, since it won't work properly anyhow.
            return;
        }

        ReplWriter redo = mTxnRedoWriter;
        StreamReplicator.Writer writer = redo.mReplWriter;

        if (writer == null || writer != expect) {
            // Must be in leader mode, and with the expected writer.
            return;
        }

        if (!cSwitchingHandle.compareAndSet(this, false, true)) {
            // Another thread is doing it.
            return;
        }

        // Invoke from a separate thread, because it can block.
        try {
            Runner.start(() -> doSwitchToReplica(redo));
        } catch (Throwable e) {
            // Panic.
            mEngine.fail(e);
        }
    }

    private void doSwitchToReplica(ReplWriter redo) {
        long pos;
        try {
            StreamReplicator.Writer writer = redo.mReplWriter;

            // Position is required, so panic if this step fails.
            pos = confirmEnd(writer);

            writer.close();

            redo.closeConsumerThread();

            // Cannot start receiving until all prepared transactions have been safely
            // transferred to ReplEngine and can be acted upon as replicas.
            mEngine.awaitPreparedTransactions();

            // Wait for any lingering transactions which optimistically committed. When a
            // checkpoint runs, it records these transactions as committed, and recovery treats
            // them as such. However, these transactions might need to roll back due to
            // leadership failover. If a checkpoint were to run now, it will fail because the
            // writer is closed, and then LocalDatabase.cleanupMasterUndoLog is called. If
            // instead the next checkpoint is run is replica mode, the optimistically committed
            // transactions will be recorded as committed even though some of them will roll
            // back. If a recovery needs to run (database restarted), then it won't roll back
            // the optimistically committed transactions that should be rolled back. This
            // erroneous state is eventually cleaned up by a later checkpoint. To prevent this
            // state from being recorded in the first place, wait for the optimistically
            // committed transactions to finish before finishing the switch to replica
            // mode. Some will finish as committed, and some will roll back.
            mEngine.mDatabase.waitForCommitted();
        } catch (Throwable e) {
            // Panic.
            mEngine.fail(e);
            return;
        }

        // The first op to receive is a reset, and so the initial transaction id can be zero.
        mEngine.startReceiving(pos, 0);

        // Use this ReplController instance for replica mode. Can only be assigned after engine
        // is at the correct position. Note the exclusive latch and the order of the two
        // volatile assignments. The exclusive latch prevents leaderNotify from assigning to
        // mTxnRedoWriter, and by assigning it first, there's no race with an immediate call to
        // switchToReplica. If the assignment was reversed, then a call to switchToReplica
        // might observe that mTxnRedoWriter still matches and then it will erroneously call
        // doSwitchToReplica again.
        acquireExclusive();
        mTxnRedoWriter = this;
        mSwitchingToReplica = false;
        releaseExclusive();

        // Allow old redo object to be garbage collected.
        mEngine.mDatabase.discardRedoWriter(redo);
    }
}
