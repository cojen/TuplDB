/*
 *  Copyright 2012-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

    synchronized void initCheckpointNumber(long num) {
        mCheckpointNum = num;
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
    synchronized boolean shouldCheckpoint(long sizeThreshold) {
        ReplicationManager.Writer writer = mTxnRedoWriter.mReplWriter;
        long pos = writer == null ? mEngine.mDecodePosition : writer.position();
        return (pos - mCheckpointPos) >= sizeThreshold;
    }

    @Override
    void checkpointPrepare() throws IOException {
        // Suspend before commit lock is acquired, preventing deadlock.
        mEngine.suspend();
    }

    @Override
    void checkpointSwitch() throws IOException {
        mCheckpointNum++;

        // Only capture new checkpoint state if previous attempt succeeded.
        if (mCheckpointPos == 0 && mCheckpointTxnId == 0) {
            ReplRedoWriter redo = mTxnRedoWriter;
            mCheckpointRedoWriter = redo;
            ReplicationManager.Writer writer = redo.mReplWriter;
            if (writer == null) {
                mCheckpointPos = mEngine.mDecodePosition;
                mCheckpointTxnId = mEngine.mDecodeTransactionId;
            } else synchronized (redo) {
                mCheckpointPos = redo.mLastCommitPos;
                mCheckpointTxnId = redo.mLastCommitTxnId;
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

        confirm: if (writer != null) {
            if (writer.confirm(mCheckpointPos, -1)) {
                synchronized (redo) {
                    if (mCheckpointPos > redo.mConfirmedPos) {
                        // Update redo writer, to prevent false undo if leadership is lost.
                        redo.mConfirmedPos = mCheckpointPos;
                        redo.mConfirmedTxnId = mCheckpointTxnId;
                    }
                }
            } else {
                // Leadership lost. Use a known confirmed position for the next checkpoint. If
                // restored from the checkpoint, any in-progress transactions will re-apply
                // earlier operations. Transactional operations are expected to be idempotent,
                // but the transactions will roll back regardless.

                synchronized (redo) {
                    long confirmedPos = redo.mConfirmedPos;
                    if (confirmedPos >= mCheckpointPos) {
                        // Was already confirmed, so okay to proceed.
                        break confirm;
                    }
                    mCheckpointPos = confirmedPos;
                    mCheckpointTxnId = redo.mConfirmedTxnId;
                }

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
    void opWriteCheck() throws IOException {
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
                    pos = mEngine.mDecodePosition;
                } else synchronized (redo) {
                    pos = redo.mLastCommitPos;
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
    synchronized void leaderNotify() throws UnmodifiableReplicaException, IOException {
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

        synchronized (redo) {
            // If these initial redo ops fail because leadership is immediately lost, the
            // unmodifiable method will be called and needs to see the redo writer.
            mTxnRedoWriter = redo;

            redo.mConfirmedPos = redo.mLastCommitPos = writer.position();
            redo.mConfirmedTxnId = redo.mLastCommitTxnId = 0;

            if (!writer.leaderNotify(() -> switchToReplica(writer, false))) {
                throw nowUnmodifiable(writer);
            }

            // Clear the log state and write a reset op to signal leader transition.
            redo.clearAndReset();

            // Record leader transition epoch.
            redo.timestamp();

            // Don't trust timestamp alone to help detect divergent logs.
            redo.nopRandom();

            redo.flush();
        }
    }

    // Also called by synchronized ReplRedoWriter.
    UnmodifiableReplicaException nowUnmodifiable(ReplicationManager.Writer expect)
        throws DatabaseException
    {
        switchToReplica(expect, false);
        return mEngine.unmodifiable();
    }

    boolean switchToReplica(final ReplicationManager.Writer expect, final boolean syncd) {
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
            // Invoke from a separate thread, avoiding deadlock. This method is invoked by
            // ReplRedoWriter while synchronized, which is an inconsistent order.
            new Thread(() -> {
                synchronized (ReplRedoController.this) {
                    if (!switchToReplica(expect, true)) {
                        return;
                    }
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
