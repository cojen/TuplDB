/*
 *  Copyright 2012-2013 Brian S O'Neill
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

/**
 * Controller is used for checkpoints and as a non-functional writer when in replica mode.
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
final class ReplRedoController extends ReplRedoWriter {
    private final ReplicationManager mManager;

    private volatile ReplRedoWriter mTxnRedoWriter;

    // These fields capture the state of the last written commit at the start of a checkpoint.
    private ReplicationManager.Writer mCheckpointWriter;
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
        ReplRedoWriter redo = mTxnRedoWriter;
        synchronized (redo) {
            ReplicationManager.Writer writer = redo.mReplWriter;
            mCheckpointWriter = writer;
            if (writer == null) {
                mCheckpointPos = mEngine.mDecodePosition;
                mCheckpointTxnId = mEngine.mDecodeTransactionId;
            } else {
                mCheckpointPos = redo.mLastCommitPos;
                mCheckpointTxnId = redo.mLastCommitTxnId;
            }
        }
        mCheckpointNum++;
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
        mCheckpointWriter = null;
    }

    @Override
    void checkpointStarted() throws IOException {
        mEngine.resume();

        // Make sure that durable replication data is not behind local database.

        ReplicationManager.Writer writer = mCheckpointWriter;
        if (writer != null && !writer.confirm(mCheckpointPos, -1)) {
            throw unmodifiable(writer);
        }

        mManager.syncConfirm(mCheckpointPos, -1);
    }

    @Override
    void checkpointFinished() throws IOException {
        mManager.checkpointed(mCheckpointPos);
        mCheckpointWriter = null;
    }

    @Override
    void opWriteCheck() throws IOException {
        throw new UnmodifiableReplicaException();
    }

    /**
     * Called by ReplRedoEngine when local instance has become the leader.
     */
    synchronized void leaderNotify() throws UnmodifiableReplicaException, IOException {
        ReplicationManager.Writer writer = mTxnRedoWriter.mReplWriter;

        if (writer == null) { // must be in replica mode
            mManager.flip();

            if ((writer = mManager.writer()) == null) {
                // False alarm?
                return;
            }

            ReplRedoWriter redo = new ReplRedoWriter(mEngine, writer);

            synchronized (redo) {
                // If these initial redo ops fail because leadership is immediately lost, the
                // unmodifiable method will be called and needs to see the redo writer.
                mTxnRedoWriter = redo;

                redo.mLastCommitPos = writer.position();
                redo.mLastCommitTxnId = 0;

                // Clear the log state and write a reset op to signal leader transition.
                redo.clearAndReset();

                // Record leader transition epoch.
                redo.timestamp();

                // Don't trust timestamp alone to help detect divergent logs.
                redo.nopRandom();

                redo.flush();
            }
        }
    }

    synchronized UnmodifiableReplicaException unmodifiable(ReplicationManager.Writer expect)
        throws IOException
    {
        ReplicationManager.Writer writer = mTxnRedoWriter.mReplWriter;

        if (writer != null && writer == expect) { // must be in leader mode
            mManager.flip();

            // Use this instance for replica mode.
            mTxnRedoWriter = this;

            final long initialPosition = mManager.readPosition();

            // Invoke from a separate thread, avoiding deadlock during the transition.
            new Thread() {
                public void run() {
                    // Start receiving if not, but does nothing if already receiving. A reset
                    // op is expected, and so the initial transaction id can be zero.
                    mEngine.startReceiving(initialPosition, 0);
                }
            }.start();
        }

        return new UnmodifiableReplicaException();
    }
}
