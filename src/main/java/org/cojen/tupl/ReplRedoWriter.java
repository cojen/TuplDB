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
 * 
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
final class ReplRedoWriter extends RedoWriter {
    private final ReplRedoEngine mEngine;
    private final ReplicationManager mManager;

    // Is non-null if writes are allowed.
    private ReplicationManager.Writer mActiveWriter;

    // These fields capture the state of the last written commit.
    private ReplicationManager.Writer mLastCommitWriter;
    private long mLastCommitPos;
    private long mLastCommitTxnId;

    // These fields capture the state of the last written commit at the start of a checkpoint.
    private ReplicationManager.Writer mCheckpointWriter;
    private long mCheckpointPos;
    private long mCheckpointTxnId;

    private long mCheckpointNum;

    ReplRedoWriter(ReplRedoEngine engine) {
        super(4096, 0);
        mEngine = engine;
        mManager = engine.mManager;
    }

    synchronized void initCheckpointNumber(long num) {
        mCheckpointNum = num;
    }

    public void recover(long initialTxnId, EventListener listener) throws IOException {
        mEngine.startReceiving(mManager.readPosition(), initialTxnId);
        mManager.recover(listener);
    }

    @Override
    public long store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Pass SYNC mode to flush the buffer and obtain the commit position. Return value from
        // this method indicates if an actual sync should be performed.
        long pos = super.store(indexId, key, value, DurabilityMode.SYNC);
        if (pos < 0) {
            throw unmodifiable();
        }
        // Replication makes no distinction between SYNC and NO_SYNC durability. The NO_REDO
        // mode is not expected to be passed into this method.
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public long storeNoLock(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Ditto comments from above.
        long pos = super.storeNoLock(indexId, key, value, DurabilityMode.SYNC);
        if (pos < 0) {
            throw unmodifiable();
        }
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public long txnCommitFinal(long txnId, DurabilityMode mode) throws IOException {
        // Ditto comments from above.
        long pos = super.txnCommitFinal(txnId, DurabilityMode.SYNC);
        if (pos < 0) {
            throw unmodifiable();
        }
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public void txnCommitSync(long commitPos) throws IOException {
        ReplicationManager.Writer writer;
        if (commitPos > 0 && ((writer = mActiveWriter) == null || !writer.confirm(commitPos))) {
            throw unmodifiable();
        }
    }

    @Override
    public synchronized void close(Throwable cause) throws IOException {
        super.close(cause);
        forceAndClose();
    }

    @Override
    public final long encoding() {
        return mManager.encoding();
    }

    @Override
    boolean isOpen() {
        // Returning false all the time prevents close and shutdown messages from being
        // written. They aren't very useful anyhow, considering that they don't prevent new log
        // messages from appearing afterwards.
        return false;
    }

    @Override
    synchronized boolean shouldCheckpoint(long sizeThreshold) {
        ReplicationManager.Writer writer = mActiveWriter;
        long pos = writer == null ? mEngine.mDecodePosition : writer.position();
        return (pos - mCheckpointPos) >= sizeThreshold;
    }

    @Override
    void checkpointPrepare() throws IOException {
        // Suspend before commit lock is acquired, preventing deadlock.
        mEngine.suspend();
    }

    @Override
    synchronized void checkpointSwitch() throws IOException {
        ReplicationManager.Writer writer = mLastCommitWriter;
        mCheckpointWriter = writer;
        if (writer == null) {
            mCheckpointPos = mEngine.mDecodePosition;
            mCheckpointTxnId = mEngine.mDecodeTransactionId;
        } else {
            mCheckpointPos = mLastCommitPos;
            mCheckpointTxnId = mLastCommitTxnId;
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

        ReplicationManager.Writer writer = mLastCommitWriter;
        if (writer != null && !writer.confirm(mCheckpointPos, -1)) {
            throw unmodifiable();
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
        if (mActiveWriter == null) {
            throw unmodifiable();
        }
    }

    @Override
    void write(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        ReplicationManager.Writer writer;
        if (len > 0 && ((writer = mActiveWriter) == null || writer.write(buffer, 0, len) < 0)) {
            throw unmodifiable();
        }
    }

    @Override
    long writeCommit(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        if (len > 0) {
            ReplicationManager.Writer writer = mActiveWriter;
            long pos;
            if (writer != null && (pos = writer.write(buffer, 0, len)) >= 0) {
                mLastCommitWriter = writer;
                mLastCommitPos = pos;
                mLastCommitTxnId = lastTransactionId();
                return pos;
            } else {
                throw unmodifiable();
            }
        }
        return 0;
    }

    @Override
    void force(boolean metadata) throws IOException {
        mManager.sync();
    }

    @Override
    void forceAndClose() throws IOException {
        IOException ex = null;
        try {
            force(false);
        } catch (IOException e) {
            ex = e;
        }
        try {
            mManager.close();
        } catch (IOException e) {
            if (ex == null) {
                ex = e;
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    void writeTerminator() throws IOException {
        // No terminators.
    }

    /**
     * Called by ReplRedoEngine when local instance has become the leader.
     */
    synchronized void leaderNotify() throws UnmodifiableReplicaException, IOException {
        if (mActiveWriter == null) {
            mManager.flip();

            if ((mActiveWriter = mManager.writer()) == null) {
                // False alarm?
                return;
            }

            mLastCommitWriter = mActiveWriter;
            mLastCommitPos = mActiveWriter.position();
            mLastCommitTxnId = 0;

            // Clear the log state and write a reset op to signal leader transition.
            clearAndReset();

            // Record leader transition epoch.
            timestamp();

            // Don't trust timestamp alone to help detect divergent logs.
            nopRandom();

            flush();
        }
    }

    private synchronized UnmodifiableReplicaException unmodifiable() throws IOException {
        if (mActiveWriter != null) {
            mManager.flip();

            mActiveWriter = null;

            mLastCommitWriter = null;
            mLastCommitPos = 0;
            mLastCommitTxnId = 0;

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
