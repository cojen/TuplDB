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

    private long mCheckpointNum;
    private long mCheckpointPos;
    private long mCheckpointTxnId;

    private volatile boolean mIsLeader;

    ReplRedoWriter(ReplRedoEngine engine) {
        super(4096, 0);
        mEngine = engine;
        mManager = engine.mManager;
    }

    synchronized void initCheckpointNumber(long num) {
        mCheckpointNum = num;
    }

    public void recover(long initialTxnId, EventListener listener) throws IOException {
        mEngine.startReceiving(initialTxnId);
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
        if (commitPos > 0) {
            mManager.confirm(commitPos);
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
        long pos;
        if (mIsLeader) {
            pos = mManager.writePosition();
        } else {
            pos = mEngine.mDecodePosition;
        }
        return (pos - mCheckpointPos) >= sizeThreshold;
    }

    @Override
    void checkpointPrepare() throws IOException {
        // Suspend before commit lock is acquired, preventing deadlock.
        mEngine.suspend();
    }

    @Override
    synchronized void checkpointSwitch() {
        mCheckpointNum++;
        if (mIsLeader) {
            mCheckpointPos = mManager.writePosition();
            mCheckpointTxnId = lastTransactionId();
        } else {
            mCheckpointPos = mEngine.mDecodePosition;
            mCheckpointTxnId = mEngine.mDecodeTransactionId;
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
    void checkpointStarted() throws IOException {
        mEngine.resume();

        // Make sure that durable replication data is not behind local database.
        mManager.syncConfirm(mCheckpointPos, -1);
    }

    @Override
    void checkpointFinished() throws IOException {
        mManager.checkpointed(mCheckpointPos);
    }

    @Override
    void opWriteCheck() throws IOException {
        if (!mIsLeader) {
            throw unmodifiable();
        }
    }

    @Override
    void write(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        if (len > 0 && !mManager.write(buffer, 0, len)) {
            throw unmodifiable();
        }
    }

    @Override
    long writeCommit(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        if (len > 0) {
            return mManager.writeCommit(buffer, 0, len);
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
        if (!mIsLeader) {
            mManager.flip();
            mIsLeader = true;

            // Clear the log state and write a reset op to signal leader transition.
            clearAndReset();

            // Record leader transition epoch.
            timestamp();

            // Don't trust timestamp alone to help detect divergent logs.
            nopRandom();

            flush();
        }
    }

    private synchronized UnmodifiableReplicaException unmodifiable() {
        if (mIsLeader) {
            mManager.flip();
            mIsLeader = false;

            // Invoke from a separate thread, avoiding deadlock during the transition.
            new Thread() {
                public void run() {
                    // Start receiving if not, but does nothing if already receiving. A reset
                    // op is expected, and so the initial transaction id can be zero.
                    mEngine.startReceiving(0);
                }
            }.start();
        }

        return new UnmodifiableReplicaException();
    }
}
